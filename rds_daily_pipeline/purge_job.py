"""Verify one archived window in S3, then delete exactly that window from RDS.

Chạy như Glue ETL job để dùng lại Glue Connection (private subnet + JDBC driver
PostgreSQL có sẵn), nên không cần cài thêm package trong subnet không có NAT.

Thứ tự bắt buộc: archive -> verify -> purge. Job fail nếu verify không đạt, và
Step Functions sẽ không commit watermark, nên window đó được xử lý lại.
"""

from __future__ import annotations

import re
import sys
from datetime import date, datetime, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

MAX_WINDOW_DAYS = 3660


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"Invalid PostgreSQL identifier: {value!r}")
    return value


def timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def flag(value: str) -> bool:
    return value.strip().lower() in {"true", "1", "yes"}


def optional(value: str) -> str:
    value = value.strip()
    return "" if value.lower() in {"", "none"} else value


def child_tables(raw: str) -> tuple[tuple[str, str], ...]:
    """Parse ``child_table:fk_column`` pairs, ví dụ ``order_items:order_id``."""
    if not optional(raw):
        return ()
    pairs: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(f"CHILD_TABLES cần dạng table:fk_column, nhận {item!r}")
        table, column = item.split(":", 1)
        pairs.append((identifier(table.strip()), identifier(column.strip())))
    return tuple(pairs)


def sibling_path(path: str, name: str) -> str:
    return f"{path.rstrip('/').rsplit('/', 1)[0]}/{name}/"


def window_days(start: datetime, end: datetime) -> list[date]:
    """Các day partition mà window ``(start, end]`` có thể ghi vào."""
    first = start.date()
    last = (end - timedelta(seconds=1)).date()
    if last < first:
        return []
    total = (last - first).days + 1
    if total > MAX_WINDOW_DAYS:
        raise RuntimeError(f"Window quá rộng: {total} ngày. Chia nhỏ catch-up.")
    return [first + timedelta(days=index) for index in range(total)]


def day_filter(days: list[date]) -> Column:
    """Filter trên đúng partition column để Spark prune, không quét cả bảng."""
    condition: Column | None = None
    for value in days:
        term = (
            (F.col("year") == value.strftime("%Y"))
            & (F.col("month") == value.strftime("%m"))
            & (F.col("day") == value.strftime("%d"))
        )
        condition = term if condition is None else (condition | term)
    return condition if condition is not None else F.lit(False)


def window_clause(alias: str, column: str, start: datetime, end: datetime) -> str:
    prefix = f'{alias}."{column}"' if alias else f'"{column}"'
    return (
        f"{prefix} > TIMESTAMPTZ '{start.isoformat()}' "
        f"AND {prefix} <= TIMESTAMPTZ '{end.isoformat()}'"
    )


def read_jdbc(glue_context: GlueContext, jdbc: dict[str, str], query: str) -> DataFrame:
    return (
        glue_context.spark_session.read.format("jdbc")
        .option("url", jdbc["fullUrl"])
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .load()
    )


def read_archive(glue_context: GlueContext, path: str, days: list[date]) -> DataFrame | None:
    """Đọc curated Parquet của một bảng; trả về None nếu prefix chưa tồn tại."""
    try:
        frame = glue_context.spark_session.read.parquet(path)
    except Exception as error:  # noqa: BLE001 - path chưa có object nào
        print(f"! Không đọc được curated path {path}: {error}")
        return None
    return frame.where(day_filter(days))


def _normalize(frame: DataFrame, key: str, checksum: str) -> DataFrame:
    """Chuẩn hóa key về string để so sánh RDS với Parquet không lệch kiểu."""
    result = frame.withColumn("_key", F.col(key).cast("string"))
    if checksum:
        # 28 chữ số để sum() còn chỗ mở rộng precision; nếu cast sát 38 thì tổng
        # có thể overflow thành null và biến so sánh checksum thành vô nghĩa.
        result = result.withColumn("_amount", F.col(checksum).cast("decimal(28,6)"))
    return result


def _summary(frame: DataFrame, checksum: str) -> dict[str, object]:
    aggregates = [
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("_key").alias("keys"),
    ]
    if checksum:
        aggregates.append(F.coalesce(F.sum("_amount"), F.lit(0)).alias("amount"))
    row = frame.agg(*aggregates).collect()[0]
    result: dict[str, object] = {"rows": row["rows"], "keys": row["keys"]}
    if checksum:
        result["amount"] = row["amount"]
    return result


def _jdbc_count(glue_context: GlueContext, jdbc: dict[str, str], query: str) -> int:
    return int(read_jdbc(glue_context, jdbc, query).collect()[0][0])


def _connect(spark_context: SparkContext, jdbc: dict[str, str]):
    """Mở JDBC connection qua JVM để chạy DELETE mà Spark không hỗ trợ."""
    url = jdbc["fullUrl"]
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    jvm = spark_context._jvm
    jvm.java.lang.Class.forName("org.postgresql.Driver")
    return jvm.java.sql.DriverManager.getConnection(url, jdbc["user"], jdbc["password"])


def _delete_window(
    spark_context: SparkContext,
    jdbc: dict[str, str],
    schema: str,
    table: str,
    primary_key: str,
    children: tuple[tuple[str, str], ...],
    window_sql: str,
    batch_size: int,
) -> dict[str, int]:
    """Xóa theo batch keyset; child trước parent, mỗi batch một transaction."""
    batch = (
        f'SELECT "{primary_key}" FROM "{schema}"."{table}" '
        f"WHERE {window_sql} ORDER BY \"{primary_key}\" LIMIT {batch_size}"
    )
    deleted = {table: 0, **{child: 0 for child, _ in children}}
    connection = _connect(spark_context, jdbc)
    try:
        statement = connection.createStatement()
        # Đặt timezone ngoài transaction để commit/rollback không hoàn tác nó.
        statement.execute("SET SESSION TIME ZONE 'UTC'")
        connection.setAutoCommit(False)
        rounds = 0
        while True:
            # Trong cùng transaction, hai statement thấy cùng snapshot, nên
            # subselect batch cho child và parent luôn ra đúng một tập key.
            for child, foreign_key in children:
                deleted[child] += statement.executeUpdate(
                    f'DELETE FROM "{schema}"."{child}" c USING ({batch}) b '
                    f'WHERE c."{foreign_key}" = b."{primary_key}"'
                )
            removed = statement.executeUpdate(
                f'DELETE FROM "{schema}"."{table}" '
                f'WHERE "{primary_key}" IN ({batch})'
            )
            connection.commit()
            deleted[table] += removed
            if removed == 0:
                break
            rounds += 1
            print(f"batch {rounds}: đã xóa {deleted[table]} row {table}")
        statement.close()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
    return deleted


def _vacuum(
    spark_context: SparkContext,
    jdbc: dict[str, str],
    schema: str,
    tables: tuple[str, ...],
) -> None:
    """VACUUM ANALYZE để giảm bloat; không chạy được trong transaction."""
    connection = _connect(spark_context, jdbc)
    try:
        connection.setAutoCommit(True)
        statement = connection.createStatement()
        for name in tables:
            statement.execute(f'VACUUM (ANALYZE) "{schema}"."{name}"')
            print(f"VACUUM ANALYZE xong: {schema}.{name}")
        statement.close()
    finally:
        connection.close()
    print("VACUUM chỉ giải phóng space trong file dữ liệu; allocated storage "
          "của RDS instance vẫn không giảm.")


def main() -> None:
    keys = [
        "JOB_NAME", "CONNECTION_NAME", "SOURCE_SCHEMA", "SOURCE_TABLE",
        "DATE_COLUMN", "PRIMARY_KEY", "CHILD_TABLES", "CHECKSUM_COLUMN",
        "CURATED_PATH", "WINDOW_FROM", "WINDOW_TO",
        "DRY_RUN", "BATCH_SIZE", "RUN_VACUUM",
    ]
    args = getResolvedOptions(sys.argv, keys)
    spark_context = SparkContext.getOrCreate()
    glue_context = GlueContext(spark_context)
    # Partition column phải giữ nguyên dạng string, nếu không "05" bị suy ra là 5
    # và filter theo month/day sẽ không khớp folder nào.
    glue_context.spark_session.conf.set(
        "spark.sql.sources.partitionColumnTypeInference.enabled", "false"
    )
    # Cùng timezone với archive job, nếu không day partition và window lệch nhau.
    glue_context.spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    schema = identifier(args["SOURCE_SCHEMA"])
    table = identifier(args["SOURCE_TABLE"])
    column = identifier(args["DATE_COLUMN"])
    primary_key = identifier(args["PRIMARY_KEY"])
    checksum = optional(args["CHECKSUM_COLUMN"])
    if checksum:
        identifier(checksum)
    children = child_tables(args["CHILD_TABLES"])
    curated = args["CURATED_PATH"]
    start, end = timestamp(args["WINDOW_FROM"]), timestamp(args["WINDOW_TO"])
    dry_run = flag(args["DRY_RUN"])
    batch_size = max(1, int(args["BATCH_SIZE"]))
    days = window_days(start, end)
    jdbc = glue_context.extract_jdbc_conf(args["CONNECTION_NAME"])
    window_sql = window_clause("", column, start, end)

    print(f"Window     : ({start.isoformat()}, {end.isoformat()}]")
    print(f"Partitions : {days[0]} .. {days[-1]}" if days else "Partitions : none")
    print(f"Mode       : {'DRY RUN' if dry_run else 'PURGE'}")

    selected = [primary_key] + ([checksum] if checksum else [])
    columns = ", ".join(f'"{name}"' for name in selected)
    source = _normalize(
        read_jdbc(
            glue_context, jdbc,
            f'SELECT {columns} FROM "{schema}"."{table}" WHERE {window_sql}',
        ),
        primary_key, checksum,
    ).cache()
    source_stats = _summary(source, checksum)
    print(f"RDS        : {source_stats}")
    if source_stats["rows"] == 0:
        print("Không còn row nào trong window; bỏ qua purge.")
        job.commit()
        return

    archive_frame = read_archive(glue_context, curated, days)
    if archive_frame is None:
        raise RuntimeError(f"Curated archive chưa tồn tại: {curated}")
    archive = _normalize(
        archive_frame.where(
            F.col(column).isNotNull()
            & (F.col(column).cast("timestamp") > F.lit(start.isoformat()).cast("timestamp"))
            & (F.col(column).cast("timestamp") <= F.lit(end.isoformat()).cast("timestamp"))
        ),
        primary_key, checksum,
    ).cache()
    archive_stats = _summary(archive, checksum)
    print(f"Archive    : {archive_stats}")

    # Anti-join là bằng chứng ở mức từng key, mạnh hơn so sánh rowcount.
    missing = (
        source.select("_key").distinct()
        .join(archive.select("_key").distinct(), "_key", "left_anti")
    ).cache()
    missing_count = missing.count()
    if missing_count:
        sample = [row["_key"] for row in missing.limit(10).collect()]
        raise RuntimeError(
            f"{missing_count} key trong RDS chưa có trong archive, ví dụ {sample}. "
            "Không purge. Chạy lại archive cho window này trước."
        )
    if checksum and source_stats["amount"] > archive_stats["amount"]:
        raise RuntimeError(
            f"Checksum {checksum} lệch: RDS={source_stats['amount']} "
            f"> archive={archive_stats['amount']}. Không purge."
        )

    parent_keys = archive.select("_key").distinct()
    for child, foreign_key in children:
        source_children = _jdbc_count(
            glue_context, jdbc,
            f'SELECT count(*) FROM "{schema}"."{child}" c '
            f'JOIN "{schema}"."{table}" p ON c."{foreign_key}" = p."{primary_key}" '
            f'WHERE {window_clause("p", column, start, end)}',
        )
        child_frame = read_archive(glue_context, sibling_path(curated, child), days)
        archived_children = 0
        if child_frame is not None:
            archived_children = (
                child_frame.withColumn("_key", F.col(foreign_key).cast("string"))
                .join(parent_keys, "_key", "left_semi")
                .count()
            )
        print(f"Child {child}: rds={source_children} archive={archived_children}")
        if archived_children < source_children:
            raise RuntimeError(
                f"Child {child} chưa archive đủ ({archived_children} < "
                f"{source_children}). Không purge."
            )

    print("Verify     : PASSED")
    if dry_run:
        print(f"DRY RUN: sẽ xóa {source_stats['rows']} row {table} "
              f"và child rows tương ứng. Không thay đổi gì trong RDS.")
        job.commit()
        return

    deleted = _delete_window(
        spark_context, jdbc, schema, table, primary_key, children,
        window_sql, batch_size,
    )
    print(f"Đã xóa     : {deleted}")
    if flag(args["RUN_VACUUM"]):
        _vacuum(spark_context, jdbc, schema, (table, *(c for c, _ in children)))
    job.commit()


if __name__ == "__main__":
    main()
