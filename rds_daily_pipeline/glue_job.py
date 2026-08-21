"""Extract one closed RDS time window and publish it as partitioned Parquet."""

from __future__ import annotations

import re
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def identifier(value: str) -> str:
    """Allow only PostgreSQL identifiers before interpolating the SQL query."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"Invalid PostgreSQL identifier: {value!r}")
    return value


def timestamp(value: str) -> str:
    """Validate an ISO timestamp and return a PostgreSQL-safe UTC literal."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat()


def child_tables(raw: str) -> tuple[tuple[str, str], ...]:
    """Parse ``child_table:fk_column`` pairs, ví dụ ``order_items:order_id``."""
    if not raw or raw.strip().lower() == "none":
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
    """Trả về folder cùng cấp với ``path`` nhưng đổi segment cuối thành ``name``."""
    return f"{path.rstrip('/').rsplit('/', 1)[0]}/{name}/"


def window_clause(alias: str, column: str, window_from: str, window_to: str) -> str:
    """Ranh giới nửa mở ``(from, to]`` để hai window liền nhau không chồng lấp."""
    prefix = f'{alias}."{column}"' if alias else f'"{column}"'
    return (
        f"{prefix} > TIMESTAMPTZ '{window_from}' "
        f"AND {prefix} <= TIMESTAMPTZ '{window_to}'"
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


def _partition(frame: DataFrame, source_column: str) -> DataFrame:
    """Thêm year/month/day theo ngày nghiệp vụ rồi bỏ cột nguồn tạm."""
    return (
        frame.withColumn("_partition_date", F.to_date(F.col(source_column)))
        .withColumn("year", F.date_format("_partition_date", "yyyy"))
        .withColumn("month", F.date_format("_partition_date", "MM"))
        .withColumn("day", F.date_format("_partition_date", "dd"))
        .drop("_partition_date")
    )


def _publish(frame: DataFrame, raw_path: str, curated_path: str) -> None:
    for path in (raw_path, curated_path):
        (
            frame.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(path)
        )


def main() -> None:
    keys = [
        "JOB_NAME", "CONNECTION_NAME", "SOURCE_SCHEMA", "SOURCE_TABLE",
        "DATE_COLUMN", "PRIMARY_KEY", "CHILD_TABLES",
        "WINDOW_FROM", "WINDOW_TO", "BATCH_DATE", "RAW_PATH", "CURATED_PATH",
    ]
    args = getResolvedOptions(sys.argv, keys)
    glue_context = GlueContext(SparkContext.getOrCreate())
    glue_context.spark_session.conf.set(
        "spark.sql.sources.partitionOverwriteMode", "dynamic"
    )
    # to_date() dùng session timezone. DATE_COLUMN là timestamptz nên phải ghim
    # UTC, nếu không một row 23:30Z bị đẩy sang day partition của ngày hôm sau.
    glue_context.spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    schema = identifier(args["SOURCE_SCHEMA"])
    table = identifier(args["SOURCE_TABLE"])
    column = identifier(args["DATE_COLUMN"])
    primary_key = identifier(args["PRIMARY_KEY"])
    window_from = timestamp(args["WINDOW_FROM"])
    window_to = timestamp(args["WINDOW_TO"])
    children = child_tables(args["CHILD_TABLES"])
    jdbc = glue_context.extract_jdbc_conf(args["CONNECTION_NAME"])

    frame = read_jdbc(
        glue_context, jdbc,
        f'SELECT * FROM "{schema}"."{table}" '
        f'WHERE {window_clause("", column, window_from, window_to)}',
    )
    # Partition bằng ngày nghiệp vụ trong DATE_COLUMN. Vì vậy một lần catch-up
    # nhiều ngày vẫn tách thành các folder ngày riêng, không gom theo batch chạy.
    _publish(_partition(frame, column), args["RAW_PATH"], args["CURATED_PATH"])

    # Child rows phải được archive cùng window với parent. Nếu bỏ qua bước này,
    # purge sẽ xóa child theo foreign key trong khi S3 chưa có bản sao nào.
    for child, foreign_key in children:
        child_frame = read_jdbc(
            glue_context, jdbc,
            f'SELECT c.*, p."{column}" AS _parent_date '
            f'FROM "{schema}"."{child}" c '
            f'JOIN "{schema}"."{table}" p '
            f'ON c."{foreign_key}" = p."{primary_key}" '
            f'WHERE {window_clause("p", column, window_from, window_to)}',
        )
        _publish(
            _partition(child_frame, "_parent_date").drop("_parent_date"),
            sibling_path(args["RAW_PATH"], child),
            sibling_path(args["CURATED_PATH"], child),
        )
    job.commit()


if __name__ == "__main__":
    main()
