"""Purge the rows that the DMS full load already archived to S3.

Quy trình bắt buộc: archive -> verify -> purge. Module này không bao giờ tự suy
diễn phạm vi xóa. Cutoff được đọc lại từ table mapping của DMS task, và chỉ
những primary key thực sự tồn tại trong curated archive mới bị xóa khỏi RDS.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
CUTOFF_FORMAT = re.compile(r"\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}:\d{2}(\.\d{1,6})?)?Z?")


@dataclass(frozen=True)
class Config:
    """Cấu hình purge, đọc từ chính dms/.env của pipeline archive."""

    region: str
    bucket: str
    glue_database: str
    curated_table: str
    athena_output: str
    athena_workgroup: str
    schema: str
    table: str
    key_column: str
    date_column: str
    amount_column: str
    child_tables: tuple[tuple[str, str], ...]
    task_id: str
    cutoff_override: str
    batch_size: int
    max_keys: int
    host: str
    port: int
    database: str
    username: str
    password: str


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("<"):
        raise ValueError(f"Missing a real value for {name} in dms/.env")
    return value


def _identifier(value: str, name: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid identifier for {name}: {value!r}")
    return value


def _child_tables(raw: str) -> tuple[tuple[str, str], ...]:
    """Parse ``child_table:fk_column`` pairs, ví dụ ``order_items:order_id``."""
    pairs: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        if ":" not in item:
            raise ValueError(
                f"PURGE_CHILD_TABLES cần dạng table:fk_column, nhận được {item!r}"
            )
        child, column = item.split(":", 1)
        pairs.append((
            _identifier(child.strip(), "PURGE_CHILD_TABLES table"),
            _identifier(column.strip(), "PURGE_CHILD_TABLES column"),
        ))
    return tuple(pairs)


def config() -> Config:
    """Load purge config từ dms/.env."""
    load_dotenv(ROOT / ".env", override=True)
    bucket = _required("S3_BUCKET")
    curated_prefix = _required("CURATED_PREFIX").strip("/")
    table_prefix = os.getenv("GLUE_TABLE_PREFIX", "orders_rds_").strip()
    default_table = f"{table_prefix}{curated_prefix.split('/')[-1]}"
    return Config(
        region=_required("AWS_REGION"),
        bucket=bucket,
        glue_database=os.getenv("GLUE_DATABASE", "archive").strip(),
        curated_table=_identifier(
            os.getenv("GLUE_CURATED_TABLE", default_table).strip(),
            "GLUE_CURATED_TABLE",
        ),
        athena_output=os.getenv(
            "ATHENA_OUTPUT",
            f"s3://{bucket}/athena-results/purge/",
        ).strip(),
        athena_workgroup=os.getenv("ATHENA_WORKGROUP", "primary").strip(),
        schema=_identifier(_required("SOURCE_SCHEMA"), "SOURCE_SCHEMA"),
        table=_identifier(_required("SOURCE_TABLE"), "SOURCE_TABLE"),
        key_column=_identifier(
            os.getenv("PRIMARY_KEY", "order_id").strip(), "PRIMARY_KEY"
        ),
        date_column=_identifier(_required("DATE_COLUMN"), "DATE_COLUMN"),
        amount_column=os.getenv("PURGE_CHECKSUM_COLUMN", "amount").strip(),
        child_tables=_child_tables(
            os.getenv("PURGE_CHILD_TABLES", "order_items:order_id")
        ),
        task_id=f"{_required('DMS_PREFIX')}-task",
        cutoff_override=os.getenv("PURGE_CUTOFF", "").strip(),
        batch_size=int(os.getenv("PURGE_BATCH_SIZE", "500")),
        max_keys=int(os.getenv("PURGE_MAX_KEYS", "200000")),
        host=_required("RDS_HOST"),
        port=int(os.getenv("RDS_PORT", "5432")),
        database=_required("RDS_DATABASE"),
        username=_required("RDS_USERNAME"),
        password=_required("RDS_PASSWORD"),
    )


def _connect(cfg: Config) -> Any:
    """Mở kết nối PostgreSQL và ép session timezone về UTC.

    Cutoff của DMS là UTC. Nếu session dùng timezone khác, so sánh timestamptz
    với literal string sẽ lệch múi giờ và xóa sai phạm vi.
    """
    import psycopg2

    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.username,
        password=cfg.password,
        connect_timeout=15,
        sslmode="require",
    )
    with conn.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    conn.commit()
    return conn


def cutoff() -> str:
    """Đọc cutoff thật mà DMS task đã dùng, hoặc PURGE_CUTOFF khi task đã bị xóa."""
    cfg = config()
    dms = boto3.client("dms", region_name=cfg.region)
    try:
        tasks = dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-id", "Values": [cfg.task_id]}],
            WithoutSettings=False,
        )["ReplicationTasks"]
    except dms.exceptions.ResourceNotFoundFault:
        tasks = []

    if tasks:
        mappings = json.loads(tasks[0]["TableMappings"])
        values = [
            condition.get("value")
            for rule in mappings.get("rules", [])
            for source_filter in rule.get("filters", [])
            if source_filter.get("column-name") == cfg.date_column
            for condition in source_filter.get("filter-conditions", [])
            if condition.get("filter-operator") == "lte" and condition.get("value")
        ]
        if len(values) != 1:
            raise RuntimeError(
                f"DMS task {cfg.task_id} không có đúng một cutoff lte cho "
                f"{cfg.date_column}; không thể xác định phạm vi purge."
            )
        return str(values[0])

    if not cfg.cutoff_override:
        raise RuntimeError(
            f"Không tìm thấy DMS task {cfg.task_id}. Nếu task đã destroy, đặt "
            "PURGE_CUTOFF trong dms/.env bằng đúng cutoff của lần full load."
        )
    if not CUTOFF_FORMAT.fullmatch(cfg.cutoff_override):
        raise ValueError(f"PURGE_CUTOFF không hợp lệ: {cfg.cutoff_override!r}")
    return cfg.cutoff_override


def _athena_rows(cfg: Config, sql: str, timeout_seconds: int = 300) -> list[list[str | None]]:
    """Chạy một query Athena và trả về các row (không gồm header)."""
    athena = boto3.client("athena", region_name=cfg.region)
    query_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": cfg.glue_database},
        ResultConfiguration={"OutputLocation": cfg.athena_output},
        WorkGroup=cfg.athena_workgroup,
    )["QueryExecutionId"]

    deadline = time.monotonic() + timeout_seconds
    while True:
        status = athena.get_query_execution(
            QueryExecutionId=query_id
        )["QueryExecution"]["Status"]
        state = status["State"]
        if state == "SUCCEEDED":
            break
        if state in {"FAILED", "CANCELLED"}:
            raise RuntimeError(
                f"Athena {state}: {status.get('StateChangeReason', 'no reason')}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Athena query timeout sau {timeout_seconds}s")
        time.sleep(2)

    rows: list[list[str | None]] = []
    first_page = True
    for page in athena.get_paginator("get_query_results").paginate(
        QueryExecutionId=query_id
    ):
        data = page["ResultSet"]["Rows"]
        if first_page:
            data = data[1:]
            first_page = False
        rows.extend(
            [column.get("VarCharValue") for column in row["Data"]] for row in data
        )
    return rows


def _sql_cutoff(value: str) -> str:
    """Chuẩn hóa cutoff về dạng ``YYYY-MM-DD HH:MM:SS[.ffffff]`` cho SQL."""
    normalized = value.strip().replace("T", " ").rstrip("Z").strip()
    if not CUTOFF_FORMAT.fullmatch(normalized):
        raise ValueError(f"Cutoff không hợp lệ: {value!r}")
    return normalized


def _archive_filter(cfg: Config, value: str) -> str:
    return (
        f'"{cfg.date_column}" IS NOT NULL '
        f'AND CAST("{cfg.date_column}" AS timestamp) <= timestamp \'{value}\''
    )


def _decimal(value: str | None) -> Decimal:
    return Decimal(value or "0").quantize(Decimal("0.01"))


def verify(value: str | None = None) -> dict[str, Any]:
    """So sánh RDS với curated archive trong phạm vi cutoff. Không xóa gì cả."""
    cfg = config()
    boundary = _sql_cutoff(value or cutoff())
    amount = cfg.amount_column
    if amount:
        _identifier(amount, "PURGE_CHECKSUM_COLUMN")

    source_sql = (
        f"SELECT count(*), count(DISTINCT {cfg.key_column})"
        + (f", coalesce(sum({amount}), 0)" if amount else ", 0")
        + f" FROM {cfg.schema}.{cfg.table}"
        f" WHERE {cfg.date_column} IS NOT NULL AND {cfg.date_column} <= %s"
    )
    conn = _connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(source_sql, (boundary,))
            source_rows, source_keys, source_amount = cur.fetchone()
    finally:
        conn.close()

    archive_sql = (
        f'SELECT count(*), count(DISTINCT "{cfg.key_column}")'
        + (f', coalesce(sum("{amount}"), 0)' if amount else ", 0")
        + f' FROM "{cfg.glue_database}"."{cfg.curated_table}"'
        f" WHERE {_archive_filter(cfg, boundary)}"
    )
    archive_row = _athena_rows(cfg, archive_sql)[0]
    archive_rows = int(archive_row[0] or 0)
    archive_keys = int(archive_row[1] or 0)
    archive_amount = _decimal(archive_row[2])

    report = {
        "cutoff": boundary,
        "source_rows": int(source_rows),
        "source_keys": int(source_keys),
        "source_amount": _decimal(str(source_amount)),
        "archive_rows": archive_rows,
        "archive_keys": archive_keys,
        "archive_amount": archive_amount,
    }
    report["duplicate_archive_rows"] = archive_rows - archive_keys
    report["missing_keys"] = int(source_keys) - archive_keys
    report["ok"] = (
        report["missing_keys"] <= 0
        and (archive_keys > 0 or int(source_rows) == 0)
        and (not amount or report["source_amount"] <= archive_amount)
    )

    print(f"Cutoff        : {boundary} (UTC)")
    print(f"RDS           : rows={report['source_rows']} keys={report['source_keys']}"
          + (f" {amount}={report['source_amount']}" if amount else ""))
    print(f"Archive       : rows={archive_rows} keys={archive_keys}"
          + (f" {amount}={archive_amount}" if amount else ""))
    if report["duplicate_archive_rows"] > 0:
        print(f"! Archive có {report['duplicate_archive_rows']} row trùng key "
              "(có thể do replay full load). Kiểm tra curated trước khi purge.")
    if report["missing_keys"] > 0:
        print(f"! {report['missing_keys']} key trong RDS chưa có trong archive. "
              "Chạy lại DMS + partition job trước khi purge.")
    print("Verify        : " + ("PASSED" if report["ok"] else "FAILED"))
    return report


def _archived_keys(cfg: Config, boundary: str, expected: int) -> set[str]:
    """Tải danh sách key đã archive để purge chỉ xóa đúng những key đó."""
    if expected > cfg.max_keys:
        raise RuntimeError(
            f"Archive có {expected} key, vượt PURGE_MAX_KEYS={cfg.max_keys}. "
            "Với khối lượng này nên dùng partition detach/drop thay vì batched delete."
        )
    rows = _athena_rows(
        cfg,
        f'SELECT DISTINCT "{cfg.key_column}" '
        f'FROM "{cfg.glue_database}"."{cfg.curated_table}" '
        f"WHERE {_archive_filter(cfg, boundary)}",
        timeout_seconds=900,
    )
    return {row[0] for row in rows if row[0] is not None}


def purge(dry_run: bool = True, value: str | None = None) -> dict[str, Any]:
    """Xóa các row đã được archive khỏi RDS theo từng batch nhỏ.

    Mặc định chạy dry-run. Gọi ``purge(dry_run=False)`` mới thực sự xóa.
    """
    report = verify(value)
    if not report["ok"]:
        raise RuntimeError("Verify thất bại; không purge. Xem log phía trên.")

    cfg = config()
    boundary = report["cutoff"]
    keys = _archived_keys(cfg, boundary, report["archive_keys"])
    print(f"Đã tải {len(keys)} key từ archive")
    if cfg.child_tables:
        children = ", ".join(f"{name}.{column}" for name, column in cfg.child_tables)
        print(f"Child rows sẽ xóa cùng parent: {children}")

    base_select = (
        f"SELECT {cfg.key_column} FROM {cfg.schema}.{cfg.table}"
        f" WHERE {cfg.date_column} IS NOT NULL AND {cfg.date_column} <= %s"
    )
    order_by = f" ORDER BY {cfg.key_column} LIMIT %s"
    delete_sql = (
        f"DELETE FROM {cfg.schema}.{cfg.table}"
        f" WHERE {cfg.key_column} = ANY(%s)"
        f" AND {cfg.date_column} IS NOT NULL AND {cfg.date_column} <= %s"
    )

    deleted = skipped = child_deleted = batches = 0
    last_key: Any = None
    conn = _connect(cfg)
    try:
        while True:
            with conn.cursor() as cur:
                if last_key is None:
                    cur.execute(base_select + order_by, (boundary, cfg.batch_size))
                else:
                    cur.execute(
                        f"{base_select} AND {cfg.key_column} > %s{order_by}",
                        (boundary, last_key, cfg.batch_size),
                    )
                batch = [row[0] for row in cur.fetchall()]
            if not batch:
                break
            last_key = batch[-1]
            removable = [key for key in batch if str(key) in keys]
            skipped += len(batch) - len(removable)
            if not removable:
                continue
            batches += 1
            if dry_run:
                deleted += len(removable)
                continue
            with conn.cursor() as cur:
                for child, column in cfg.child_tables:
                    cur.execute(
                        f"DELETE FROM {cfg.schema}.{child} WHERE {column} = ANY(%s)",
                        (removable,),
                    )
                    child_deleted += cur.rowcount
                cur.execute(delete_sql, (removable, boundary))
                deleted += cur.rowcount
            conn.commit()
            # Xóa xong batch thì key đã bị loại khỏi bảng, nên keyset cursor tiếp
            # tục từ last_key thay vì quét lại từ đầu.
            print(f"batch {batches}: deleted={deleted} child={child_deleted}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    result = {
        "cutoff": boundary,
        "dry_run": dry_run,
        "deleted_rows": deleted,
        "deleted_child_rows": child_deleted,
        "skipped_rows": skipped,
    }
    label = "DRY RUN, không xóa gì" if dry_run else "Đã xóa"
    print(f"{label}: {deleted} row {cfg.table}, {child_deleted} child row")
    if skipped:
        print(f"Bỏ qua {skipped} row chưa có trong archive (giữ nguyên trong RDS).")
    if not dry_run:
        print("Lưu ý: allocated storage của RDS không tự thu nhỏ. Chạy vacuum() "
              "để giảm bloat và cập nhật statistics.")
    return result


def status(value: str | None = None) -> dict[str, Any]:
    """Đếm phần dữ liệu còn lại trong phạm vi cutoff và tổng số row hiện tại."""
    cfg = config()
    boundary = _sql_cutoff(value or cutoff())
    conn = _connect(cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {cfg.schema}.{cfg.table}"
                f" WHERE {cfg.date_column} IS NOT NULL AND {cfg.date_column} <= %s",
                (boundary,),
            )
            remaining = cur.fetchone()[0]
            counts: dict[str, int] = {}
            for name in (cfg.table, *(child for child, _ in cfg.child_tables)):
                cur.execute(f"SELECT count(*) FROM {cfg.schema}.{name}")
                counts[name] = cur.fetchone()[0]
    finally:
        conn.close()
    print(f"Cutoff            : {boundary} (UTC)")
    print(f"Còn trong cutoff  : {remaining} row {cfg.table}")
    for name, total in counts.items():
        print(f"Tổng {name:<12}: {total} row")
    return {"cutoff": boundary, "remaining_in_cutoff": remaining, "row_counts": counts}


def vacuum() -> None:
    """VACUUM ANALYZE các bảng vừa purge để giảm bloat và refresh statistics."""
    cfg = config()
    conn = _connect(cfg)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for name in (cfg.table, *(child for child, _ in cfg.child_tables)):
                cur.execute(f"VACUUM (ANALYZE) {cfg.schema}.{name}")
                print(f"VACUUM ANALYZE xong: {cfg.schema}.{name}")
    finally:
        conn.close()
    print("VACUUM chỉ giải phóng space trong file dữ liệu; allocated storage của "
          "RDS instance vẫn không giảm.")


if __name__ == "__main__":
    verify()
    purge(dry_run=True)
