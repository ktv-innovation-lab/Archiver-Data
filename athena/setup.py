"""Set up a cost-controlled Athena workgroup for the archive data lake."""

from __future__ import annotations

import argparse
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


MODULE_DIR = Path(__file__).resolve().parent
MIN_SCAN_CUTOFF_MB = 10


@dataclass(frozen=True)
class Config:
    """Validated Athena configuration loaded from ``athena/.env``."""

    region: str
    bucket: str
    results_prefix: str
    workgroup: str
    database: str
    rds_table: str
    ddb_table: str
    scan_cutoff_mb: int

    @property
    def output_location(self) -> str:
        """Return the enforced S3 location for Athena query results."""
        return f"s3://{self.bucket}/{self.results_prefix}/"

    @property
    def scan_cutoff_bytes(self) -> int:
        """Convert the human-friendly MiB limit to bytes for the Athena API."""
        return self.scan_cutoff_mb * 1024 * 1024


def _required(name: str) -> str:
    """Read a required environment variable and reject template placeholders."""
    value = os.getenv(name, "").strip()
    if not value or value.startswith("<"):
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _identifier(name: str, value: str) -> str:
    """Validate a Glue/Athena identifier before using it inside SQL."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(
            f"{name}={value!r} is not a safe Athena identifier. "
            "Use only letters, numbers and underscores; it cannot start with a number."
        )
    return value


def load_config() -> Config:
    """Load ``.env`` on every command so notebook and CLI runs stay consistent."""
    load_dotenv(MODULE_DIR / ".env", override=True)
    scan_cutoff_mb = int(os.getenv("BYTES_SCAN_CUTOFF_MB", "1024"))
    if scan_cutoff_mb < MIN_SCAN_CUTOFF_MB:
        raise ValueError(
            f"BYTES_SCAN_CUTOFF_MB must be at least {MIN_SCAN_CUTOFF_MB} MB"
        )

    return Config(
        region=_required("AWS_REGION"),
        bucket=_required("S3_BUCKET"),
        results_prefix=_required("ATHENA_RESULTS_PREFIX").strip("/"),
        workgroup=_required("ATHENA_WORKGROUP"),
        database=_identifier("GLUE_DATABASE", _required("GLUE_DATABASE")),
        rds_table=_identifier("RDS_GLUE_TABLE", _required("RDS_GLUE_TABLE")),
        ddb_table=_identifier("DDB_GLUE_TABLE", _required("DDB_GLUE_TABLE")),
        scan_cutoff_mb=scan_cutoff_mb,
    )


def _clients(cfg: Config) -> tuple[Any, Any, Any, str]:
    """Create AWS clients and return the current account ID."""
    session = boto3.Session(region_name=cfg.region)
    account_id = session.client("sts").get_caller_identity()["Account"]
    return (
        session.client("athena"),
        session.client("glue"),
        session.client("s3"),
        account_id,
    )


def _validate_bucket(s3: Any, cfg: Config) -> None:
    """Fail early when the result bucket is missing or belongs to another Region."""
    try:
        s3.head_bucket(Bucket=cfg.bucket)
        response = s3.get_bucket_location(Bucket=cfg.bucket)
    except ClientError as error:
        raise RuntimeError(f"Cannot access S3 bucket {cfg.bucket!r}") from error

    # AWS represents us-east-1 as None and historically represents eu-west-1 as EU.
    location = response.get("LocationConstraint") or "us-east-1"
    location = "eu-west-1" if location == "EU" else location
    if location != cfg.region:
        raise ValueError(
            f"S3 bucket is in {location}, but AWS_REGION is {cfg.region}. "
            "Athena results should stay in the same Region."
        )


def _table_location(glue: Any, cfg: Config, table_name: str) -> str:
    """Validate a catalog table and return its curated S3 location."""
    try:
        table = glue.get_table(DatabaseName=cfg.database, Name=table_name)["Table"]
    except glue.exceptions.EntityNotFoundException as error:
        available = _catalog_tables(glue, cfg.database)
        if available:
            choices = "\n".join(
                f"  - {item['name']} -> {item['location'] or 'location unavailable'}"
                for item in available
            )
        else:
            choices = "  (database currently contains no tables)"
        raise RuntimeError(
            f"Glue table {cfg.database}.{table_name} does not exist. "
            "Run discover_tables() or copy the exact table name below into athena/.env.\n"
            f"Available tables in {cfg.database}:\n{choices}"
        ) from error

    location = table.get("StorageDescriptor", {}).get("Location", "")
    if not location.startswith("s3://"):
        raise RuntimeError(
            f"Glue table {cfg.database}.{table_name} has no valid S3 location"
        )
    return location


def _catalog_tables(glue: Any, database: str) -> list[dict[str, str]]:
    """List table names and S3 locations from one Glue database."""
    tables: list[dict[str, str]] = []
    try:
        paginator = glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database):
            for table in page.get("TableList", []):
                tables.append(
                    {
                        "name": table["Name"],
                        "location": table.get("StorageDescriptor", {}).get("Location", ""),
                    }
                )
    except glue.exceptions.EntityNotFoundException as error:
        databases = _catalog_databases(glue)
        available = ", ".join(databases) if databases else "(no Glue databases found)"
        raise RuntimeError(
            f"Glue database {database!r} does not exist. Available databases: {available}"
        ) from error
    return sorted(tables, key=lambda item: item["name"])


def _catalog_databases(glue: Any) -> list[str]:
    """Return every Glue database visible to the current AWS identity."""
    databases: list[str] = []
    paginator = glue.get_paginator("get_databases")
    for page in paginator.paginate():
        databases.extend(item["Name"] for item in page.get("DatabaseList", []))
    return sorted(databases)


def discover_tables() -> list[dict[str, str]]:
    """Print account, Region, databases and tables for catalog troubleshooting."""
    cfg = load_config()
    session = boto3.Session(region_name=cfg.region)
    account_id = session.client("sts").get_caller_identity()["Account"]
    glue = session.client("glue")
    databases = _catalog_databases(glue)
    print(f"AWS account  : {account_id}")
    print(f"AWS Region   : {cfg.region}")
    print(f"Glue databases: {', '.join(databases) if databases else '(none)'}")
    print("")
    if cfg.database not in databases:
        print(f"Configured database {cfg.database!r} does not exist in this account/Region.")
        print("Check AWS_PROFILE/AWS_REGION or run the partition Glue crawler first.")
        return []

    tables = _catalog_tables(glue, cfg.database)
    print(f"Glue database: {cfg.database}")
    if not tables:
        print("No tables found. Run the partition job and Glue crawler first.")
        return tables
    for item in tables:
        print(f"- {item['name']}")
        print(f"  {item['location'] or 'S3 location unavailable'}")
    print("")
    print("Copy the exact names into RDS_GLUE_TABLE and DDB_GLUE_TABLE in athena/.env.")
    return tables


def _ensure_workgroup(athena: Any, cfg: Config, account_id: str) -> None:
    """Create or update the dedicated workgroup idempotently."""
    result_configuration = {
        "OutputLocation": cfg.output_location,
        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        "ExpectedBucketOwner": account_id,
        "AclConfiguration": {"S3AclOption": "BUCKET_OWNER_FULL_CONTROL"},
    }
    try:
        athena.get_work_group(WorkGroup=cfg.workgroup)
    except athena.exceptions.InvalidRequestException:
        athena.create_work_group(
            Name=cfg.workgroup,
            Description="Queries for the Archiver Data curated Parquet datasets",
            Configuration={
                "ResultConfiguration": result_configuration,
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": True,
                "BytesScannedCutoffPerQuery": cfg.scan_cutoff_bytes,
                "EngineVersion": {"SelectedEngineVersion": "AUTO"},
            },
            Tags=[
                {"Key": "Project", "Value": "archiver-data"},
                {"Key": "ManagedBy", "Value": "athena-setup"},
            ],
        )
        print(f"Created Athena workgroup: {cfg.workgroup}")
        return

    athena.update_work_group(
        WorkGroup=cfg.workgroup,
        Description="Queries for the Archiver Data curated Parquet datasets",
        State="ENABLED",
        ConfigurationUpdates={
            "ResultConfigurationUpdates": {
                "OutputLocation": cfg.output_location,
                "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                "ExpectedBucketOwner": account_id,
                "AclConfiguration": {"S3AclOption": "BUCKET_OWNER_FULL_CONTROL"},
            },
            "EnforceWorkGroupConfiguration": True,
            "PublishCloudWatchMetricsEnabled": True,
            "BytesScannedCutoffPerQuery": cfg.scan_cutoff_bytes,
            "EngineVersion": {"SelectedEngineVersion": "AUTO"},
        },
    )
    print(f"Updated Athena workgroup: {cfg.workgroup}")


def _named_queries(cfg: Config) -> list[dict[str, str]]:
    """Build saved queries that always filter by the three partition keys."""
    rds = f'"{cfg.database}"."{cfg.rds_table}"'
    ddb = f'"{cfg.database}"."{cfg.ddb_table}"'
    return [
        {
            "Name": f"{cfg.workgroup}-rds-daily-summary",
            "Description": "RDS order summary for one partition; replace the date values.",
            "Database": cfg.database,
            "QueryString": (
                "SELECT status, COUNT(*) AS total_orders, "
                "SUM(amount) AS total_amount\n"
                f"FROM {rds}\n"
                "WHERE year = '2026' AND month = '08' AND day = '01'\n"
                "GROUP BY status\nORDER BY total_orders DESC;"
            ),
        },
        {
            "Name": f"{cfg.workgroup}-ddb-daily-summary",
            "Description": "DynamoDB order summary for one partition; replace the date values.",
            "Database": cfg.database,
            "QueryString": (
                "SELECT status, COUNT(*) AS total_orders, "
                "SUM(amount) AS total_amount\n"
                f"FROM {ddb}\n"
                "WHERE year = '2026' AND month = '08' AND day = '01'\n"
                "GROUP BY status\nORDER BY total_orders DESC;"
            ),
        },
        {
            "Name": f"{cfg.workgroup}-rds-preview",
            "Description": "Preview at most 20 RDS rows from one partition.",
            "Database": cfg.database,
            "QueryString": (
                f"SELECT * FROM {rds}\n"
                "WHERE year = '2026' AND month = '08' AND day = '01'\nLIMIT 20;"
            ),
        },
        {
            "Name": f"{cfg.workgroup}-ddb-preview",
            "Description": "Preview at most 20 DynamoDB rows from one partition.",
            "Database": cfg.database,
            "QueryString": (
                f"SELECT * FROM {ddb}\n"
                "WHERE year = '2026' AND month = '08' AND day = '01'\nLIMIT 20;"
            ),
        },
    ]


def _list_named_queries(athena: Any, workgroup: str) -> list[dict[str, Any]]:
    """Return every saved query in a workgroup, handling API pagination."""
    query_ids: list[str] = []
    paginator = athena.get_paginator("list_named_queries")
    for page in paginator.paginate(WorkGroup=workgroup):
        query_ids.extend(page.get("NamedQueryIds", []))

    queries: list[dict[str, Any]] = []
    for offset in range(0, len(query_ids), 50):
        batch = query_ids[offset : offset + 50]
        queries.extend(athena.batch_get_named_query(NamedQueryIds=batch)["NamedQueries"])
    return queries


def _replace_named_queries(athena: Any, cfg: Config) -> None:
    """Replace only saved queries owned by this module, avoiding duplicates."""
    definitions = _named_queries(cfg)
    managed_names = {item["Name"] for item in definitions}
    for existing in _list_named_queries(athena, cfg.workgroup):
        if existing["Name"] in managed_names:
            athena.delete_named_query(NamedQueryId=existing["NamedQueryId"])

    for definition in definitions:
        response = athena.create_named_query(
            **definition,
            WorkGroup=cfg.workgroup,
        )
        print(f"Saved query: {definition['Name']} ({response['NamedQueryId']})")


def setup() -> None:
    """Validate partitioned tables and configure Athena query infrastructure."""
    cfg = load_config()
    athena, glue, s3, account_id = _clients(cfg)
    _validate_bucket(s3, cfg)

    rds_location = _table_location(glue, cfg, cfg.rds_table)
    ddb_location = _table_location(glue, cfg, cfg.ddb_table)
    _ensure_workgroup(athena, cfg, account_id)
    _replace_named_queries(athena, cfg)

    print("")
    print("Athena setup completed")
    print(f"Workgroup : {cfg.workgroup}")
    print(f"Results   : {cfg.output_location}")
    print(f"Scan limit: {cfg.scan_cutoff_mb:,} MB/query")
    print(f"RDS table : {cfg.database}.{cfg.rds_table} -> {rds_location}")
    print(f"DDB table : {cfg.database}.{cfg.ddb_table} -> {ddb_location}")


def _wait_for_query(athena: Any, execution_id: str, timeout_seconds: int = 120) -> dict[str, Any]:
    """Wait for an Athena query and raise a useful error on failure or timeout."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        execution = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state == "SUCCEEDED":
            return execution
        if state in {"FAILED", "CANCELLED"}:
            reason = execution["Status"].get("StateChangeReason", "No reason returned")
            raise RuntimeError(f"Athena query {state.lower()}: {reason}")
        time.sleep(2)
    athena.stop_query_execution(QueryExecutionId=execution_id)
    raise TimeoutError(f"Athena query exceeded {timeout_seconds} seconds and was cancelled")


def test_query(table: str = "rds") -> list[list[str]]:
    """Run a small preview query and return its rows for notebook reuse."""
    cfg = load_config()
    if table not in {"rds", "ddb"}:
        raise ValueError("table must be either 'rds' or 'ddb'")
    table_name = cfg.rds_table if table == "rds" else cfg.ddb_table
    athena, glue, _s3, _account_id = _clients(cfg)
    _table_location(glue, cfg, table_name)

    query = f'SELECT * FROM "{cfg.database}"."{table_name}" LIMIT 10'
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": cfg.database, "Catalog": "AwsDataCatalog"},
        WorkGroup=cfg.workgroup,
    )
    execution_id = response["QueryExecutionId"]
    execution = _wait_for_query(athena, execution_id)
    result = athena.get_query_results(QueryExecutionId=execution_id, MaxResults=11)
    rows = [
        [column.get("VarCharValue", "") for column in row["Data"]]
        for row in result["ResultSet"]["Rows"]
    ]
    scanned = execution["Statistics"].get("DataScannedInBytes", 0)
    print(f"Query succeeded: {execution_id}")
    print(f"Data scanned   : {scanned / 1024 / 1024:.3f} MB")
    for row in rows:
        print(" | ".join(row))
    return rows


def status() -> None:
    """Print the workgroup, tables and saved queries without changing AWS."""
    cfg = load_config()
    athena, glue, _s3, _account_id = _clients(cfg)
    try:
        workgroup = athena.get_work_group(WorkGroup=cfg.workgroup)["WorkGroup"]
    except athena.exceptions.InvalidRequestException:
        print(f"Workgroup {cfg.workgroup!r} does not exist")
        return

    configuration = workgroup.get("Configuration", {})
    output = configuration.get("ResultConfiguration", {}).get("OutputLocation", "not configured")
    print(f"Workgroup : {workgroup['Name']} ({workgroup['State']})")
    print(f"Results   : {output}")
    print(f"Scan limit: {configuration.get('BytesScannedCutoffPerQuery', 0) / 1024 / 1024:,.0f} MB")
    for table_name in (cfg.rds_table, cfg.ddb_table):
        try:
            location = _table_location(glue, cfg, table_name)
            print(f"Table     : {cfg.database}.{table_name} -> {location}")
        except RuntimeError as error:
            print(f"Table     : {error}")
    for query in _list_named_queries(athena, cfg.workgroup):
        print(f"Saved     : {query['Name']} ({query['NamedQueryId']})")


def destroy() -> None:
    """Delete this module's workgroup and saved queries, preserving S3 and Glue."""
    cfg = load_config()
    athena = boto3.client("athena", region_name=cfg.region)
    try:
        athena.get_work_group(WorkGroup=cfg.workgroup)
    except athena.exceptions.InvalidRequestException:
        print(f"Workgroup {cfg.workgroup!r} does not exist; nothing to delete")
        return

    athena.delete_work_group(WorkGroup=cfg.workgroup, RecursiveDeleteOption=True)
    print(f"Deleted Athena workgroup and its saved queries: {cfg.workgroup}")
    print("Preserved Glue database/tables, S3 source data and Athena result objects.")


def main() -> None:
    """Provide a small CLI while keeping functions importable from notebooks."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        nargs="?",
        default="setup",
        choices=["discover", "setup", "status", "test-rds", "test-ddb", "destroy"],
    )
    args = parser.parse_args()
    if args.command == "discover":
        discover_tables()
    elif args.command == "setup":
        setup()
    elif args.command == "status":
        status()
    elif args.command == "test-rds":
        test_query("rds")
    elif args.command == "test-ddb":
        test_query("ddb")
    else:
        destroy()


if __name__ == "__main__":
    main()
