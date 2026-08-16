"""Deploy and run the Glue job that partitions the initial DMS full load."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
GLUE_SERVICE_POLICY_ARN = (
    "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
)


@dataclass(frozen=True)
class Config:
    """Configuration shared by deployment and execution helpers."""

    region: str
    bucket: str
    raw_prefix: str
    curated_prefix: str
    schema: str
    table: str
    date_column: str
    job_name: str
    glue_database: str
    table_prefix: str


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("<"):
        raise ValueError(f"Missing a real value for {name} in dms/.env")
    return value


def config() -> Config:
    """Load the existing DMS config plus curated destination settings."""
    load_dotenv(ROOT / ".env", override=True)
    dms_prefix = _required("DMS_PREFIX")
    return Config(
        region=_required("AWS_REGION"),
        bucket=_required("S3_BUCKET"),
        raw_prefix=_required("S3_PREFIX").strip("/"),
        curated_prefix=_required("CURATED_PREFIX").strip("/"),
        schema=_required("SOURCE_SCHEMA"),
        table=_required("SOURCE_TABLE"),
        date_column=_required("DATE_COLUMN"),
        job_name=f"{dms_prefix}-partition-initial",
        glue_database=os.getenv("GLUE_DATABASE", "archive").strip(),
        table_prefix=os.getenv("GLUE_TABLE_PREFIX", "orders_rds_").strip(),
    )


def _s3_write_resources(bucket: str, prefix: str) -> list[str]:
    """Return the target objects plus Hadoop directory-marker ARNs.

    Spark's S3 connector can create legacy markers such as
    ``curated_$folder$`` before it writes to ``curated/rds/orders``. These
    marker keys live outside ``curated/rds/orders/*`` and therefore need to be
    granted explicitly.
    """
    parts = [part for part in prefix.strip("/").split("/") if part]
    resources = [f"arn:aws:s3:::{bucket}/{prefix.strip('/')}/*"]
    for index, part in enumerate(parts):
        parent = "/".join(parts[:index])
        marker_key = f"{part}_$folder$"
        if parent:
            marker_key = f"{parent}/{marker_key}"
        resources.append(f"arn:aws:s3:::{bucket}/{marker_key}")
    return resources


def _ensure_role(iam: Any, cfg: Config) -> str:
    role_name = f"{cfg.job_name}-role"
    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "glue.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }
    try:
        role = iam.get_role(RoleName=role_name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust),
        )["Role"]

    # Luôn repair trust policy. Nếu role cùng tên còn lại từ một lần deploy lỗi
    # hoặc được tạo thủ công với principal khác, get_role() vẫn thành công nhưng
    # Glue không thể assume role đó.
    iam.update_assume_role_policy(
        RoleName=role_name,
        PolicyDocument=json.dumps(trust),
    )

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
                "Resource": "*",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket", "s3:GetBucketLocation"],
                "Resource": f"arn:aws:s3:::{cfg.bucket}",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject"],
                "Resource": f"arn:aws:s3:::{cfg.bucket}/{cfg.raw_prefix}/*",
            },
            {
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                "Resource": _s3_write_resources(cfg.bucket, cfg.curated_prefix),
            },
            {
                "Effect": "Allow",
                "Action": [
                    "glue:GetDatabase", "glue:GetDatabases", "glue:GetTable",
                    "glue:GetTables", "glue:CreateTable", "glue:UpdateTable",
                    "glue:BatchCreatePartition", "glue:BatchUpdatePartition",
                ],
                "Resource": "*",
            },
        ],
    }
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName="GluePartitionAccess",
        PolicyDocument=json.dumps(policy),
    )
    # AWS-managed baseline contains the catalog/log/EC2 permissions expected by
    # Glue jobs and crawlers. The inline policy above remains responsible for
    # access to this project's exact S3 prefixes.
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=GLUE_SERVICE_POLICY_ARN,
    )
    # IAM là eventually consistent. get_role() thành công không có nghĩa Glue
    # đã nhìn thấy trust/inline policy mới ngay lập tức.
    time.sleep(10)
    return role["Arn"]


def _ensure_crawler(
    glue: Any,
    crawler_name: str,
    crawler: dict[str, Any],
    timeout_seconds: int = 120,
) -> None:
    """Create/update a crawler, retrying while the IAM role propagates."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            try:
                glue.get_crawler(Name=crawler_name)
                glue.update_crawler(Name=crawler_name, **crawler)
            except glue.exceptions.EntityNotFoundException:
                glue.create_crawler(Name=crawler_name, **crawler)
            return
        except glue.exceptions.InvalidInputException as error:
            message = str(error)
            if "unable to assume provided role" not in message.lower():
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Glue vẫn chưa assume được IAM role sau "
                    f"{timeout_seconds} giây. Kiểm tra trust principal "
                    "glue.amazonaws.com và quyền iam:PassRole của identity deploy."
                ) from error
            print("IAM role chưa propagate tới Glue; thử lại sau 10 giây...")
            time.sleep(10)


def setup_partition_job() -> None:
    """Upload the script and create/update the Glue bootstrap job."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    s3, iam, glue = session.client("s3"), session.client("iam"), session.client("glue")
    s3.head_bucket(Bucket=cfg.bucket)
    role_arn = _ensure_role(iam, cfg)
    script_key = f"{cfg.raw_prefix}/_pipeline/glue_partition_initial.py"
    s3.upload_file(str(ROOT / "glue_partition_job.py"), cfg.bucket, script_key)

    source_path = (
        f"s3://{cfg.bucket}/{cfg.raw_prefix}/{cfg.schema}/{cfg.table}/"
    )
    job = {
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": f"s3://{cfg.bucket}/{script_key}",
            "PythonVersion": "3",
        },
        "DefaultArguments": {
            "--SOURCE_PATH": source_path,
            "--TARGET_PATH": f"s3://{cfg.bucket}/{cfg.curated_prefix}/",
            "--DATE_COLUMN": cfg.date_column,
            "--job-language": "python",
        },
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 2,
        "MaxRetries": 0,
        "Timeout": 60,
    }
    try:
        glue.get_job(JobName=cfg.job_name)
        glue.update_job(JobName=cfg.job_name, JobUpdate=job)
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=cfg.job_name, **job)
    try:
        glue.get_database(Name=cfg.glue_database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": cfg.glue_database})
    crawler_name = f"{cfg.job_name}-crawler"
    crawler = {
        "Role": role_arn,
        "DatabaseName": cfg.glue_database,
        "TablePrefix": cfg.table_prefix,
        "Targets": {
            "S3Targets": [{"Path": f"s3://{cfg.bucket}/{cfg.curated_prefix}/"}],
        },
        "SchemaChangePolicy": {
            "UpdateBehavior": "UPDATE_IN_DATABASE",
            "DeleteBehavior": "LOG",
        },
    }
    _ensure_crawler(glue, crawler_name, crawler)
    print(f"Glue job ready: {cfg.job_name}")
    print(f"Source : {source_path}")
    print(f"Target : s3://{cfg.bucket}/{cfg.curated_prefix}/year=YYYY/month=MM/day=DD/")


def run_partition_job(wait: bool = True) -> str:
    """Start the bootstrap partition job and optionally wait for completion."""
    cfg = config()
    glue = boto3.client("glue", region_name=cfg.region)
    run_id = glue.start_job_run(JobName=cfg.job_name)["JobRunId"]
    print(f"Started Glue run: {run_id}")
    if not wait:
        return run_id

    terminal = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
    while True:
        run = glue.get_job_run(
            JobName=cfg.job_name,
            RunId=run_id,
            PredecessorsIncluded=False,
        )["JobRun"]
        state = run["JobRunState"]
        print(f"Glue state: {state}")
        if state in terminal:
            if state != "SUCCEEDED":
                raise RuntimeError(run.get("ErrorMessage", f"Glue job ended with {state}"))
            _run_crawler(
                glue,
                f"{cfg.job_name}-crawler",
                not_before=run.get("CompletedOn"),
            )
            return run_id
        time.sleep(20)


def _run_crawler(
    glue: Any,
    crawler_name: str,
    not_before: Any | None = None,
) -> None:
    """Ensure a successful crawler run exists for the completed Glue job."""
    crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
    last_crawl = crawler.get("LastCrawl", {})
    last_start = last_crawl.get("StartTime")
    is_current_success = (
        crawler["State"] == "READY"
        and last_crawl.get("Status") == "SUCCEEDED"
        and (
            not_before is None
            or (last_start is not None and last_start >= not_before)
        )
    )
    if is_current_success:
        print(f"Crawler already succeeded: {crawler_name}")
        return
    if crawler["State"] == "READY":
        glue.start_crawler(Name=crawler_name)
        print(f"Started crawler: {crawler_name}")
    while True:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
        if crawler["State"] == "READY":
            status = crawler.get("LastCrawl", {}).get("Status")
            if status != "SUCCEEDED":
                last_crawl = crawler.get("LastCrawl", {})
                error_message = last_crawl.get("ErrorMessage", "No error details")
                log_group = last_crawl.get("LogGroup", "unknown")
                log_stream = last_crawl.get("LogStream", "unknown")
                raise RuntimeError(
                    f"Glue crawler ended with {status}: {error_message}. "
                    f"CloudWatch log: {log_group}/{log_stream}"
                )
            print(f"Crawler succeeded: {crawler_name}")
            return
        print(f"Crawler state: {crawler['State']}")
        time.sleep(15)


def status_partition_job(run_id: str | None = None) -> None:
    """Print job status and finalize the crawler after a successful async run."""
    cfg = config()
    glue = boto3.client("glue", region_name=cfg.region)
    if run_id is None:
        runs = glue.get_job_runs(JobName=cfg.job_name, MaxResults=1)["JobRuns"]
        if not runs:
            print("No Glue runs yet")
            return
        run = runs[0]
    else:
        run = glue.get_job_run(JobName=cfg.job_name, RunId=run_id)["JobRun"]
    print(f"Run   : {run['Id']}")
    print(f"State : {run['JobRunState']}")
    if run.get("ErrorMessage"):
        print(f"Error : {run['ErrorMessage']}")
    if run["JobRunState"] == "SUCCEEDED":
        _run_crawler(
            glue,
            f"{cfg.job_name}-crawler",
            not_before=run.get("CompletedOn"),
        )


def retry_crawler() -> None:
    """Repair the Glue role and retry only the crawler for the latest good job."""
    setup_partition_job()
    cfg = config()
    glue = boto3.client("glue", region_name=cfg.region)
    runs = glue.get_job_runs(JobName=cfg.job_name, MaxResults=25)["JobRuns"]
    successful = [run for run in runs if run["JobRunState"] == "SUCCEEDED"]
    if not successful:
        raise RuntimeError(
            f"No successful Glue run exists for {cfg.job_name}; run the partition job first."
        )
    latest = successful[0]
    print(f"Using successful Glue run: {latest['Id']}")
    _run_crawler(
        glue,
        f"{cfg.job_name}-crawler",
        not_before=latest.get("CompletedOn"),
    )


def crawler_status() -> dict[str, Any]:
    """Print and return crawler state plus the last AWS error details."""
    cfg = config()
    crawler_name = f"{cfg.job_name}-crawler"
    glue = boto3.client("glue", region_name=cfg.region)
    crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
    last = crawler.get("LastCrawl", {})
    print(f"Crawler: {crawler_name}")
    print(f"State  : {crawler['State']}")
    print(f"Last   : {last.get('Status', 'NOT_RUN')}")
    if last.get("ErrorMessage"):
        print(f"Error  : {last['ErrorMessage']}")
    if last.get("LogGroup") or last.get("LogStream"):
        print(f"Log    : {last.get('LogGroup', '')}/{last.get('LogStream', '')}")
    return crawler


def destroy_partition_job() -> None:
    """Delete Glue bootstrap compute resources while preserving all S3 data."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    glue, iam = session.client("glue"), session.client("iam")
    crawler_name = f"{cfg.job_name}-crawler"
    try:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
        if crawler["State"] != "READY":
            try:
                glue.stop_crawler(Name=crawler_name)
            except glue.exceptions.CrawlerNotRunningException:
                pass
            deadline = time.monotonic() + 180
            while glue.get_crawler(Name=crawler_name)["Crawler"]["State"] != "READY":
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Crawler did not stop: {crawler_name}")
                time.sleep(5)
        glue.delete_crawler(Name=crawler_name)
        print(f"Deleted crawler: {crawler_name}")
    except glue.exceptions.EntityNotFoundException:
        pass
    try:
        runs = glue.get_job_runs(JobName=cfg.job_name, MaxResults=25)["JobRuns"]
        active_ids = [
            run["Id"] for run in runs
            if run["JobRunState"] in {"STARTING", "RUNNING", "WAITING"}
        ]
        if active_ids:
            glue.batch_stop_job_run(JobName=cfg.job_name, JobRunIds=active_ids)
            print(f"Stopping {len(active_ids)} Glue run(s): {cfg.job_name}")
    except glue.exceptions.EntityNotFoundException:
        pass
    try:
        glue.delete_job(JobName=cfg.job_name)
        print(f"Deleted Glue job: {cfg.job_name}")
    except glue.exceptions.EntityNotFoundException:
        pass

    role_name = f"{cfg.job_name}-role"
    try:
        iam.detach_role_policy(
            RoleName=role_name,
            PolicyArn=GLUE_SERVICE_POLICY_ARN,
        )
    except iam.exceptions.NoSuchEntityException:
        pass
    try:
        iam.delete_role_policy(RoleName=role_name, PolicyName="GluePartitionAccess")
    except iam.exceptions.NoSuchEntityException:
        pass
    try:
        iam.delete_role(RoleName=role_name)
        print(f"Deleted IAM role: {role_name}")
    except iam.exceptions.NoSuchEntityException:
        pass
    print("Preserved DMS raw data, curated data, Glue database and catalog tables.")


if __name__ == "__main__":
    setup_partition_job()
    run_partition_job(wait=True)
