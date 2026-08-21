"""Idempotent boto3 deployment for the daily RDS -> S3 Glue pipeline."""

from __future__ import annotations

import io
import json
import os
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Config:
    region: str; name: str; schedule: str; enable_schedule: bool
    host: str; port: int; database: str; username: str; password: str
    schema: str; table: str; date_column: str; retention_days: int; dms_prefix: str
    bucket: str; raw_prefix: str; curated_prefix: str
    glue_database: str; glue_table: str; subnet_id: str; security_groups: list[str]
    primary_key: str; child_tables: tuple[tuple[str, str], ...]; checksum_column: str
    enable_purge: bool; purge_dry_run: bool; purge_batch_size: int; purge_vacuum: bool


def _identifier(name: str, value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"{name} is not a valid PostgreSQL identifier: {value!r}")
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
            _identifier("PURGE_CHILD_TABLES table", child.strip()),
            _identifier("PURGE_CHILD_TABLES column", column.strip()),
        ))
    return tuple(pairs)


def _sibling_path(path: str, name: str) -> str:
    """Folder cùng cấp với ``path``, đổi segment cuối thành ``name``."""
    return f"{path.rstrip('/').rsplit('/', 1)[0]}/{name}/"


def _flag(key: str, default: str = "false") -> bool:
    return os.getenv(key, default).strip().lower() == "true"


def config() -> Config:
    load_dotenv(ROOT / ".env", override=True)

    def required(key: str) -> str:
        value = os.getenv(key, "").strip()
        if not value or value.startswith("<"):
            raise ValueError(f"Missing a real value for {key} in rds_daily_pipeline/.env")
        return value

    return Config(
        region=required("AWS_REGION"), name=required("PIPELINE_NAME"),
        schedule=required("SCHEDULE_EXPRESSION"), host=required("RDS_HOST"),
        enable_schedule=os.getenv("ENABLE_SCHEDULE", "false").strip().lower() == "true",
        port=int(required("RDS_PORT")), database=required("RDS_DATABASE"),
        username=required("RDS_USERNAME"), password=required("RDS_PASSWORD"),
        schema=_identifier("SOURCE_SCHEMA", required("SOURCE_SCHEMA")),
        table=_identifier("SOURCE_TABLE", required("SOURCE_TABLE")),
        date_column=_identifier("DATE_COLUMN", required("DATE_COLUMN")),
        retention_days=int(required("ARCHIVE_RETENTION_DAYS")),
        dms_prefix=required("DMS_PREFIX"), bucket=required("S3_BUCKET"),
        raw_prefix=required("RAW_PREFIX").strip("/"),
        curated_prefix=required("CURATED_PREFIX").strip("/"),
        glue_database=required("GLUE_DATABASE"), glue_table=required("GLUE_TABLE"),
        subnet_id=os.getenv("SUBNET_ID", "").strip(),
        security_groups=[
            x.strip() for x in os.getenv("SECURITY_GROUP_IDS", "").split(",")
            if x.strip()
        ],
        primary_key=_identifier("PRIMARY_KEY", required("PRIMARY_KEY")),
        child_tables=_child_tables(os.getenv("PURGE_CHILD_TABLES", "")),
        checksum_column=_identifier(
            "PURGE_CHECKSUM_COLUMN", os.getenv("PURGE_CHECKSUM_COLUMN", "").strip()
        ) if os.getenv("PURGE_CHECKSUM_COLUMN", "").strip() else "",
        enable_purge=_flag("ENABLE_PURGE"),
        purge_dry_run=_flag("PURGE_DRY_RUN", "true"),
        purge_batch_size=int(os.getenv("PURGE_BATCH_SIZE", "500")),
        purge_vacuum=_flag("PURGE_VACUUM"),
    )


def _role(iam: Any, name: str, service: str, statements: list[dict[str, Any]]) -> str:
    trust = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"
    }]}
    created = False
    try:
        role = iam.get_role(RoleName=name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]
        created = True
    # Repair an existing role left by a partial/older deployment.
    iam.update_assume_role_policy(
        RoleName=name,
        PolicyDocument=json.dumps(trust),
    )
    iam.put_role_policy(
        RoleName=name, PolicyName=f"{name}-policy",
        PolicyDocument=json.dumps({"Version": "2012-10-17", "Statement": statements}),
    )
    if created:
        time.sleep(10)
    return role["Arn"]


def _create_lambda_with_retry(
    lamb: Any,
    arguments: dict[str, Any],
    timeout_seconds: int = 120,
) -> None:
    """Create Lambda after IAM trust propagation, retrying only that known error."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            lamb.create_function(**arguments)
            return
        except lamb.exceptions.InvalidParameterValueException as error:
            if "cannot be assumed by Lambda" not in str(error):
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Lambda still cannot assume its IAM role after "
                    f"{timeout_seconds} seconds. Check the role trust principal "
                    "lambda.amazonaws.com."
                ) from error
            print("IAM role has not propagated to Lambda; retrying in 10 seconds...")
            time.sleep(10)


def _zip(path: Path, archive_name: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(path, archive_name)
    return buffer.getvalue()


def _bootstrap_metadata(cfg: Config, dms: Any, glue: Any) -> dict[str, str]:
    """Read the initial cutoff and curated target from deployed AWS resources."""
    task_id = f"{cfg.dms_prefix}-task"
    tasks = dms.describe_replication_tasks(
        Filters=[{"Name": "replication-task-id", "Values": [task_id]}]
    )["ReplicationTasks"]
    if not tasks:
        raise RuntimeError(f"DMS bootstrap task not found: {task_id}")
    task = tasks[0]
    stats = task.get("ReplicationTaskStats", {})
    if task["Status"] != "stopped" or stats.get("TablesErrored", 0) > 0:
        raise RuntimeError(
            f"DMS bootstrap is not successfully completed: status={task['Status']}, "
            f"tables_errored={stats.get('TablesErrored', 0)}"
        )

    mappings = json.loads(task["TableMappings"])
    cutoff: str | None = None
    for rule in mappings.get("rules", []):
        locator = rule.get("object-locator", {})
        if (
            rule.get("rule-type") != "selection"
            or locator.get("schema-name") != cfg.schema
            or locator.get("table-name") != cfg.table
        ):
            continue
        for source_filter in rule.get("filters", []):
            if source_filter.get("column-name") != cfg.date_column:
                continue
            for condition in source_filter.get("filter-conditions", []):
                if condition.get("filter-operator") == "lte":
                    cutoff = condition.get("value")
    if not cutoff:
        raise RuntimeError(
            f"Cannot find an lte cutoff for {cfg.date_column} in DMS task mappings"
        )
    watermark = cutoff if "T" in cutoff else f"{cutoff}T00:00:00Z"

    bootstrap_job = f"{cfg.dms_prefix}-partition-initial"
    try:
        job = glue.get_job(JobName=bootstrap_job)["Job"]
    except glue.exceptions.EntityNotFoundException as error:
        raise RuntimeError(
            f"Glue bootstrap job not found: {bootstrap_job}. "
            "Run dms/run_setup.ipynb first."
        ) from error
    actual_target = job.get("DefaultArguments", {}).get("--TARGET_PATH", "").rstrip("/")
    expected_target = f"s3://{cfg.bucket}/{cfg.curated_prefix}".rstrip("/")
    if actual_target != expected_target:
        raise ValueError(
            "Initial and daily curated locations differ: "
            f"bootstrap={actual_target!r}, daily={expected_target!r}"
        )
    runs = glue.get_job_runs(JobName=bootstrap_job, MaxResults=1)["JobRuns"]
    if not runs or runs[0]["JobRunState"] != "SUCCEEDED":
        state = runs[0]["JobRunState"] if runs else "NOT_RUN"
        raise RuntimeError(
            f"Glue bootstrap job has not succeeded: {bootstrap_job}, state={state}"
        )
    crawler_name = f"{bootstrap_job}-crawler"
    try:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
    except glue.exceptions.EntityNotFoundException as error:
        raise RuntimeError(f"Glue bootstrap crawler not found: {crawler_name}") from error
    if crawler.get("LastCrawl", {}).get("Status") != "SUCCEEDED":
        raise RuntimeError(
            f"Glue bootstrap crawler has not succeeded: {crawler_name}. "
            "In dms/run_pipeline.ipynb, reload partition_initial "
            "and call status_partition_job() to finalize the crawler."
        )
    return {
        "watermark": watermark,
        "date_column": cfg.date_column,
        "curated_path": expected_target,
        "bootstrap_task": task_id,
    }


def _glue_network(cfg: Config, dms: Any) -> tuple[str, list[str]]:
    """Resolve Glue subnet/security groups, reusing DMS networking by default."""
    if cfg.subnet_id and cfg.security_groups:
        print("Using explicit Glue network overrides from .env")
        return cfg.subnet_id, cfg.security_groups

    instance_id = f"{cfg.dms_prefix}-instance"
    instances = dms.describe_replication_instances(
        Filters=[{"Name": "replication-instance-id", "Values": [instance_id]}]
    )["ReplicationInstances"]
    if not instances:
        missing = []
        if not cfg.subnet_id:
            missing.append("SUBNET_ID")
        if not cfg.security_groups:
            missing.append("SECURITY_GROUP_IDS")
        raise RuntimeError(
            f"DMS instance not found: {instance_id}. Set {', '.join(missing)} "
            "explicitly, or keep the DMS instance until daily setup completes."
        )

    instance = instances[0]
    subnet_items = instance.get("ReplicationSubnetGroup", {}).get("Subnets", [])
    derived_subnets = [
        item["SubnetIdentifier"] for item in subnet_items
        if item.get("SubnetStatus", "Active").lower() == "active"
    ]
    derived_security_groups = [
        item["VpcSecurityGroupId"]
        for item in instance.get("VpcSecurityGroups", [])
        if item.get("Status", "active").lower() == "active"
    ]
    subnet_id = cfg.subnet_id or (derived_subnets[0] if derived_subnets else "")
    security_groups = cfg.security_groups or derived_security_groups
    if not subnet_id or not security_groups:
        raise RuntimeError(
            f"Cannot derive subnet/security groups from DMS instance {instance_id}"
        )
    print(f"Glue network: subnet={subnet_id}, security_groups={security_groups}")
    return subnet_id, security_groups


def _seed_control_watermark(
    cfg: Config,
    ddb: Any,
    dms: Any,
    glue: Any,
    control_table: str,
) -> None:
    """Seed the DMS cutoff once; never replace a watermark already in use."""
    key = {"pipeline": {"S": cfg.name}}
    existing = ddb.get_item(
        TableName=control_table, Key=key, ConsistentRead=True
    ).get("Item")
    if existing:
        existing_column = existing.get("date_column", {}).get("S")
        existing_path = existing.get("curated_path", {}).get("S")
        expected_path = f"s3://{cfg.bucket}/{cfg.curated_prefix}".rstrip("/")
        if existing_column and existing_column != cfg.date_column:
            raise ValueError(
                f"Control table uses DATE_COLUMN={existing_column}, config uses {cfg.date_column}"
            )
        if existing_path and existing_path != expected_path:
            raise ValueError(
                f"Control table uses curated_path={existing_path}, config uses {expected_path}"
            )
        if not existing_column or not existing_path:
            # Upgrade control items created by the older manual-watermark version.
            metadata = _bootstrap_metadata(cfg, dms, glue)
            existing_watermark = datetime.fromisoformat(
                existing["watermark"]["S"].replace("Z", "+00:00")
            )
            bootstrap_watermark = datetime.fromisoformat(
                metadata["watermark"].replace("Z", "+00:00")
            )
            if existing_watermark < bootstrap_watermark:
                raise ValueError(
                    "Existing control watermark is earlier than the DMS cutoff; "
                    "continuing would duplicate initial data."
                )
            ddb.update_item(
                TableName=control_table,
                Key=key,
                UpdateExpression=(
                    "SET date_column = :column, curated_path = :path, "
                    "bootstrap_task = :task"
                ),
                ExpressionAttributeValues={
                    ":column": {"S": metadata["date_column"]},
                    ":path": {"S": metadata["curated_path"]},
                    ":task": {"S": metadata["bootstrap_task"]},
                },
            )
        print(f"Watermark already exists: {existing['watermark']['S']}")
        return

    metadata = _bootstrap_metadata(cfg, dms, glue)
    ddb.put_item(
        TableName=control_table,
        Item={"pipeline": {"S": cfg.name}, **{
            name: {"S": value} for name, value in metadata.items()
        }},
        ConditionExpression="attribute_not_exists(pipeline)",
    )
    print(f"Seeded watermark from DMS task: {metadata['watermark']}")
    print(f"Shared curated path: {metadata['curated_path']}")


def setup() -> None:
    """Create/update network connection, Glue job, state machine and daily schedule."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    account = session.client("sts").get_caller_identity()["Account"]
    iam, ec2, ddb, s3, dms = (
        session.client(x) for x in ("iam", "ec2", "dynamodb", "s3", "dms")
    )
    glue, lamb, sfn, events = (session.client(x) for x in ("glue", "lambda", "stepfunctions", "events"))
    s3.head_bucket(Bucket=cfg.bucket)

    # Validate the completed initial pipeline before creating daily compute.
    control_table = f"{cfg.name}-control"
    try:
        ddb.describe_table(TableName=control_table)
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(
            TableName=control_table, BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pipeline", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pipeline", "KeyType": "HASH"}],
        )
        ddb.get_waiter("table_exists").wait(TableName=control_table)
    _seed_control_watermark(cfg, ddb, dms, glue, control_table)

    subnet_id, security_groups = _glue_network(cfg, dms)
    subnet = ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"][0]
    availability_zone = subnet["AvailabilityZone"]
    connection_name = f"{cfg.name}-postgres"
    connection = {
        "Name": connection_name, "ConnectionType": "JDBC",
        "ConnectionProperties": {
            "JDBC_CONNECTION_URL": f"jdbc:postgresql://{cfg.host}:{cfg.port}/{cfg.database}",
            "USERNAME": cfg.username, "PASSWORD": cfg.password,
            "JDBC_ENFORCE_SSL": "true",
        },
        "PhysicalConnectionRequirements": {
            "SubnetId": subnet_id, "SecurityGroupIdList": security_groups,
            "AvailabilityZone": availability_zone,
        },
    }
    try:
        glue.get_connection(Name=connection_name)
        glue.update_connection(Name=connection_name, ConnectionInput=connection)
    except glue.exceptions.EntityNotFoundException:
        glue.create_connection(ConnectionInput=connection)

    lambda_role = _role(iam, f"{cfg.name}-lambda-role", "lambda.amazonaws.com", [
        {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem"], "Resource": f"arn:aws:dynamodb:{cfg.region}:{account}:table/{control_table}"},
    ])
    function_name = f"{cfg.name}-coordinator"
    environment = {"Variables": {
        "CONTROL_TABLE": control_table, "PIPELINE_NAME": cfg.name,
        "RETENTION_DAYS": str(cfg.retention_days),
    }}
    package = _zip(ROOT / "coordinator.py", "coordinator.py")
    try:
        lamb.get_function(FunctionName=function_name)
        lamb.update_function_code(FunctionName=function_name, ZipFile=package, Publish=True)
        lamb.get_waiter("function_updated_v2").wait(FunctionName=function_name)
        lamb.update_function_configuration(
            FunctionName=function_name, Role=lambda_role, Runtime="python3.11",
            Handler="coordinator.handler", Timeout=30, Environment=environment,
        )
    except lamb.exceptions.ResourceNotFoundException:
        _create_lambda_with_retry(lamb, {
            "FunctionName": function_name, "Role": lambda_role, "Runtime": "python3.11",
            "Handler": "coordinator.handler", "Timeout": 30, "Environment": environment,
            "Code": {"ZipFile": package}, "Publish": True,
        })
    function_arn = f"arn:aws:lambda:{cfg.region}:{account}:function:{function_name}"

    script_key = f"{cfg.raw_prefix}/_pipeline/glue_job.py"
    purge_key = f"{cfg.raw_prefix}/_pipeline/purge_job.py"
    s3.upload_file(str(ROOT / "glue_job.py"), cfg.bucket, script_key)
    s3.upload_file(str(ROOT / "purge_job.py"), cfg.bucket, purge_key)
    glue_role = _role(iam, f"{cfg.name}-glue-role", "glue.amazonaws.com", [
        {"Effect": "Allow", "Action": ["logs:*", "glue:GetConnection", "glue:GetConnections"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["ec2:CreateNetworkInterface", "ec2:DeleteNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeVpcEndpoints", "ec2:DescribeRouteTables", "ec2:DescribeVpcAttribute", "ec2:CreateTags"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"], "Resource": [f"arn:aws:s3:::{cfg.bucket}", f"arn:aws:s3:::{cfg.bucket}/*"]},
    ])
    raw_path = f"s3://{cfg.bucket}/{cfg.raw_prefix}/"
    curated_path = f"s3://{cfg.bucket}/{cfg.curated_prefix}/"
    child_spec = ",".join(f"{child}:{column}" for child, column in cfg.child_tables)
    shared = {
        "--CONNECTION_NAME": connection_name, "--SOURCE_SCHEMA": cfg.schema,
        "--SOURCE_TABLE": cfg.table, "--DATE_COLUMN": cfg.date_column,
        "--PRIMARY_KEY": cfg.primary_key, "--CHILD_TABLES": child_spec or "none",
        "--job-language": "python",
    }
    job_name = f"{cfg.name}-extract-transform"
    job_update = {
        "Role": glue_role,
        "Command": {"Name": "glueetl", "ScriptLocation": f"s3://{cfg.bucket}/{script_key}", "PythonVersion": "3"},
        "DefaultArguments": {
            **shared, "--RAW_PATH": raw_path, "--CURATED_PATH": curated_path,
        },
        "Connections": {"Connections": [connection_name]},
        "GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 2,
        "MaxRetries": 0, "Timeout": 60,
    }
    purge_job_name = f"{cfg.name}-purge-source"
    purge_update = {
        "Role": glue_role,
        "Command": {"Name": "glueetl", "ScriptLocation": f"s3://{cfg.bucket}/{purge_key}", "PythonVersion": "3"},
        "DefaultArguments": {
            **shared, "--CURATED_PATH": curated_path,
            "--CHECKSUM_COLUMN": cfg.checksum_column or "none",
            "--DRY_RUN": str(cfg.purge_dry_run).lower(),
            "--BATCH_SIZE": str(cfg.purge_batch_size),
            "--RUN_VACUUM": str(cfg.purge_vacuum).lower(),
        },
        "Connections": {"Connections": [connection_name]},
        "GlueVersion": "4.0", "WorkerType": "G.1X", "NumberOfWorkers": 2,
        "MaxRetries": 0, "Timeout": 120,
    }
    for name, update in ((job_name, job_update), (purge_job_name, purge_update)):
        try:
            glue.get_job(JobName=name)
            glue.update_job(JobName=name, JobUpdate=update)
        except glue.exceptions.EntityNotFoundException:
            glue.create_job(Name=name, **update)

    try:
        glue.get_database(Name=cfg.glue_database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": cfg.glue_database})
    crawler_name = f"{cfg.name}-catalog"
    crawler_targets = [{"Path": curated_path}] + [
        {"Path": _sibling_path(curated_path, child)} for child, _ in cfg.child_tables
    ]
    crawler = {
        "Role": glue_role, "DatabaseName": cfg.glue_database,
        "Targets": {"S3Targets": crawler_targets},
        "TablePrefix": f"{cfg.glue_table}_",
        "SchemaChangePolicy": {"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"},
    }
    try:
        glue.get_crawler(Name=crawler_name)
        glue.update_crawler(Name=crawler_name, **crawler)
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(Name=crawler_name, **crawler)

    sfn_role = _role(iam, f"{cfg.name}-sfn-role", "states.amazonaws.com", [
        {"Effect": "Allow", "Action": "lambda:InvokeFunction", "Resource": function_arn},
        {"Effect": "Allow", "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun", "glue:StartCrawler", "glue:GetCrawler"], "Resource": "*"},
        {"Effect": "Allow", "Action": ["events:PutTargets", "events:PutRule", "events:DescribeRule"], "Resource": "*"},
    ])
    definition = _state_machine(
        function_arn, job_name, crawler_name,
        purge_job_name if cfg.enable_purge else "",
    )
    state_name = f"{cfg.name}-workflow"
    state_arn = f"arn:aws:states:{cfg.region}:{account}:stateMachine:{state_name}"
    try:
        sfn.describe_state_machine(stateMachineArn=state_arn)
        sfn.update_state_machine(stateMachineArn=state_arn, definition=json.dumps(definition), roleArn=sfn_role)
    except sfn.exceptions.StateMachineDoesNotExist:
        sfn.create_state_machine(name=state_name, definition=json.dumps(definition), roleArn=sfn_role, type="STANDARD")

    events_role = _role(iam, f"{cfg.name}-events-role", "events.amazonaws.com", [
        {"Effect": "Allow", "Action": "states:StartExecution", "Resource": state_arn}
    ])
    rule = f"{cfg.name}-schedule"
    rule_state = "ENABLED" if cfg.enable_schedule else "DISABLED"
    events.put_rule(Name=rule, ScheduleExpression=cfg.schedule, State=rule_state)
    events.put_targets(Rule=rule, Targets=[{"Id": "daily", "Arn": state_arn, "RoleArn": events_role}])
    print(f"Daily RDS pipeline ready: {state_arn}")
    print(f"Schedule: {rule_state} ({cfg.schedule})")
    if not cfg.enable_purge:
        print("Purge: DISABLED (chỉ archive, RDS giữ nguyên dữ liệu)")
    else:
        mode = "DRY RUN" if cfg.purge_dry_run else "DELETE THẬT"
        print(f"Purge: ENABLED, {mode}, child={child_spec or 'none'}")


def run_once() -> str:
    """Start one daily workflow manually and return its execution ARN."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    account = session.client("sts").get_caller_identity()["Account"]
    state_machine_arn = (
        f"arn:aws:states:{cfg.region}:{account}:stateMachine:{cfg.name}-workflow"
    )
    response = session.client("stepfunctions").start_execution(
        stateMachineArn=state_machine_arn,
    )
    execution_arn = response["executionArn"]
    print(f"Execution started: {execution_arn}")
    return execution_arn


def execution_status(execution_arn: str) -> dict[str, Any]:
    """Print and return the current status of a workflow execution."""
    cfg = config()
    sfn = boto3.client("stepfunctions", region_name=cfg.region)
    execution = sfn.describe_execution(executionArn=execution_arn)
    print(f"Status : {execution['status']}")
    print(f"Started: {execution['startDate']}")
    if execution.get("stopDate"):
        print(f"Stopped: {execution['stopDate']}")
    if execution.get("error"):
        print(f"Error  : {execution['error']}")
    if execution.get("cause"):
        print(f"Cause  : {execution['cause']}")
    return execution


def _delete_role(iam: Any, role_name: str) -> None:
    """Delete one pipeline role and its known inline policy if present."""
    try:
        iam.delete_role_policy(RoleName=role_name, PolicyName=f"{role_name}-policy")
    except iam.exceptions.NoSuchEntityException:
        pass
    try:
        iam.delete_role(RoleName=role_name)
        print(f"Deleted IAM role: {role_name}")
    except iam.exceptions.NoSuchEntityException:
        pass


def _stop_job_runs(glue: Any, job_name: str) -> None:
    """Stop active Glue runs before deleting the job and role."""
    try:
        runs = glue.get_job_runs(JobName=job_name, MaxResults=25)["JobRuns"]
    except glue.exceptions.EntityNotFoundException:
        return
    active_ids = [
        run["Id"] for run in runs
        if run["JobRunState"] in {"STARTING", "RUNNING", "WAITING"}
    ]
    if active_ids:
        glue.batch_stop_job_run(JobName=job_name, JobRunIds=active_ids)
        print(f"Stopping {len(active_ids)} Glue run(s): {job_name}")


def _delete_crawler(glue: Any, crawler_name: str) -> None:
    """Stop a running crawler, wait for READY, then delete it."""
    try:
        crawler = glue.get_crawler(Name=crawler_name)["Crawler"]
    except glue.exceptions.EntityNotFoundException:
        return
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
    print(f"Deleted Glue crawler: {crawler_name}")


def destroy() -> None:
    """Delete daily pipeline infrastructure; preserve RDS, S3 data and catalog tables."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    account = session.client("sts").get_caller_identity()["Account"]
    events, sfn = session.client("events"), session.client("stepfunctions")
    glue, lamb = session.client("glue"), session.client("lambda")
    ddb, iam = session.client("dynamodb"), session.client("iam")
    rule = f"{cfg.name}-schedule"
    try:
        events.remove_targets(Rule=rule, Ids=["daily"], Force=True)
        events.delete_rule(Name=rule, Force=True)
        print(f"Deleted EventBridge rule: {rule}")
    except events.exceptions.ResourceNotFoundException:
        pass

    state_arn = f"arn:aws:states:{cfg.region}:{account}:stateMachine:{cfg.name}-workflow"
    try:
        for execution in sfn.list_executions(
            stateMachineArn=state_arn, statusFilter="RUNNING"
        )["executions"]:
            sfn.stop_execution(
                executionArn=execution["executionArn"],
                cause="Pipeline destroy requested",
            )
        sfn.delete_state_machine(stateMachineArn=state_arn)
        print(f"Deleted state machine: {cfg.name}-workflow")
    except sfn.exceptions.StateMachineDoesNotExist:
        pass

    _delete_crawler(glue, f"{cfg.name}-catalog")
    for job_name in (f"{cfg.name}-extract-transform", f"{cfg.name}-purge-source"):
        _stop_job_runs(glue, job_name)
        try:
            glue.delete_job(JobName=job_name)
            print(f"Deleted Glue job: {job_name}")
        except glue.exceptions.EntityNotFoundException:
            pass
    try:
        glue.delete_connection(ConnectionName=f"{cfg.name}-postgres")
        print(f"Deleted Glue connection: {cfg.name}-postgres")
    except glue.exceptions.EntityNotFoundException:
        pass
    try:
        lamb.delete_function(FunctionName=f"{cfg.name}-coordinator")
        print(f"Deleted Lambda: {cfg.name}-coordinator")
    except lamb.exceptions.ResourceNotFoundException:
        pass
    try:
        ddb.delete_table(TableName=f"{cfg.name}-control")
        print(f"Deleting control table: {cfg.name}-control")
    except ddb.exceptions.ResourceNotFoundException:
        pass
    for suffix in ("lambda-role", "glue-role", "sfn-role", "events-role"):
        _delete_role(iam, f"{cfg.name}-{suffix}")
    print("Preserved RDS, S3 bucket/objects, Glue database and catalog tables.")


def _state_machine(
    function_arn: str, job_name: str, crawler_name: str, purge_job_name: str = ""
) -> dict[str, Any]:
    invoke = "arn:aws:states:::lambda:invoke"
    # Purge nằm giữa crawler và commit: nếu xóa lỗi, watermark không commit và
    # window đó được archive + verify lại ở lần chạy sau.
    after_crawler = "PurgeSource" if purge_job_name else "Commit"
    purge_states: dict[str, Any] = {} if not purge_job_name else {
        "PurgeSource": {
            "Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync",
            "Parameters": {"JobName": purge_job_name, "Arguments": {
                "--WINDOW_FROM.$": "$.window_from", "--WINDOW_TO.$": "$.window_to",
            }},
            "ResultPath": "$.purge", "Next": "Commit",
        }
    }
    return {"StartAt": "Prepare", "States": {**purge_states,
        "Prepare": {"Type": "Task", "Resource": invoke, "Parameters": {"FunctionName": function_arn, "Payload": {"action": "prepare"}}, "OutputPath": "$.Payload", "Next": "Skip?"},
        "Skip?": {"Type": "Choice", "Choices": [{"Variable": "$.skip", "BooleanEquals": True, "Next": "Done"}], "Default": "RunGlue"},
        "RunGlue": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": job_name, "Arguments": {"--WINDOW_FROM.$": "$.window_from", "--WINDOW_TO.$": "$.window_to", "--BATCH_DATE.$": "$.batch_date"}}, "ResultPath": "$.glue", "Next": "StartCrawler"},
        "StartCrawler": {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:glue:startCrawler", "Parameters": {"Name": crawler_name}, "ResultPath": "$.crawler", "Next": "WaitCrawler"},
        "WaitCrawler": {"Type": "Wait", "Seconds": 30, "Next": "CrawlerStatus"},
        "CrawlerStatus": {"Type": "Task", "Resource": "arn:aws:states:::aws-sdk:glue:getCrawler", "Parameters": {"Name": crawler_name}, "ResultPath": "$.crawler_status", "Next": "CrawlerDone?"},
        "CrawlerDone?": {"Type": "Choice", "Choices": [{"Variable": "$.crawler_status.Crawler.State", "StringEquals": "READY", "Next": "CrawlerSucceeded?"}], "Default": "WaitCrawler"},
        "CrawlerSucceeded?": {"Type": "Choice", "Choices": [{"Variable": "$.crawler_status.Crawler.LastCrawl.Status", "StringEquals": "SUCCEEDED", "Next": after_crawler}], "Default": "CrawlerFailed"},
        "CrawlerFailed": {"Type": "Fail", "Cause": "Glue crawler did not succeed"},
        "Commit": {"Type": "Task", "Resource": invoke, "Parameters": {"FunctionName": function_arn, "Payload": {"action": "commit", "window_to.$": "$.window_to", "batch_date.$": "$.batch_date"}}, "OutputPath": "$.Payload", "End": True},
        "Done": {"Type": "Succeed"},
    }}


if __name__ == "__main__":
    setup()
