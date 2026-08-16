"""Deploy the DynamoDB export -> S3 -> daily Glue pipeline with boto3."""

from __future__ import annotations

import io
import json
import os
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent


@dataclass(frozen=True)
class Config:
    region: str
    table: str
    bucket: str
    raw_prefix: str
    curated_prefix: str
    name: str
    schedule: str
    database: str
    catalog_table: str


def config() -> Config:
    """Load and validate deployment configuration."""
    load_dotenv(ROOT / ".env", override=True)

    def required(key: str) -> str:
        value = os.getenv(key, "").strip()
        if not value or value.startswith("<"):
            raise ValueError(f"Missing a real value for {key} in dynamodb_pipeline/.env")
        return value

    return Config(
        region=required("AWS_REGION"),
        table=required("DDB_TABLE"),
        bucket=required("S3_BUCKET"),
        raw_prefix=required("RAW_PREFIX").strip("/"),
        curated_prefix=required("CURATED_PREFIX").strip("/"),
        name=required("PIPELINE_NAME"),
        schedule=required("SCHEDULE_EXPRESSION"),
        database=required("GLUE_DATABASE"),
        catalog_table=required("GLUE_TABLE"),
    )


def _role(iam: Any, name: str, service: str, policy: dict[str, Any]) -> str:
    trust = {"Version": "2012-10-17", "Statement": [{
        "Effect": "Allow", "Principal": {"Service": service}, "Action": "sts:AssumeRole"
    }]}
    created = False
    try:
        role = iam.get_role(RoleName=name)["Role"]
    except iam.exceptions.NoSuchEntityException:
        role = iam.create_role(
            RoleName=name, AssumeRolePolicyDocument=json.dumps(trust)
        )["Role"]
        created = True
    iam.update_assume_role_policy(
        RoleName=name,
        PolicyDocument=json.dumps(trust),
    )
    iam.put_role_policy(
        RoleName=name, PolicyName=f"{name}-policy", PolicyDocument=json.dumps(policy)
    )
    if created:
        time.sleep(10)
    return role["Arn"]


def _create_lambda_with_retry(
    lambda_client: Any,
    arguments: dict[str, Any],
    timeout_seconds: int = 120,
) -> None:
    """Retry Lambda creation while its new IAM trust policy propagates."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        try:
            lambda_client.create_function(**arguments)
            return
        except lambda_client.exceptions.InvalidParameterValueException as error:
            if "cannot be assumed by Lambda" not in str(error):
                raise
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    "Lambda still cannot assume its IAM role after "
                    f"{timeout_seconds} seconds. Check lambda.amazonaws.com trust."
                ) from error
            print("IAM role has not propagated to Lambda; retrying in 10 seconds...")
            time.sleep(10)


def _lambda_zip() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(ROOT / "orchestrator.py", "orchestrator.py")
    return buffer.getvalue()


def setup() -> None:
    """Create or update all resources; safe to run repeatedly."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    account = session.client("sts").get_caller_identity()["Account"]
    iam, ddb, s3 = session.client("iam"), session.client("dynamodb"), session.client("s3")
    lambda_client = session.client("lambda")
    glue, sfn, events = session.client("glue"), session.client("stepfunctions"), session.client("events")

    table = ddb.describe_table(TableName=cfg.table)["Table"]
    table_arn = table["TableArn"]
    pitr = ddb.describe_continuous_backups(TableName=cfg.table)["ContinuousBackupsDescription"]
    if pitr["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"] != "ENABLED":
        raise RuntimeError("DynamoDB PITR must be ENABLED before native export")
    s3.head_bucket(Bucket=cfg.bucket)

    control_table = f"{cfg.name}-control"
    try:
        ddb.describe_table(TableName=control_table)
    except ddb.exceptions.ResourceNotFoundException:
        ddb.create_table(
            TableName=control_table,
            BillingMode="PAY_PER_REQUEST",
            AttributeDefinitions=[{"AttributeName": "pipeline", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "pipeline", "KeyType": "HASH"}],
        )
        ddb.get_waiter("table_exists").wait(TableName=control_table)

    lambda_role = _role(iam, f"{cfg.name}-lambda-role", "lambda.amazonaws.com", {
        "Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["dynamodb:ExportTableToPointInTime", "dynamodb:DescribeExport"], "Resource": [table_arn, f"{table_arn}/export/*"]},
            {"Effect": "Allow", "Action": ["dynamodb:GetItem", "dynamodb:PutItem"], "Resource": f"arn:aws:dynamodb:{cfg.region}:{account}:table/{control_table}"},
            {"Effect": "Allow", "Action": ["s3:AbortMultipartUpload", "s3:PutObject", "s3:PutObjectAcl"], "Resource": f"arn:aws:s3:::{cfg.bucket}/{cfg.raw_prefix}/*"},
        ]
    })
    function_name = f"{cfg.name}-orchestrator"
    environment = {"Variables": {
        "CONTROL_TABLE": control_table, "PIPELINE_NAME": cfg.name,
        "TABLE_ARN": table_arn, "S3_BUCKET": cfg.bucket, "RAW_PREFIX": cfg.raw_prefix,
    }}
    package = _lambda_zip()
    try:
        lambda_client.get_function(FunctionName=function_name)
        lambda_client.update_function_code(FunctionName=function_name, ZipFile=package, Publish=True)
        lambda_client.get_waiter("function_updated_v2").wait(FunctionName=function_name)
        lambda_client.update_function_configuration(
            FunctionName=function_name, Role=lambda_role, Runtime="python3.11",
            Handler="orchestrator.handler", Timeout=60, Environment=environment,
        )
    except lambda_client.exceptions.ResourceNotFoundException:
        _create_lambda_with_retry(lambda_client, {
            "FunctionName": function_name, "Role": lambda_role, "Runtime": "python3.11",
            "Handler": "orchestrator.handler", "Timeout": 60, "Code": {"ZipFile": package},
            "Environment": environment, "Publish": True,
        })
    function_arn = f"arn:aws:lambda:{cfg.region}:{account}:function:{function_name}"

    script_key = f"{cfg.raw_prefix}/_pipeline/glue_job.py"
    s3.upload_file(str(ROOT / "glue_job.py"), cfg.bucket, script_key)
    glue_role = _role(iam, f"{cfg.name}-glue-role", "glue.amazonaws.com", {
        "Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["logs:*", "glue:Get*", "glue:CreatePartition", "glue:BatchCreatePartition", "glue:UpdatePartition"], "Resource": "*"},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket", "s3:PutObject", "s3:DeleteObject"], "Resource": [f"arn:aws:s3:::{cfg.bucket}", f"arn:aws:s3:::{cfg.bucket}/*"]},
        ]
    })
    job_name = f"{cfg.name}-daily"
    job_args = {
        "--SOURCE_PATH": f"s3://{cfg.bucket}/{cfg.raw_prefix}/",
        "--TARGET_PATH": f"s3://{cfg.bucket}/{cfg.curated_prefix}/",
        "--job-language": "python", "--enable-glue-datacatalog": "true",
    }
    job = {
        "Role": glue_role,
        "Command": {"Name": "glueetl", "ScriptLocation": f"s3://{cfg.bucket}/{script_key}", "PythonVersion": "3"},
        "DefaultArguments": job_args, "GlueVersion": "4.0", "WorkerType": "G.1X",
        "NumberOfWorkers": 2, "MaxRetries": 1, "Timeout": 60,
    }
    try:
        glue.get_job(JobName=job_name)
        glue.update_job(JobName=job_name, JobUpdate=job)
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=job_name, **job)

    # Register the stable curated location so Athena can query it immediately.
    try:
        glue.get_database(Name=cfg.database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": cfg.database})
    columns = [{"Name": name, "Type": data_type} for name, data_type in [
        ("order_id", "string"), ("created_at", "timestamp"),
        ("updated_at", "timestamp"), ("closed_at", "timestamp"),
        ("customer_id", "string"), ("status", "string"),
        ("amount", "double"), ("currency", "string"),
    ]]
    table_input = {
        "Name": cfg.catalog_table, "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "projection.enabled": "true",
            "projection.year.type": "integer",
            "projection.year.range": "2020,2035",
            "projection.month.type": "integer",
            "projection.month.range": "1,12",
            "projection.month.digits": "2",
            "projection.day.type": "integer",
            "projection.day.range": "1,31",
            "projection.day.digits": "2",
            "storage.location.template": (
                f"s3://{cfg.bucket}/{cfg.curated_prefix}/"
                "year=${year}/month=${month}/day=${day}/"
            ),
        },
        "PartitionKeys": [
            {"Name": "year", "Type": "string"},
            {"Name": "month", "Type": "string"},
            {"Name": "day", "Type": "string"},
        ],
        "StorageDescriptor": {
            "Columns": columns,
            "Location": f"s3://{cfg.bucket}/{cfg.curated_prefix}/",
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
        },
    }
    try:
        glue.get_table(DatabaseName=cfg.database, Name=cfg.catalog_table)
        glue.update_table(DatabaseName=cfg.database, TableInput=table_input)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=cfg.database, TableInput=table_input)

    sfn_role = _role(iam, f"{cfg.name}-sfn-role", "states.amazonaws.com", {
        "Version": "2012-10-17", "Statement": [{
            "Effect": "Allow", "Action": ["lambda:InvokeFunction"], "Resource": function_arn
        }, {
            "Effect": "Allow", "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"], "Resource": "*"
        }]
    })
    definition = _state_machine(function_arn, job_name)
    state_machine_name = f"{cfg.name}-daily"
    state_machine_arn = f"arn:aws:states:{cfg.region}:{account}:stateMachine:{state_machine_name}"
    try:
        sfn.describe_state_machine(stateMachineArn=state_machine_arn)
        sfn.update_state_machine(stateMachineArn=state_machine_arn, definition=json.dumps(definition), roleArn=sfn_role)
    except sfn.exceptions.StateMachineDoesNotExist:
        sfn.create_state_machine(name=state_machine_name, definition=json.dumps(definition), roleArn=sfn_role, type="STANDARD")

    events_role = _role(iam, f"{cfg.name}-events-role", "events.amazonaws.com", {
        "Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "states:StartExecution", "Resource": state_machine_arn}]
    })
    events.put_rule(Name=f"{cfg.name}-schedule", ScheduleExpression=cfg.schedule, State="ENABLED")
    events.put_targets(Rule=f"{cfg.name}-schedule", Targets=[{"Id": "pipeline", "Arn": state_machine_arn, "RoleArn": events_role}])
    print(f"Pipeline ready: {state_machine_arn}")
    print(f"Raw     : s3://{cfg.bucket}/{cfg.raw_prefix}/")
    print(f"Curated : s3://{cfg.bucket}/{cfg.curated_prefix}/")


def run_once() -> str:
    """Start one workflow execution manually and return its ARN."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    account = session.client("sts").get_caller_identity()["Account"]
    state_machine_arn = (
        f"arn:aws:states:{cfg.region}:{account}:stateMachine:{cfg.name}-daily"
    )
    response = session.client("stepfunctions").start_execution(
        stateMachineArn=state_machine_arn,
    )
    execution_arn = response["executionArn"]
    print(f"Execution started: {execution_arn}")
    return execution_arn


def execution_status(execution_arn: str) -> dict[str, Any]:
    """Print and return the current status of a manual execution."""
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
    """Delete one pipeline role and its known inline policy if it exists."""
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
    """Stop active Glue runs before deleting the job and its IAM role."""
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


def destroy() -> None:
    """Delete pipeline resources while preserving source tables and all S3 data."""
    cfg = config()
    session = boto3.Session(region_name=cfg.region)
    account = session.client("sts").get_caller_identity()["Account"]
    events, sfn = session.client("events"), session.client("stepfunctions")
    glue, lamb = session.client("glue"), session.client("lambda")
    ddb, iam = session.client("dynamodb"), session.client("iam")
    rule = f"{cfg.name}-schedule"
    try:
        events.remove_targets(Rule=rule, Ids=["pipeline"], Force=True)
        events.delete_rule(Name=rule, Force=True)
        print(f"Deleted EventBridge rule: {rule}")
    except events.exceptions.ResourceNotFoundException:
        pass

    state_arn = f"arn:aws:states:{cfg.region}:{account}:stateMachine:{cfg.name}-daily"
    try:
        for execution in sfn.list_executions(
            stateMachineArn=state_arn, statusFilter="RUNNING"
        )["executions"]:
            sfn.stop_execution(
                executionArn=execution["executionArn"],
                cause="Pipeline destroy requested",
            )
        sfn.delete_state_machine(stateMachineArn=state_arn)
        print(f"Deleted state machine: {cfg.name}-daily")
    except sfn.exceptions.StateMachineDoesNotExist:
        pass

    _stop_job_runs(glue, f"{cfg.name}-daily")
    try:
        glue.delete_job(JobName=f"{cfg.name}-daily")
        print(f"Deleted Glue job: {cfg.name}-daily")
    except glue.exceptions.EntityNotFoundException:
        pass
    try:
        glue.delete_table(DatabaseName=cfg.database, Name=cfg.catalog_table)
        print(f"Deleted Glue table: {cfg.database}.{cfg.catalog_table}")
    except glue.exceptions.EntityNotFoundException:
        pass
    try:
        lamb.delete_function(FunctionName=f"{cfg.name}-orchestrator")
        print(f"Deleted Lambda: {cfg.name}-orchestrator")
    except lamb.exceptions.ResourceNotFoundException:
        pass
    try:
        ddb.delete_table(TableName=f"{cfg.name}-control")
        print(f"Deleting control table: {cfg.name}-control")
    except ddb.exceptions.ResourceNotFoundException:
        pass
    for suffix in ("lambda-role", "glue-role", "sfn-role", "events-role"):
        _delete_role(iam, f"{cfg.name}-{suffix}")
    print("Preserved DynamoDB source table, S3 bucket and all S3 objects.")


def _state_machine(function_arn: str, job_name: str) -> dict[str, Any]:
    """Build export polling -> Glue sync -> watermark commit workflow."""
    invoke = "arn:aws:states:::lambda:invoke"
    return {
        "StartAt": "StartExport",
        "States": {
            "StartExport": {"Type": "Task", "Resource": invoke, "Parameters": {"FunctionName": function_arn, "Payload": {"action": "start"}}, "OutputPath": "$.Payload", "Next": "Skip?"},
            "Skip?": {"Type": "Choice", "Choices": [{"Variable": "$.skip", "BooleanEquals": True, "Next": "Done"}], "Default": "WaitExport"},
            "WaitExport": {"Type": "Wait", "Seconds": 60, "Next": "ExportStatus"},
            "ExportStatus": {"Type": "Task", "Resource": invoke, "Parameters": {"FunctionName": function_arn, "Payload": {"action": "status", "export_arn.$": "$.export_arn", "export_to.$": "$.export_to"}}, "OutputPath": "$.Payload", "Next": "ExportComplete?"},
            "ExportComplete?": {"Type": "Choice", "Choices": [{"Variable": "$.status", "StringEquals": "COMPLETED", "Next": "RunGlue"}, {"Variable": "$.status", "StringEquals": "FAILED", "Next": "ExportFailed"}], "Default": "WaitExport"},
            "ExportFailed": {"Type": "Fail", "Cause": "DynamoDB export failed"},
            "RunGlue": {"Type": "Task", "Resource": "arn:aws:states:::glue:startJobRun.sync", "Parameters": {"JobName": job_name, "Arguments": {"--SOURCE_PATH.$": "$.source_path"}}, "ResultPath": "$.glue", "Next": "Commit"},
            "Commit": {"Type": "Task", "Resource": invoke, "Parameters": {"FunctionName": function_arn, "Payload": {"action": "commit", "export_arn.$": "$.export_arn", "export_to.$": "$.export_to"}}, "OutputPath": "$.Payload", "End": True},
            "Done": {"Type": "Succeed"},
        },
    }


if __name__ == "__main__":
    setup()
