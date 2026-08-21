"""AWS DMS one-time full load: PostgreSQL RDS -> S3 Parquet.

Chia rõ hai nhóm hàm:

- Tạo hạ tầng: ``provision()`` và ``destroy()``.
- Chạy pipeline: ``start()`` và ``status()``.

``provision()`` không start full load, và ``start()`` không tạo hạ tầng. Nhờ vậy
notebook tạo resource và notebook chạy pipeline không chồng nhiệm vụ.
"""

from __future__ import annotations

import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
DMS_VPC_ROLE_NAME = "dms-vpc-role"
DMS_VPC_POLICY_ARN = "arn:aws:iam::aws:policy/service-role/AmazonDMSVPCManagementRole"


@dataclass(frozen=True)
class Config:
    """Toàn bộ cấu hình cần cho pipeline archive."""

    aws_region: str
    rds_id: str
    rds_host: str
    rds_port: int
    rds_database: str
    rds_username: str
    rds_password: str
    subnet_ids: list[str]
    security_group_ids: list[str]
    source_schema: str
    source_table: str
    date_column: str
    retention_days: int
    s3_bucket: str
    s3_prefix: str
    dms_prefix: str
    dms_instance_class: str
    dms_storage_gb: int

    @property
    def instance_id(self) -> str:
        return f"{self.dms_prefix}-instance"

    @property
    def subnet_group_id(self) -> str:
        return f"{self.dms_prefix}-subnet"

    @property
    def source_endpoint_id(self) -> str:
        return f"{self.dms_prefix}-source"

    @property
    def target_endpoint_id(self) -> str:
        return f"{self.dms_prefix}-target"

    @property
    def task_id(self) -> str:
        return f"{self.dms_prefix}-task"

    @property
    def s3_role_name(self) -> str:
        return f"{self.dms_prefix}-s3-role"


@dataclass(frozen=True)
class Aws:
    """Nhóm AWS clients để truyền giữa các helper."""

    s3: Any
    dms: Any
    iam: Any
    ec2: Any


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value.startswith("<"):
        raise ValueError(f"Thiếu giá trị thật cho {name} trong dms/.env")
    return value


def _positive_int(name: str, default: str) -> int:
    raw_value = os.getenv(name, default).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} phải là số nguyên, nhận được {raw_value!r}") from error
    if value <= 0:
        raise ValueError(f"{name} phải lớn hơn 0")
    return value


def _identifier(name: str, value: str) -> str:
    """Chặn nhầm tên schema/table/column trước khi gửi table mapping tới DMS."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"{name} không phải PostgreSQL identifier hợp lệ: {value!r}")
    return value


def config() -> Config:
    """Đọc dms/.env và trả về cấu hình đã validate."""
    load_dotenv(ROOT / ".env", override=True)
    cfg = Config(
        aws_region=_required("AWS_REGION"),
        rds_id=_required("RDS_ID"),
        rds_host=_required("RDS_HOST"),
        rds_port=_positive_int("RDS_PORT", "5432"),
        rds_database=_required("RDS_DATABASE"),
        rds_username=_required("RDS_USERNAME"),
        rds_password=_required("RDS_PASSWORD"),
        subnet_ids=[item.strip() for item in _required("SUBNET_IDS").split(",") if item.strip()],
        security_group_ids=[
            item.strip() for item in _required("SECURITY_GROUP_IDS").split(",") if item.strip()
        ],
        source_schema=_identifier("SOURCE_SCHEMA", os.getenv("SOURCE_SCHEMA", "public").strip()),
        source_table=_identifier("SOURCE_TABLE", os.getenv("SOURCE_TABLE", "orders").strip()),
        date_column=_identifier("DATE_COLUMN", os.getenv("DATE_COLUMN", "closed_at_utc").strip()),
        retention_days=_positive_int("ARCHIVE_RETENTION_DAYS", "90"),
        s3_bucket=_required("S3_BUCKET"),
        s3_prefix=_required("S3_PREFIX").strip("/"),
        dms_prefix=_required("DMS_PREFIX"),
        dms_instance_class=os.getenv("DMS_INSTANCE_CLASS", "dms.t3.medium").strip(),
        dms_storage_gb=_positive_int("DMS_STORAGE_GB", "50"),
    )
    if len(cfg.subnet_ids) < 2:
        raise ValueError("SUBNET_IDS cần ít nhất 2 subnet cho DMS subnet group")
    if not cfg.security_group_ids:
        raise ValueError("SECURITY_GROUP_IDS không được rỗng")
    return cfg


def _context() -> tuple[Config, Aws]:
    """Đọc cấu hình mới nhất và tạo AWS clients."""
    cfg = config()
    session = boto3.Session(region_name=cfg.aws_region)
    return cfg, Aws(
        s3=session.client("s3"),
        dms=session.client("dms"),
        iam=session.client("iam"),
        ec2=session.client("ec2"),
    )


def _find_instance(cfg: Config, aws: Aws) -> dict[str, Any] | None:
    try:
        items = aws.dms.describe_replication_instances(
            Filters=[{"Name": "replication-instance-id", "Values": [cfg.instance_id]}]
        )["ReplicationInstances"]
    except aws.dms.exceptions.ResourceNotFoundFault:
        return None  # Resource vừa bị xóa trong lúc destroy() poll.
    return items[0] if items else None


def _find_endpoint(endpoint_id: str, aws: Aws) -> dict[str, Any] | None:
    try:
        items = aws.dms.describe_endpoints(
            Filters=[{"Name": "endpoint-id", "Values": [endpoint_id]}]
        )["Endpoints"]
    except aws.dms.exceptions.ResourceNotFoundFault:
        return None
    return items[0] if items else None


def _find_task(cfg: Config, aws: Aws) -> dict[str, Any] | None:
    try:
        items = aws.dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-id", "Values": [cfg.task_id]}]
        )["ReplicationTasks"]
    except aws.dms.exceptions.ResourceNotFoundFault:
        return None
    return items[0] if items else None


def _wait_for(
    description: str,
    fetch: Any,
    is_done: Any,
    timeout_seconds: int = 900,
    interval_seconds: int = 10,
) -> Any:
    """Poll AWS với timeout để notebook không chờ vô hạn."""
    deadline = time.monotonic() + timeout_seconds
    while True:
        value = fetch()
        if is_done(value):
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Hết thời gian chờ: {description}")
        print(f"  Đang chờ {description}...")
        time.sleep(interval_seconds)


def _ensure_bucket(cfg: Config, aws: Aws) -> None:
    try:
        aws.s3.head_bucket(Bucket=cfg.s3_bucket)
        print(f"✓ S3 bucket đã tồn tại: {cfg.s3_bucket}")
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        arguments: dict[str, Any] = {"Bucket": cfg.s3_bucket}
        if cfg.aws_region != "us-east-1":
            arguments["CreateBucketConfiguration"] = {"LocationConstraint": cfg.aws_region}
        aws.s3.create_bucket(**arguments)
        print(f"✓ Đã tạo S3 bucket: {cfg.s3_bucket}")

    aws.s3.put_public_access_block(
        Bucket=cfg.s3_bucket,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        },
    )


def _ensure_iam_roles(cfg: Config, aws: Aws) -> str:
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "dms.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }

    created_role = False
    try:
        aws.iam.get_role(RoleName=DMS_VPC_ROLE_NAME)
    except aws.iam.exceptions.NoSuchEntityException:
        aws.iam.create_role(
            RoleName=DMS_VPC_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
        )
        created_role = True
    aws.iam.attach_role_policy(RoleName=DMS_VPC_ROLE_NAME, PolicyArn=DMS_VPC_POLICY_ARN)
    print(f"✓ IAM role cho VPC: {DMS_VPC_ROLE_NAME}")

    try:
        role = aws.iam.get_role(RoleName=cfg.s3_role_name)["Role"]
    except aws.iam.exceptions.NoSuchEntityException:
        role = aws.iam.create_role(
            RoleName=cfg.s3_role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
        )["Role"]
        created_role = True

    s3_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": f"arn:aws:s3:::{cfg.s3_bucket}",
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:PutObjectTagging",
                    "s3:PutObjectAcl",
                    "s3:GetObject",
                    "s3:DeleteObject",
                ],
                "Resource": f"arn:aws:s3:::{cfg.s3_bucket}/{cfg.s3_prefix}/*",
            },
        ],
    }
    aws.iam.put_role_policy(
        RoleName=cfg.s3_role_name,
        PolicyName="DmsS3Access",
        PolicyDocument=json.dumps(s3_policy),
    )
    print(f"✓ IAM role cho S3: {cfg.s3_role_name}")
    if created_role:
        print("  Chờ AWS đồng bộ IAM role...")
        time.sleep(10)
    return role["Arn"]


def _ensure_subnet_group(cfg: Config, aws: Aws) -> None:
    try:
        aws.dms.create_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=cfg.subnet_group_id,
            ReplicationSubnetGroupDescription="DMS PostgreSQL RDS to S3",
            SubnetIds=cfg.subnet_ids,
        )
        print(f"✓ Đã tạo subnet group: {cfg.subnet_group_id}")
    except aws.dms.exceptions.ResourceAlreadyExistsFault:
        print(f"✓ Subnet group đã tồn tại: {cfg.subnet_group_id}")


def _route_table_id(vpc_id: str, subnet_id: str, aws: Aws) -> str:
    tables = aws.ec2.describe_route_tables(
        Filters=[{"Name": "association.subnet-id", "Values": [subnet_id]}]
    )["RouteTables"]
    if tables:
        return tables[0]["RouteTableId"]
    main_tables = aws.ec2.describe_route_tables(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "association.main", "Values": ["true"]},
    ])["RouteTables"]
    if not main_tables:
        raise RuntimeError(f"Không tìm thấy route table cho subnet {subnet_id}")
    return main_tables[0]["RouteTableId"]


def _ensure_s3_gateway_endpoint(cfg: Config, aws: Aws) -> None:
    group = aws.dms.describe_replication_subnet_groups(
        Filters=[{"Name": "replication-subnet-group-id", "Values": [cfg.subnet_group_id]}]
    )["ReplicationSubnetGroups"][0]
    vpc_id = group["VpcId"]
    service_name = f"com.amazonaws.{cfg.aws_region}.s3"
    route_table_ids = sorted(
        {_route_table_id(vpc_id, subnet_id, aws) for subnet_id in cfg.subnet_ids}
    )
    endpoints = aws.ec2.describe_vpc_endpoints(Filters=[
        {"Name": "vpc-id", "Values": [vpc_id]},
        {"Name": "service-name", "Values": [service_name]},
        {"Name": "vpc-endpoint-type", "Values": ["Gateway"]},
    ])["VpcEndpoints"]

    if endpoints:
        endpoint = endpoints[0]
        missing = sorted(set(route_table_ids) - set(endpoint.get("RouteTableIds", [])))
        if missing:
            aws.ec2.modify_vpc_endpoint(
                VpcEndpointId=endpoint["VpcEndpointId"],
                AddRouteTableIds=missing,
            )
        endpoint_id = endpoint["VpcEndpointId"]
    else:
        endpoint_id = aws.ec2.create_vpc_endpoint(
            VpcId=vpc_id,
            ServiceName=service_name,
            VpcEndpointType="Gateway",
            RouteTableIds=route_table_ids,
        )["VpcEndpoint"]["VpcEndpointId"]

    # Botocore cũ không có waiter cho VPC Endpoint, nên poll trực tiếp.
    deadline = time.monotonic() + 300
    while True:
        endpoint = aws.ec2.describe_vpc_endpoints(
            VpcEndpointIds=[endpoint_id]
        )["VpcEndpoints"][0]
        state = endpoint["State"]
        if state == "available":
            break
        if state in {"failed", "rejected"}:
            raise RuntimeError(f"S3 Gateway Endpoint {endpoint_id} thất bại: {state}")
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"S3 Gateway Endpoint chưa available sau 300 giây; state={state}"
            )
        print(f"  Đang chờ S3 Gateway Endpoint: {state}")
        time.sleep(5)

    print(f"✓ Private route tới S3: {endpoint_id}")


def _ensure_instance(cfg: Config, aws: Aws) -> str:
    instance = _find_instance(cfg, aws)
    if instance is None:
        aws.dms.create_replication_instance(
            ReplicationInstanceIdentifier=cfg.instance_id,
            ReplicationInstanceClass=cfg.dms_instance_class,
            AllocatedStorage=cfg.dms_storage_gb,
            VpcSecurityGroupIds=cfg.security_group_ids,
            ReplicationSubnetGroupIdentifier=cfg.subnet_group_id,
            PubliclyAccessible=False,
        )
        print(f"✓ Đã yêu cầu tạo DMS instance: {cfg.instance_id}")
    else:
        print(f"✓ DMS instance đã tồn tại: {cfg.instance_id}")

    instance = _wait_for(
        "DMS instance available",
        lambda: _find_instance(cfg, aws),
        lambda item: item is not None and item["ReplicationInstanceStatus"] == "available",
    )
    return instance["ReplicationInstanceArn"]


def _ensure_endpoints(cfg: Config, aws: Aws, s3_role_arn: str) -> tuple[str, str]:
    source = _find_endpoint(cfg.source_endpoint_id, aws)
    if source is None:
        source = aws.dms.create_endpoint(
            EndpointIdentifier=cfg.source_endpoint_id,
            EndpointType="source",
            EngineName="postgres",
            ServerName=cfg.rds_host,
            Port=cfg.rds_port,
            DatabaseName=cfg.rds_database,
            Username=cfg.rds_username,
            Password=cfg.rds_password,
            SslMode="require",
        )["Endpoint"]
        print(f"✓ Đã tạo source endpoint: {cfg.source_endpoint_id}")
    else:
        print(f"✓ Source endpoint đã tồn tại: {cfg.source_endpoint_id}")

    target = _find_endpoint(cfg.target_endpoint_id, aws)
    if target is None:
        target = aws.dms.create_endpoint(
            EndpointIdentifier=cfg.target_endpoint_id,
            EndpointType="target",
            EngineName="s3",
            S3Settings={
                "BucketName": cfg.s3_bucket,
                "BucketFolder": cfg.s3_prefix,
                "ServiceAccessRoleArn": s3_role_arn,
                "DataFormat": "parquet",
                "CompressionType": "GZIP",
            },
        )["Endpoint"]
        print(f"✓ Đã tạo target endpoint: {cfg.target_endpoint_id}")
    else:
        print(f"✓ Target endpoint đã tồn tại: {cfg.target_endpoint_id}")
    return source["EndpointArn"], target["EndpointArn"]


def _test_endpoints(
    instance_arn: str,
    source_arn: str,
    target_arn: str,
    aws: Aws,
    timeout_seconds: int = 600,
) -> None:
    endpoint_arns = {"RDS": source_arn, "S3": target_arn}
    for name, endpoint_arn in endpoint_arns.items():
        try:
            aws.dms.test_connection(
                ReplicationInstanceArn=instance_arn,
                EndpointArn=endpoint_arn,
            )
            print(f"  Bắt đầu test kết nối {name}")
        except aws.dms.exceptions.InvalidResourceStateFault:
            print(f"  Test kết nối {name} đang chạy")

    time.sleep(5)  # DMS cần vài giây để cập nhật kết quả test mới.
    deadline = time.monotonic() + timeout_seconds
    while True:
        statuses: dict[str, str] = {}
        failures: dict[str, str] = {}
        for name, endpoint_arn in endpoint_arns.items():
            connections = aws.dms.describe_connections(
                Filters=[{"Name": "endpoint-arn", "Values": [endpoint_arn]}]
            )["Connections"]
            status_value = connections[0]["Status"] if connections else "not-tested"
            statuses[name] = status_value
            if status_value == "failed":
                failures[name] = connections[0].get("LastFailureMessage", "Không có chi tiết")

        if failures:
            details = "; ".join(f"{name}: {message}" for name, message in failures.items())
            raise RuntimeError(f"Test endpoint thất bại — {details}")
        if all(item == "successful" for item in statuses.values()):
            print("✓ Kết nối RDS và S3 đều thành công")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Test endpoint quá {timeout_seconds} giây: {statuses}")
        print(f"  Đang test endpoint: {statuses}")
        time.sleep(10)


def _table_mapping(cfg: Config) -> tuple[dict[str, Any], str]:
    # DMS hỗ trợ timestamp dạng YYYY-MM-DD HH:MM:SS.SSS. Giữ đầy đủ thời gian
    # để retention chính xác N * 24 giờ, thay vì vô tình làm tròn về 00:00 UTC.
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=cfg.retention_days)
    ).strftime("%Y-%m-%d %H:%M:%S.000")
    mapping = {
        "rules": [{
            "rule-type": "selection",
            "rule-id": "1",
            "rule-name": "archive-old-rows",
            "object-locator": {
                "schema-name": cfg.source_schema,
                "table-name": cfg.source_table,
            },
            "rule-action": "include",
            # DMS OR các conditions trong cùng một filter, nhưng AND các filter
            # riêng. Vì vậy notnull và lte phải tách ra.
            "filters": [
                {
                    "filter-type": "source",
                    "column-name": cfg.date_column,
                    "filter-conditions": [{"filter-operator": "notnull"}],
                },
                {
                    "filter-type": "source",
                    "column-name": cfg.date_column,
                    "filter-conditions": [{"filter-operator": "lte", "value": cutoff}],
                },
            ],
        }],
    }
    return mapping, cutoff


def _effective_cutoff(task: dict[str, Any], cfg: Config) -> str:
    """Validate mapping thực tế trên AWS và trả về cutoff mà task đang dùng."""
    try:
        mapping = json.loads(task["TableMappings"])
        rules = mapping["rules"]
        selection_rules = [rule for rule in rules if rule.get("rule-type") == "selection"]
        if len(selection_rules) != 1:
            raise ValueError("cần đúng một selection rule")

        rule = selection_rules[0]
        locator = rule.get("object-locator", {})
        if rule.get("rule-action") != "include":
            raise ValueError("selection rule không phải include")
        if (
            locator.get("schema-name") != cfg.source_schema
            or locator.get("table-name") != cfg.source_table
        ):
            raise ValueError("selection rule trỏ sai source table")

        conditions_by_column: dict[str, list[dict[str, Any]]] = {}
        for source_filter in rule.get("filters", []):
            conditions_by_column.setdefault(
                source_filter.get("column-name", ""), []
            ).extend(source_filter.get("filter-conditions", []))
        date_conditions = conditions_by_column.get(cfg.date_column, [])
        has_notnull = any(
            item.get("filter-operator") == "notnull" for item in date_conditions
        )
        lte_values = [
            item.get("value")
            for item in date_conditions
            if item.get("filter-operator") == "lte"
        ]
        if not has_notnull or len(lte_values) != 1 or not lte_values[0]:
            raise ValueError(f"thiếu filter {cfg.date_column} IS NOT NULL AND <= cutoff")
        return str(lte_values[0])
    except (KeyError, TypeError, json.JSONDecodeError, ValueError) as error:
        raise RuntimeError(
            f"Task {cfg.task_id} đang dùng table mapping không an toàn: {error}. "
            "Hãy chạy destroy(), dùng raw S3 prefix trống, rồi provision() lại."
        ) from error


def _ensure_task(
    cfg: Config,
    aws: Aws,
    instance_arn: str,
    source_arn: str,
    target_arn: str,
) -> None:
    """Tạo replication task nếu chưa có. Không start full load ở bước này."""
    mapping, cutoff = _table_mapping(cfg)
    task = _find_task(cfg, aws)
    if task is None:
        aws.dms.create_replication_task(
            ReplicationTaskIdentifier=cfg.task_id,
            SourceEndpointArn=source_arn,
            TargetEndpointArn=target_arn,
            ReplicationInstanceArn=instance_arn,
            MigrationType="full-load",
            TableMappings=json.dumps(mapping),
            ReplicationTaskSettings=json.dumps({
                "FullLoadSettings": {
                    "TargetTablePrepMode": "DO_NOTHING",
                    "MaxFullLoadSubTasks": 8,
                    "CommitRate": 10_000,
                },
                "Logging": {"EnableLogging": True},
            }),
        )
        print(f"✓ Đã tạo replication task: {cfg.task_id}")
        print(f"  Mapping khởi tạo: {cfg.date_column} <= {cutoff}")
    else:
        print(f"✓ Replication task đã tồn tại: {cfg.task_id} ({task['Status']})")
        if task["Status"] != "ready":
            print(f"  Mapping thực tế: {cfg.date_column} <= {_effective_cutoff(task, cfg)}")

    task = _wait_for(
        "task sẵn sàng",
        lambda: _find_task(cfg, aws),
        lambda item: item is not None and item["Status"] != "creating",
        timeout_seconds=300,
    )
    print(f"✓ Task ở trạng thái: {task['Status']}")


def provision() -> None:
    """Tạo/tái sử dụng toàn bộ hạ tầng DMS và test kết nối. Không chạy full load."""
    cfg, aws = _context()
    print("=" * 72)
    print("PROVISION — hạ tầng DMS cho PostgreSQL RDS → S3")
    print("=" * 72)
    print(f"Nguồn : {cfg.source_schema}.{cfg.source_table}")
    print(f"Bộ lọc: {cfg.date_column} cũ hơn {cfg.retention_days} ngày")
    print(f"Đích  : s3://{cfg.s3_bucket}/{cfg.s3_prefix}/")

    _ensure_bucket(cfg, aws)
    s3_role_arn = _ensure_iam_roles(cfg, aws)
    _ensure_subnet_group(cfg, aws)
    _ensure_s3_gateway_endpoint(cfg, aws)
    instance_arn = _ensure_instance(cfg, aws)
    source_arn, target_arn = _ensure_endpoints(cfg, aws, s3_role_arn)
    _test_endpoints(instance_arn, source_arn, target_arn, aws)
    _ensure_task(cfg, aws, instance_arn, source_arn, target_arn)

    print("-" * 72)
    print("Hạ tầng đã sẵn sàng. Chạy start() trong notebook pipeline để bắt đầu full load.")


def start() -> str:
    """Refresh cutoff rồi start full load. Trả về cutoff mà task đang dùng.

    Task đã chạy hoặc đã hoàn tất sẽ không bị start lại, vì
    ``TargetTablePrepMode=DO_NOTHING`` có thể tạo file trùng trên S3.
    """
    cfg, aws = _context()
    print("=" * 72)
    print("START — full load")
    print("=" * 72)

    task = _find_task(cfg, aws)
    if task is None:
        raise RuntimeError(
            f"Không tìm thấy task {cfg.task_id}. Chạy provision() trong notebook "
            "tạo resource trước."
        )

    deadline = time.monotonic() + 300
    while True:
        task = _find_task(cfg, aws)
        if task is None:
            raise RuntimeError("Replication task biến mất trong lúc start()")
        task_status = task["Status"]
        if task_status == "ready":
            # Cutoff được tính lại ngay trước khi start để retention đúng với
            # thời điểm chạy thật, không phải thời điểm provision().
            mapping, cutoff = _table_mapping(cfg)
            aws.dms.modify_replication_task(
                ReplicationTaskArn=task["ReplicationTaskArn"],
                TableMappings=json.dumps(mapping),
            )
            _wait_for(
                "task sẵn sàng sau khi cập nhật mapping",
                lambda: _find_task(cfg, aws),
                lambda item: item is not None and item["Status"] == "ready",
                timeout_seconds=300,
            )
            aws.dms.start_replication_task(
                ReplicationTaskArn=task["ReplicationTaskArn"],
                StartReplicationTaskType="start-replication",
            )
            print(f"✓ Đã bắt đầu full load: {cfg.date_column} <= {cutoff}")
            print("Chạy status() để theo dõi tiến độ.")
            return cutoff
        if task_status in {"starting", "running"}:
            cutoff = _effective_cutoff(task, cfg)
            print(f"✓ Full load đang {task_status}: {cfg.date_column} <= {cutoff}")
            return cutoff
        if task_status == "stopped":
            errors = task.get("ReplicationTaskStats", {}).get("TablesErrored", 0)
            if errors == 0:
                cutoff = _effective_cutoff(task, cfg)
                print("✓ Full load trước đó đã hoàn tất; không chạy lại để tránh file trùng trên S3")
                print(f"  Cutoff đang dùng: {cfg.date_column} <= {cutoff}")
                return cutoff
            raise RuntimeError(
                "Task đã dừng và có table lỗi. Xem status(); sau đó destroy() và "
                "provision() để chạy mới."
            )
        if task_status == "failed":
            raise RuntimeError(
                f"Task thất bại: {task.get('LastFailureMessage', 'Không có chi tiết')}"
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Task không chuyển sang ready; status={task_status}")
        print(f"  Đang chờ task sẵn sàng: {task_status}")
        time.sleep(10)


def cutoff() -> str:
    """Đọc cutoff thật mà task đang dùng, không thay đổi trạng thái task."""
    cfg, aws = _context()
    task = _find_task(cfg, aws)
    if task is None:
        raise RuntimeError(f"Không tìm thấy task {cfg.task_id}")
    return _effective_cutoff(task, cfg)


def status() -> None:
    """In trạng thái ngắn gọn và đủ thông tin để tìm lỗi phổ biến."""
    cfg, aws = _context()
    print("=" * 72)
    print("STATUS — DMS full load")
    print("=" * 72)

    instance = _find_instance(cfg, aws)
    instance_status = instance["ReplicationInstanceStatus"] if instance else "not-found"
    print(f"DMS instance : {instance_status}")

    for label, endpoint_id in [
        ("RDS endpoint", cfg.source_endpoint_id),
        ("S3 endpoint ", cfg.target_endpoint_id),
    ]:
        endpoint = _find_endpoint(endpoint_id, aws)
        if endpoint is None:
            print(f"{label}: not-found")
            continue
        connections = aws.dms.describe_connections(
            Filters=[{"Name": "endpoint-arn", "Values": [endpoint["EndpointArn"]]}]
        )["Connections"]
        connection = connections[0] if connections else {}
        connection_status = connection.get("Status", "not-tested")
        print(f"{label}: {connection_status}")
        if connection_status == "failed":
            print(f"  Lỗi: {connection.get('LastFailureMessage', 'Không có chi tiết')}")

    task = _find_task(cfg, aws)
    if task is None:
        print("Task         : not-found — hãy chạy provision()")
    else:
        stats = task.get("ReplicationTaskStats", {})
        print(f"Task         : {task['Status']}")
        print(
            "Tiến độ      : "
            f"{stats.get('FullLoadProgressPercent', 0)}% | "
            f"completed={stats.get('TablesCompleted', 0)} | "
            f"loading={stats.get('TablesLoading', 0)} | "
            f"queued={stats.get('TablesQueued', 0)} | "
            f"errors={stats.get('TablesErrored', 0)}"
        )
        failure = task.get("LastFailureMessage")
        if failure:
            print(f"Task error   : {failure}")

        try:
            tables = aws.dms.describe_table_statistics(
                ReplicationTaskArn=task["ReplicationTaskArn"]
            )["TableStatistics"]
            for table in tables:
                print(
                    f"Table        : {table['SchemaName']}.{table['TableName']} | "
                    f"state={table['TableState']} | rows={table.get('FullLoadRows', 0)}"
                )
                if table.get("LastFailureMessage"):
                    print(f"  Lỗi table: {table['LastFailureMessage']}")
        except aws.dms.exceptions.InvalidResourceStateFault:
            pass  # Task mới tạo có thể chưa có table statistics.

    print(f"S3           : s3://{cfg.s3_bucket}/{cfg.s3_prefix}/")
    try:
        response = aws.s3.list_objects_v2(
            Bucket=cfg.s3_bucket,
            Prefix=f"{cfg.s3_prefix}/",
            MaxKeys=10,
        )
        objects = response.get("Contents", [])
        label = "chưa có file" if not objects else f"hiển thị {len(objects)} file đầu tiên"
        print(f"S3 objects   : {label}")
        for item in objects:
            print(f"  - {item['Key']} ({item['Size']:,} bytes)")
    except ClientError as error:
        print(f"S3 error     : {error}")


def destroy() -> None:
    """Xóa tài nguyên DMS của pipeline; giữ nguyên RDS, bucket và dữ liệu S3."""
    cfg, aws = _context()
    print("=" * 72)
    print("DESTROY — chỉ xóa tài nguyên DMS")
    print("=" * 72)

    def find_or_none(fetch: Any) -> Any | None:
        """Coi ResourceNotFoundFault là resource đã được xóa thành công."""
        try:
            return fetch()
        except aws.dms.exceptions.ResourceNotFoundFault:
            return None

    find_task = lambda: find_or_none(lambda: _find_task(cfg, aws))
    find_endpoint = lambda endpoint_id: find_or_none(
        lambda: _find_endpoint(endpoint_id, aws)
    )
    find_instance = lambda: find_or_none(lambda: _find_instance(cfg, aws))

    task = find_task()
    if task is not None:
        active_statuses = {"running", "starting", "modifying", "stopping"}
        if task["Status"] in active_statuses:
            if task["Status"] != "stopping":
                try:
                    aws.dms.stop_replication_task(
                        ReplicationTaskArn=task["ReplicationTaskArn"]
                    )
                except aws.dms.exceptions.InvalidResourceStateFault:
                    pass
            task = _wait_for(
                "task dừng",
                find_task,
                lambda item: item is None or item["Status"] not in active_statuses,
            )
        if task is not None and task["Status"] != "deleting":
            aws.dms.delete_replication_task(ReplicationTaskArn=task["ReplicationTaskArn"])
        _wait_for("task được xóa", find_task, lambda item: item is None)
        print(f"✓ Đã xóa task: {cfg.task_id}")
    else:
        print(f"- Task không tồn tại: {cfg.task_id}")

    for endpoint_id in [cfg.source_endpoint_id, cfg.target_endpoint_id]:
        endpoint = find_endpoint(endpoint_id)
        if endpoint is not None:
            if endpoint["Status"] != "deleting":
                aws.dms.delete_endpoint(EndpointArn=endpoint["EndpointArn"])
            _wait_for(
                f"endpoint {endpoint_id} được xóa",
                lambda endpoint_id=endpoint_id: find_endpoint(endpoint_id),
                lambda item: item is None,
                interval_seconds=5,
            )
            print(f"✓ Đã xóa endpoint: {endpoint_id}")
        else:
            print(f"- Endpoint không tồn tại: {endpoint_id}")

    instance = find_instance()
    if instance is not None:
        if instance["ReplicationInstanceStatus"] != "deleting":
            aws.dms.delete_replication_instance(
                ReplicationInstanceArn=instance["ReplicationInstanceArn"]
            )
        _wait_for("DMS instance được xóa", find_instance, lambda item: item is None)
        print(f"✓ Đã xóa instance: {cfg.instance_id}")
    else:
        print(f"- Instance không tồn tại: {cfg.instance_id}")

    try:
        aws.dms.delete_replication_subnet_group(
            ReplicationSubnetGroupIdentifier=cfg.subnet_group_id
        )
        print(f"✓ Đã xóa subnet group: {cfg.subnet_group_id}")
    except aws.dms.exceptions.ResourceNotFoundFault:
        print(f"- Subnet group không tồn tại: {cfg.subnet_group_id}")

    try:
        try:
            aws.iam.delete_role_policy(
                RoleName=cfg.s3_role_name,
                PolicyName="DmsS3Access",
            )
        except aws.iam.exceptions.NoSuchEntityException:
            pass
        aws.iam.delete_role(RoleName=cfg.s3_role_name)
        print(f"✓ Đã xóa IAM role: {cfg.s3_role_name}")
    except aws.iam.exceptions.NoSuchEntityException:
        print(f"- IAM role không tồn tại: {cfg.s3_role_name}")

    print("-" * 72)
    print("Đã giữ nguyên: RDS, S3 bucket, dữ liệu S3, dms-vpc-role và S3 Gateway Endpoint.")


if __name__ == "__main__":
    provision()
