"""Lambda handler used by Step Functions to manage DynamoDB export watermarks."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3

DDB = boto3.client("dynamodb")
EXPORT = boto3.client("dynamodb")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Prepare/start, inspect, or commit one idempotent incremental window."""
    action = event["action"]
    control_table = os.environ["CONTROL_TABLE"]
    pipeline = os.environ["PIPELINE_NAME"]

    if action == "start":
        response = DDB.get_item(
            TableName=control_table,
            Key={"pipeline": {"S": pipeline}},
            ConsistentRead=True,
        )
        item = response.get("Item", {})
        # Keep a small safety delay because PITR is not current to the last second.
        export_to = datetime.now(timezone.utc) - timedelta(minutes=5)
        export_from_raw = item.get("watermark", {}).get("S")

        request: dict[str, Any] = {
            "TableArn": os.environ["TABLE_ARN"],
            "S3Bucket": os.environ["S3_BUCKET"],
            "S3Prefix": os.environ["RAW_PREFIX"],
            "ExportFormat": "DYNAMODB_JSON",
            "ClientToken": f"{pipeline}-{export_to:%Y%m%d%H%M}",
        }
        if export_from_raw:
            export_from = _parse(export_from_raw)
            if export_from >= export_to:
                return {"skip": True, "reason": "No new PITR window"}
            request.update(
                ExportType="INCREMENTAL_EXPORT",
                IncrementalExportSpecification={
                    "ExportFromTime": export_from,
                    "ExportToTime": export_to,
                    "ExportViewType": "NEW_AND_OLD_IMAGES",
                },
            )
        else:
            # First run is the bootstrap snapshot. Its timestamp becomes the watermark.
            request["ExportTime"] = export_to

        export = EXPORT.export_table_to_point_in_time(**request)["ExportDescription"]
        return {
            "skip": False,
            "export_arn": export["ExportArn"],
            "export_to": _iso(export_to),
        }

    if action == "status":
        export = EXPORT.describe_export(ExportArn=event["export_arn"])["ExportDescription"]
        export_id = event["export_arn"].rsplit("/", 1)[-1]
        return {
            **event,
            "status": export["ExportStatus"],
            "source_path": (
                f"s3://{os.environ['S3_BUCKET']}/{os.environ['RAW_PREFIX']}/"
                f"AWSDynamoDB/{export_id}/data/"
            ),
            "failure_code": export.get("FailureCode"),
            "failure_message": export.get("FailureMessage"),
        }

    if action == "commit":
        DDB.put_item(
            TableName=control_table,
            Item={
                "pipeline": {"S": pipeline},
                "watermark": {"S": event["export_to"]},
                "export_arn": {"S": event["export_arn"]},
            },
        )
        return {"committed": True, "watermark": event["export_to"]}

    raise ValueError(f"Unsupported action: {action}")
