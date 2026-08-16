"""Lambda actions for preparing and committing daily RDS archive windows."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3

DDB = boto3.client("dynamodb")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """Return the next non-overlapping window or commit it after Glue succeeds."""
    action = event["action"]
    table = os.environ["CONTROL_TABLE"]
    pipeline = os.environ["PIPELINE_NAME"]

    if action == "prepare":
        response = DDB.get_item(
            TableName=table,
            Key={"pipeline": {"S": pipeline}},
            ConsistentRead=True,
        )
        stored = response.get("Item", {}).get("watermark", {}).get("S")
        if not stored:
            raise RuntimeError(
                "Control watermark is missing. Run deploy.setup() after the DMS "
                "bootstrap and partition job complete."
            )
        start = _parse(stored)
        # Normalize về 00:00 UTC để mỗi window chứa trọn ngày nghiệp vụ. Nếu giữ
        # cả giờ/phút, hai lần chạy kế tiếp có thể cùng ghi một day partition.
        end = (datetime.now(timezone.utc) - timedelta(
            days=int(os.environ["RETENTION_DAYS"])
        )).replace(hour=0, minute=0, second=0, microsecond=0)
        if start >= end:
            return {"skip": True, "reason": "No eligible archive window"}
        return {
            "skip": False,
            "window_from": _iso(start),
            "window_to": _iso(end),
            "batch_date": end.strftime("%Y-%m-%d"),
        }

    if action == "commit":
        DDB.put_item(
            TableName=table,
            Item={
                "pipeline": {"S": pipeline},
                "watermark": {"S": event["window_to"]},
                "last_batch_date": {"S": event["batch_date"]},
            },
        )
        return {"committed": True, "watermark": event["window_to"]}

    raise ValueError(f"Unsupported action: {action}")
