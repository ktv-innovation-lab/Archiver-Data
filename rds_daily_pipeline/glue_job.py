"""Extract one closed RDS time window and publish it as partitioned Parquet."""

from __future__ import annotations

import re
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def _identifier(value: str) -> str:
    """Allow only PostgreSQL identifiers before interpolating the SQL query."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"Invalid PostgreSQL identifier: {value!r}")
    return value


def _timestamp(value: str) -> str:
    """Validate an ISO timestamp and return a PostgreSQL-safe UTC literal."""
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.isoformat()


def main() -> None:
    keys = [
        "JOB_NAME", "CONNECTION_NAME", "SOURCE_SCHEMA", "SOURCE_TABLE",
        "DATE_COLUMN", "WINDOW_FROM", "WINDOW_TO", "BATCH_DATE",
        "RAW_PATH", "CURATED_PATH",
    ]
    args = getResolvedOptions(sys.argv, keys)
    glue_context = GlueContext(SparkContext.getOrCreate())
    glue_context.spark_session.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    schema = _identifier(args["SOURCE_SCHEMA"])
    table = _identifier(args["SOURCE_TABLE"])
    column = _identifier(args["DATE_COLUMN"])
    window_from = _timestamp(args["WINDOW_FROM"])
    window_to = _timestamp(args["WINDOW_TO"])
    jdbc = glue_context.extract_jdbc_conf(args["CONNECTION_NAME"])
    query = (
        f'SELECT * FROM "{schema}"."{table}" '
        f'WHERE "{column}" > TIMESTAMPTZ \'{window_from}\' '
        f'AND "{column}" <= TIMESTAMPTZ \'{window_to}\''
    )
    frame = (
        glue_context.spark_session.read.format("jdbc")
        .option("url", jdbc["fullUrl"])
        .option("user", jdbc["user"])
        .option("password", jdbc["password"])
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .load()
    )

    # Partition bằng ngày nghiệp vụ trong DATE_COLUMN. Vì vậy một lần catch-up
    # nhiều ngày vẫn tách thành các folder ngày riêng, không gom theo batch chạy.
    partitioned = (
        frame.withColumn("_partition_date", F.to_date(F.col(column)))
        .withColumn("year", F.date_format("_partition_date", "yyyy"))
        .withColumn("month", F.date_format("_partition_date", "MM"))
        .withColumn("day", F.date_format("_partition_date", "dd"))
        .drop("_partition_date")
    )
    (
        partitioned.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(args["RAW_PATH"])
    )
    (
        partitioned.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(args["CURATED_PATH"])
    )
    job.commit()


if __name__ == "__main__":
    main()
