"""Repartition the one-time DMS full-load output into Hive-style day folders."""

from __future__ import annotations

import re
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F


def _identifier(value: str) -> str:
    """Validate a column name before passing it to Spark."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", value):
        raise ValueError(f"Invalid DATE_COLUMN: {value!r}")
    return value


def main() -> None:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH", "DATE_COLUMN"],
    )
    glue_context = GlueContext(SparkContext.getOrCreate())
    spark = glue_context.spark_session
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    date_column = _identifier(args["DATE_COLUMN"])
    source = (
        spark.read.option("recursiveFileLookup", "true")
        .parquet(args["SOURCE_PATH"])
    )
    if date_column not in source.columns:
        raise ValueError(
            f"DATE_COLUMN={date_column!r} does not exist in DMS output. "
            f"Available columns: {source.columns}"
        )

    partitioned = (
        source.where(F.col(date_column).isNotNull())
        .withColumn("_partition_date", F.to_date(F.col(date_column)))
        .withColumn("year", F.date_format("_partition_date", "yyyy"))
        .withColumn("month", F.date_format("_partition_date", "MM"))
        .withColumn("day", F.date_format("_partition_date", "dd"))
        .drop("_partition_date")
    )
    (
        partitioned.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(args["TARGET_PATH"])
    )
    job.commit()


if __name__ == "__main__":
    main()
