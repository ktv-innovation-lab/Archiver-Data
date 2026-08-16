"""AWS Glue job: decode DynamoDB export JSON and write partitioned Parquet."""

from __future__ import annotations

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def attribute(name: str, dynamodb_type: str = "S") -> F.Column:
    """Read one DynamoDB AttributeValue without silently exposing its wrapper."""
    return F.col(f"_record.{name}.{dynamodb_type}")


def normalize_orders(source: DataFrame) -> DataFrame:
    """Map the demo orders table to an explicit, Athena-friendly schema."""
    return (
        source.select(
            attribute("order_id").alias("order_id"),
            attribute("created_at").alias("created_at"),
            attribute("updated_at").alias("updated_at"),
            attribute("closed_at").alias("closed_at"),
            attribute("customer_id").alias("customer_id"),
            attribute("status").alias("status"),
            attribute("amount", "N").cast(DoubleType()).alias("amount"),
            attribute("currency").alias("currency"),
        )
        .withColumn("created_at", F.to_timestamp("created_at"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("closed_at", F.to_timestamp("closed_at"))
        # Partition theo ngày nghiệp vụ, không theo ngày pipeline chạy. Order đã
        # đóng dùng closed_at; record chưa có closed_at fallback về created_at.
        .withColumn("_partition_date", F.coalesce(F.to_date("closed_at"), F.to_date("created_at")))
        .withColumn("year", F.date_format("_partition_date", "yyyy"))
        .withColumn("month", F.date_format("_partition_date", "MM"))
        .withColumn("day", F.date_format("_partition_date", "dd"))
        .drop("_partition_date")
    )


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "SOURCE_PATH", "TARGET_PATH"])
    glue_context = GlueContext(SparkContext.getOrCreate())
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # DynamoDB native export writes newline-delimited JSON, usually gzip-compressed.
    source = (
        glue_context.spark_session.read
        .option("recursiveFileLookup", "true")
        .json(args["SOURCE_PATH"])
    )
    # Full exports use Item; incremental exports use NewImage/OldImage.
    # Deleted records have no NewImage and are intentionally excluded from this
    # append-only archive. Use Iceberg MERGE if delete propagation is needed.
    if "Item" not in source.columns and "NewImage" not in source.columns:
        # A window containing only deletes has nothing to append to this archive.
        job.commit()
        return
    record_column = "Item" if "Item" in source.columns else "NewImage"
    source = source.withColumn("_record", F.col(record_column)).where(F.col("_record").isNotNull())
    normalized = normalize_orders(source).dropDuplicates(["order_id", "created_at"])

    (
        normalized.write.mode("append")
        .partitionBy("year", "month", "day")
        .parquet(args["TARGET_PATH"])
    )
    job.commit()


if __name__ == "__main__":
    main()
