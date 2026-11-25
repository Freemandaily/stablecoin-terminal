"""Shared Spark session builder for the StableFlow lakehouse.

Same pattern as the working Solana-Data job: Iceberg Hadoop catalog on GCS,
packages requested via `spark.jars.packages`, version 2.1 on Dataproc.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

CATALOG_NAME = "iceberg"
DEFAULT_WAREHOUSE = os.environ.get("ICEBERG_WAREHOUSE", "gs://term-flow/warehouse")


def _on_dataproc() -> bool:
    return (
        os.environ.get("DATAPROC_IMAGE_VERSION") is not None
        or os.environ.get("DATAPROC_VERSION") is not None
        or os.path.exists("/etc/dataproc")
        or os.path.exists("/usr/local/share/google/dataproc")
    )


def get_spark(
    app_name: str = "stableflow-lakehouse",
    *,
    warehouse: str = DEFAULT_WAREHOUSE,
    shuffle_partitions: str = "512",
) -> SparkSession:
    """Return a SparkSession configured for Iceberg HadoopCatalog via GCS."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl",
            "org.apache.iceberg.hadoop.HadoopCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse)
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.io-impl",
            "org.apache.iceberg.hadoop.HadoopFileIO",
        )
        # Row-level DELETE (empty-day partition reset) uses copy-on-write.
        .config(f"spark.sql.catalog.{CATALOG_NAME}.write.delete.mode", "copy-on-write")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.write.update.mode", "copy-on-write")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.write.merge.mode", "copy-on-write")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11,"
            "org.apache.iceberg:iceberg-gcp-bundle:1.6.1,"
            "org.scala-lang:scala-library:2.12.18",
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
    )
    # Local/WSL uses ADC; Dataproc uses the VM service account.
    if not _on_dataproc():
        builder = builder.config(
            "spark.hadoop.google.cloud.auth.service.account.enable", "false"
        )
    return builder.getOrCreate()
