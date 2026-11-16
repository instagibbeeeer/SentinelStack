"""Canonical Spark window contract, exported unchanged to SentinelFlow's Spark job.

This module depends only on PySpark and the standard library. Keep the exported
copy byte-identical; integration tests check both the source and computed rows.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

FEATURE_VERSION = "sentinelflow-window-v2"
FEATURE_COLUMNS = [
    "failed_logins",
    "privilege_changes",
    "log_download_bytes",
    "unique_ips",
    "countries",
    "event_count",
    "login_successes",
    "process_starts",
]
EVENT_TYPES = [
    "LOGIN_SUCCESS",
    "LOGIN_FAILED",
    "FILE_DOWNLOAD",
    "PRIVILEGE_CHANGE",
    "PROCESS_START",
]
EVENT_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType()),
        T.StructField("timestamp", T.StringType()),
        T.StructField("user_id", T.StringType()),
        T.StructField("device_id", T.StringType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("ip", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("bytes", T.LongType()),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
        T.StructField("_corrupt_record", T.StringType()),
    ]
)


def window_id() -> F.Column:
    return F.sha2(
        F.concat_ws(
            "|",
            F.lit(FEATURE_VERSION),
            F.col("window_seconds").cast("string"),
            F.col("slide_seconds").cast("string"),
            F.col("user_id"),
            F.unix_micros("window_start").cast("string"),
            F.unix_micros("window_end").cast("string"),
        ),
        256,
    )


def build_window_features(
    events: DataFrame, window_seconds: int = 30, slide_seconds: int = 10
) -> DataFrame:
    if window_seconds < 1 or not 1 <= slide_seconds <= window_seconds:
        raise ValueError("require 1 <= slide_seconds <= window_seconds")
    if "event_ts" not in events.columns:
        events = events.withColumn("event_ts", F.try_to_timestamp("timestamp"))
    windows = events.groupBy(
        F.window("event_ts", f"{window_seconds} seconds", f"{slide_seconds} seconds"),
        "user_id",
    ).agg(
        F.sum(F.when(F.col("event_type") == "LOGIN_FAILED", 1).otherwise(0)).alias("failed_logins"),
        F.sum(F.when(F.col("event_type") == "PRIVILEGE_CHANGE", 1).otherwise(0)).alias(
            "privilege_changes"
        ),
        F.sum(
            F.when(
                F.col("event_type") == "FILE_DOWNLOAD", F.coalesce(F.col("bytes"), F.lit(0))
            ).otherwise(0)
        ).alias("download_bytes"),
        F.approx_count_distinct("ip").alias("unique_ips"),
        F.approx_count_distinct("country").alias("countries"),
        F.count(F.lit(1)).alias("event_count"),
        F.sum(F.when(F.col("event_type") == "LOGIN_SUCCESS", 1).otherwise(0)).alias(
            "login_successes"
        ),
        F.sum(F.when(F.col("event_type") == "PROCESS_START", 1).otherwise(0)).alias(
            "process_starts"
        ),
    )
    return (
        windows.withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .withColumn("window_seconds", F.lit(window_seconds))
        .withColumn("slide_seconds", F.lit(slide_seconds))
        .withColumn("feature_version", F.lit(FEATURE_VERSION))
        .withColumn("log_download_bytes", F.log1p(F.col("download_bytes")))
        .withColumn("window_id", window_id())
    )


def add_rule_score(
    frame: DataFrame,
    failed_login_weight: float = 0.055,
    privilege_change_weight: float = 0.25,
    large_download_weight: float = 0.30,
    multi_country_weight: float = 0.10,
    large_download_bytes: int = 1_000_000_000,
) -> DataFrame:
    return frame.withColumn(
        "risk_score",
        F.least(
            F.lit(1.0),
            F.col("failed_logins") * F.lit(failed_login_weight)
            + F.col("privilege_changes") * F.lit(privilege_change_weight)
            + F.when(
                F.col("download_bytes") > large_download_bytes, large_download_weight
            ).otherwise(0.0)
            + F.when(F.col("countries") > 1, multi_country_weight).otherwise(0.0),
        ),
    )


def public_features(frame: DataFrame) -> DataFrame:
    return frame.select(
        "window_id",
        "user_id",
        "window_start",
        "window_end",
        "window_seconds",
        "slide_seconds",
        "feature_version",
        "failed_logins",
        "privilege_changes",
        "download_bytes",
        "log_download_bytes",
        "unique_ips",
        "countries",
        "event_count",
        "login_successes",
        "process_starts",
    )
