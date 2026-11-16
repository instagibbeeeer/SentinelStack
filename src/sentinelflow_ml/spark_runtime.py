

import os
from functools import lru_cache

from pyspark import SparkConf
from pyspark.sql import SparkSession

SPARK_VERSION = "4.2.0"


@lru_cache(maxsize=4)
def get_spark(master: str | None = None, packages: str = "") -> SparkSession:
    master = master or os.getenv("SPARK_MASTER") or SparkConf().get("spark.master", "local[2]")
    builder = (
        SparkSession.builder.appName("SentinelFlowML")
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "2"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.enabled", "false")
    )
    builder = builder.config("spark.ui.showConsoleProgress", "false")
    if master.startswith("local"):
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1").config(
            "spark.driver.host", "127.0.0.1"
        )
    packages = packages or os.getenv("SPARK_EXTRA_PACKAGES", "")
    if packages:
        builder = builder.config("spark.jars.packages", packages)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def stop_spark() -> None:
    session = SparkSession.getActiveSession()
    if session:
        session.stop()
    get_spark.cache_clear()
