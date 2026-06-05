"""Helpers for building a local SparkSession for the training kit.

The kit is meant to run on a single laptop with no pre-installed Java. This
module locates the JDK that ``scripts/bootstrap_java.py`` downloaded into
``.jdk/`` (or an existing ``JAVA_HOME``) and configures a small, sane local
Spark session.
"""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
JDK_DIR = PROJECT_ROOT / ".jdk"
DATA_DIR = PROJECT_ROOT / "data"


def find_java_home() -> str | None:
    """Locate a Java 17+ home: existing ``JAVA_HOME`` first, then ``.jdk/``."""
    env_home = os.environ.get("JAVA_HOME")
    if env_home and (Path(env_home) / "bin" / "java").exists():
        return env_home
    if JDK_DIR.exists():
        for java in JDK_DIR.rglob("bin/java"):
            return str(java.parent.parent)
    return None


def ensure_java() -> str:
    """Return a usable ``JAVA_HOME`` or raise with a helpful message."""
    home = find_java_home()
    if home is None:
        raise RuntimeError(
            "No Java runtime found. Run:\n"
            "    uv run python scripts/bootstrap_java.py\n"
            "or set JAVA_HOME to a JDK 17+ installation."
        )
    os.environ["JAVA_HOME"] = home
    os.environ["PATH"] = f"{home}/bin{os.pathsep}{os.environ.get('PATH', '')}"
    return home


def build_session(app_name: str = "fmcg-training", shuffle_partitions: int = 8):
    """Build a local SparkSession tuned for laptop-scale training.

    Args:
        app_name: Spark application name.
        shuffle_partitions: ``spark.sql.shuffle.partitions``. Kept small so
            local jobs don't spawn 200 tiny tasks. Performance exercises may
            override this to demonstrate the effect.
    """
    ensure_java()

    # Import after JAVA_HOME is set so the JVM launches against the right Java.
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", "2g")
        # Keep Spark's own logs quiet during exercises.
        .config("spark.sql.adaptive.enabled", "true")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def data_path(*parts: str) -> str:
    """Absolute path inside the kit's ``data/`` directory."""
    return str(DATA_DIR.joinpath(*parts))


def read_dim(spark, name: str):
    """Read a clean Parquet dimension/fact (e.g. ``dim_products``)."""
    return spark.read.parquet(data_path(name))


def read_sales_raw(spark):
    """Read the raw, deliberately-dirty ``fact_sales`` CSV (all columns string).

    Nothing is cleaned or cast here on purpose — that's the candidate's job.
    """
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(data_path("fact_sales_csv"))
    )


def read_sales_typed(spark):
    """Return a typed, de-duplicated, valid-rows-only sales fact with revenue.

    Convenience for notebooks that focus on joins/windows/performance rather
    than cleaning (notebook 03 covers building this from scratch). Drops
    duplicate transactions and rows with non-positive/invalid quantity or
    out-of-range discount, then adds ``revenue``.
    """
    from pyspark.sql import functions as F

    raw = read_sales_raw(spark)
    return (
        raw.withColumn("txn_date", F.to_date("txn_date"))
        .withColumn("quantity", F.expr("try_cast(quantity as int)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price as double)"))
        .withColumn("discount", F.expr("try_cast(discount as double)"))
        .dropDuplicates(["txn_id"])
        .filter((F.col("quantity") > 0) & F.col("discount").between(0, 1))
        .withColumn(
            "revenue",
            F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount")),
        )
    )
