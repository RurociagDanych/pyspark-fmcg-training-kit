"""Notebook 04 — performance & advanced topics."""

from __future__ import annotations

from .common import Notebook, Task, setup_cell

INTRO = """Performance and advanced patterns: partitioning & pruning, broadcast
join tuning, diagnosing skew, caching, UDF vs native functions, and Spark SQL.

`sales` is the typed/clean fact (`read_sales_typed`)."""

SETUP_EXTRA = '''sales = read_sales_typed(spark)
products = read_dim(spark, "dim_products").withColumn("category", F.initcap(F.trim("category")))
print("typed sales rows:", sales.count())'''


def get() -> Notebook:
    return Notebook(
        slug="04_performance_advanced",
        title="04 · Performance & Advanced",
        intro=INTRO,
        setup=setup_cell("nb04", SETUP_EXTRA),
        tasks=[
            Task(
                title="Partitioning & partition pruning",
                prompt=(
                    "Add a `txn_month` column (`yyyy-MM`) to `sales` and write it "
                    "to a temp directory **partitioned by `txn_month`** as Parquet. "
                    "Provide `out_path` (the directory) and `months` (the list of "
                    "`txn_month=...` partition folders). Then read it back filtering "
                    "to `'2026-05'` into `one_month` — Spark prunes the other "
                    "partitions."
                ),
                stub='# TODO: write partitioned, then read one partition\nimport tempfile, os\nout_path = tempfile.mkdtemp(prefix="sales_part_")\nmonths = []\none_month = sales',
                solution=(
                    'import tempfile, os\n'
                    'out_path = tempfile.mkdtemp(prefix="sales_part_")\n'
                    '(\n'
                    '    sales.withColumn("txn_month", F.date_format("txn_date", "yyyy-MM"))\n'
                    '    .write.mode("overwrite").partitionBy("txn_month").parquet(out_path)\n'
                    ')\n'
                    'months = [d for d in os.listdir(out_path) if d.startswith("txn_month=")]\n'
                    'one_month = spark.read.parquet(out_path).filter(F.col("txn_month") == "2026-05")\n'
                    'print("partitions:", len(months), "| 2026-05 rows:", one_month.count())'
                ),
                check=(
                    'assert len(months) >= 6, months\n'
                    'assert one_month.filter(F.col("txn_month") != "2026-05").count() == 0\n'
                    'print("✅ Task 1 passed — partitions:", len(months))'
                ),
            ),
            Task(
                title="Broadcast vs shuffle join",
                prompt=(
                    "Join `sales` to `products` two ways and inspect the physical "
                    "plans: `no_bc` (plain join) and `with_bc` (broadcasting "
                    "`products`). Capture `with_bc`'s executed plan as the string "
                    "`bc_plan` and confirm it uses a broadcast join."
                ),
                stub='# TODO\nno_bc = sales.join(products, "product_id")\nwith_bc = sales\nbc_plan = ""',
                solution=(
                    'no_bc = sales.join(products, "product_id")\n'
                    'with_bc = sales.join(F.broadcast(products), "product_id")\n'
                    'bc_plan = with_bc._jdf.queryExecution().executedPlan().toString()\n'
                    'print(bc_plan[:400])'
                ),
                check=(
                    'assert "BroadcastHashJoin" in bc_plan or "Broadcast" in bc_plan\n'
                    'print("✅ Task 2 passed — broadcast confirmed")'
                ),
            ),
            Task(
                title="Diagnose data skew",
                prompt=(
                    "One store dominates the sales (a classic skew problem). "
                    "Compute `top_store` (the `store_id` with the most rows) and "
                    "`top_share` (its fraction of all rows, 0–1). A naive "
                    "`groupBy(\"store_id\")` puts a disproportionate load on the "
                    "task handling this key."
                ),
                stub='# TODO\ntop_store = None\ntop_share = 0.0',
                solution=(
                    '_counts = sales.groupBy("store_id").count()\n'
                    '_total = sales.count()\n'
                    '_top = _counts.orderBy(F.desc("count")).first()\n'
                    'top_store = _top["store_id"]\n'
                    'top_share = _top["count"] / _total\n'
                    'print("skewed store:", top_store, "share:", round(top_share, 3))'
                ),
                check=(
                    'assert top_store == "ST001", top_store\n'
                    'assert top_share > 0.1, top_share\n'
                    'print("✅ Task 3 passed — skew on", top_store, round(top_share, 3))'
                ),
            ),
            Task(
                title="Cache an intermediate",
                prompt=(
                    "Cache a filtered intermediate so repeated actions reuse it. "
                    "Create `cached = sales.filter(revenue > 5)`, mark it cached, "
                    "and materialize it with a `count()`. Confirm it's held in "
                    "memory."
                ),
                stub='# TODO\ncached = sales.filter(F.col("revenue") > 5)',
                solution=(
                    'cached = sales.filter(F.col("revenue") > 5).cache()\n'
                    '_ = cached.count()  # materialize the cache\n'
                    'print("in memory:", cached.storageLevel.useMemory, "| rows:", cached.count())'
                ),
                check=(
                    'assert cached.storageLevel.useMemory, "DataFrame is not cached in memory"\n'
                    'print("✅ Task 4 passed")'
                ),
            ),
            Task(
                title="UDF vs native expression",
                prompt=(
                    "Label each sale's discount band: `'none'` if discount == 0, "
                    "`'low'` if < 0.2, else `'high'`. Implement it **two ways** on "
                    "`sales` → `banded`: a Python UDF column `band_udf` and a "
                    "native `when` column `band_native`. They must agree. "
                    "(Native is preferred — no Python serialization per row.)"
                ),
                stub='# TODO\nbanded = sales',
                solution=(
                    'from pyspark.sql.types import StringType\n'
                    '\n'
                    'def _band(d):\n'
                    '    if d is None:\n'
                    '        return None\n'
                    '    if d == 0:\n'
                    '        return "none"\n'
                    '    return "low" if d < 0.2 else "high"\n'
                    '\n'
                    'band_udf = F.udf(_band, StringType())\n'
                    'banded = (\n'
                    '    sales\n'
                    '    .withColumn("band_udf", band_udf(F.col("discount")))\n'
                    '    .withColumn(\n'
                    '        "band_native",\n'
                    '        F.when(F.col("discount") == 0, "none")\n'
                    '        .when(F.col("discount") < 0.2, "low")\n'
                    '        .otherwise("high"),\n'
                    '    )\n'
                    ')\n'
                    'banded.select("discount", "band_udf", "band_native").show(5)'
                ),
                check=(
                    '_mismatch = banded.filter(\n'
                    '    F.col("discount").isNotNull() & (F.col("band_udf") != F.col("band_native"))\n'
                    ').count()\n'
                    'assert _mismatch == 0, ("udf/native disagree on", _mismatch, "rows")\n'
                    'print("✅ Task 5 passed — UDF and native agree; prefer native")'
                ),
            ),
            Task(
                title="Equivalent Spark SQL",
                prompt=(
                    "Register `sales` as a temp view `sales_v` and write a SQL "
                    "query returning per-store `total_revenue` (rounded) and "
                    "`n` (txn count), top 5 by revenue. Name the result "
                    "`sql_result`."
                ),
                stub='# TODO\nsales.createOrReplaceTempView("sales_v")\nsql_result = spark.sql("SELECT 1")',
                solution=(
                    'sales.createOrReplaceTempView("sales_v")\n'
                    'sql_result = spark.sql(\n'
                    '    "SELECT store_id, round(sum(revenue), 2) AS total_revenue, count(*) AS n "\n'
                    '    "FROM sales_v GROUP BY store_id ORDER BY total_revenue DESC LIMIT 5"\n'
                    ')\n'
                    'sql_result.show()'
                ),
                check=(
                    'assert sql_result.count() == 5\n'
                    'assert sql_result.first()["store_id"] == "ST001"\n'
                    'print("✅ Task 6 passed")'
                ),
            ),
        ],
    )
