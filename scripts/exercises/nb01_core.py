"""Notebook 01 — core DataFrame operations."""

from __future__ import annotations

from .common import Notebook, Task, setup_cell

INTRO = """Core single-table DataFrame skills: imposing types on raw data,
deriving columns, filtering, and aggregating.

Each task has a **CHECK** cell below it — run it to grade your answer. Write your
solution in the cell with the `# TODO`."""

SETUP_EXTRA = '''sales_raw = read_sales_raw(spark)
products = read_dim(spark, "dim_products")
stores = read_dim(spark, "dim_stores")
customers = read_dim(spark, "dim_customers")
print("raw sales rows:", sales_raw.count())
sales_raw.printSchema()'''


def get() -> Notebook:
    return Notebook(
        slug="01_core_dataframe_ops",
        title="01 · Core DataFrame Operations",
        intro=INTRO,
        setup=setup_cell("nb01", SETUP_EXTRA),
        tasks=[
            Task(
                title="Impose types (safe casting)",
                prompt=(
                    "The raw sales CSV has **every column as a string**, and "
                    "`quantity` even contains junk like `\"N/A\"`. Build a typed "
                    "DataFrame `sales` from `sales_raw` with `txn_date`→date, "
                    "`quantity`→int, `unit_price`→double, `discount`→double. Keep "
                    "all other columns.\n\n"
                    "Use **safe** casting so bad values become `null` instead of "
                    "crashing — Spark 4 runs in ANSI mode, so a plain "
                    "`.cast('int')` on `\"N/A\"` throws. Use `try_cast` "
                    "(`F.expr(\"try_cast(col as int)\")`)."
                ),
                stub='# TODO: build `sales` with proper types using try_cast\nsales = sales_raw  # replace me\nsales.printSchema()',
                solution=(
                    'sales = (\n'
                    '    sales_raw\n'
                    '    .withColumn("txn_date", F.to_date("txn_date"))\n'
                    '    .withColumn("quantity", F.expr("try_cast(quantity as int)"))\n'
                    '    .withColumn("unit_price", F.expr("try_cast(unit_price as double)"))\n'
                    '    .withColumn("discount", F.expr("try_cast(discount as double)"))\n'
                    ')\n'
                    'sales.printSchema()'
                ),
                check=(
                    'types = dict(sales.dtypes)\n'
                    'assert types["txn_date"] == "date", types["txn_date"]\n'
                    'assert types["quantity"] == "int", types["quantity"]\n'
                    'assert types["unit_price"] == "double"\n'
                    'assert types["discount"] == "double"\n'
                    '# safe cast turned non-numeric quantities into nulls (no crash)\n'
                    'assert sales.filter(F.col("quantity").isNull()).count() > 0\n'
                    'print("✅ Task 1 passed")'
                ),
            ),
            Task(
                title="Derive a revenue column",
                prompt=(
                    "Add a `revenue` column to `sales`, defined as "
                    "`quantity × unit_price × (1 − discount)`. Name the result "
                    "`sales_rev`. Rows with a null quantity should naturally get a "
                    "null revenue."
                ),
                stub='# TODO: add `revenue`\nsales_rev = sales\nsales_rev.select("txn_id", "quantity", "unit_price", "discount").show(5)',
                solution=(
                    'sales_rev = sales.withColumn(\n'
                    '    "revenue",\n'
                    '    F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount")),\n'
                    ')\n'
                    'sales_rev.select("txn_id", "quantity", "unit_price", "discount", "revenue").show(5)'
                ),
                check=(
                    'assert "revenue" in sales_rev.columns\n'
                    '_ref = sales.withColumn("_r", F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")))\n'
                    '_exp = _ref.agg(F.round(F.sum("_r"), 2)).first()[0]\n'
                    '_got = sales_rev.agg(F.round(F.sum("revenue"), 2)).first()[0]\n'
                    'assert _exp is not None and abs(_exp - _got) < 1.0, (_exp, _got)\n'
                    'print("✅ Task 2 passed — total revenue:", _got)'
                ),
            ),
            Task(
                title="Filter to valid rows",
                prompt=(
                    "Create `valid_sales` from `sales_rev` keeping only rows with a "
                    "**positive quantity** AND a **discount within [0, 1]** (this "
                    "drops negatives, the `N/A` nulls, and out-of-range discounts). "
                    "Select just: `txn_id`, `txn_date`, `store_id`, `product_id`, "
                    "`quantity`, `revenue`."
                ),
                stub='# TODO\nvalid_sales = sales_rev',
                solution=(
                    'valid_sales = sales_rev.filter(\n'
                    '    (F.col("quantity") > 0) & F.col("discount").between(0, 1)\n'
                    ').select("txn_id", "txn_date", "store_id", "product_id", "quantity", "revenue")\n'
                    'valid_sales.show(5)'
                ),
                check=(
                    'assert set(valid_sales.columns) == {"txn_id", "txn_date", "store_id", "product_id", "quantity", "revenue"}\n'
                    'assert valid_sales.filter(F.col("quantity") <= 0).count() == 0\n'
                    'assert valid_sales.filter(F.col("revenue").isNull()).count() == 0\n'
                    'print("✅ Task 3 passed — valid rows:", valid_sales.count())'
                ),
            ),
            Task(
                title="Aggregate per store",
                prompt=(
                    "From `valid_sales`, compute per `store_id`: `total_revenue` "
                    "(rounded to 2 dp), `total_units` (sum of quantity), and "
                    "`n_txns` (row count). Order by `total_revenue` descending. "
                    "Name it `store_summary`."
                ),
                stub='# TODO\nstore_summary = valid_sales',
                solution=(
                    'store_summary = (\n'
                    '    valid_sales.groupBy("store_id")\n'
                    '    .agg(\n'
                    '        F.round(F.sum("revenue"), 2).alias("total_revenue"),\n'
                    '        F.sum("quantity").alias("total_units"),\n'
                    '        F.count("*").alias("n_txns"),\n'
                    '    )\n'
                    '    .orderBy(F.desc("total_revenue"))\n'
                    ')\n'
                    'store_summary.show(5)'
                ),
                check=(
                    'assert set(store_summary.columns) == {"store_id", "total_revenue", "total_units", "n_txns"}\n'
                    '_top = store_summary.first()\n'
                    '# the mega-store ST001 should top the revenue ranking\n'
                    'assert _top["store_id"] == "ST001", _top["store_id"]\n'
                    'print("✅ Task 4 passed — top store:", _top["store_id"])'
                ),
            ),
            Task(
                title="Monthly revenue trend",
                prompt=(
                    "Add a `year_month` column (formatted `yyyy-MM`) to "
                    "`valid_sales` and compute monthly total revenue as "
                    "`monthly_rev` with columns `year_month`, `revenue` (rounded), "
                    "ordered by `year_month`."
                ),
                stub='# TODO\nmonthly_rev = valid_sales',
                solution=(
                    'monthly_rev = (\n'
                    '    valid_sales\n'
                    '    .withColumn("year_month", F.date_format("txn_date", "yyyy-MM"))\n'
                    '    .groupBy("year_month")\n'
                    '    .agg(F.round(F.sum("revenue"), 2).alias("revenue"))\n'
                    '    .orderBy("year_month")\n'
                    ')\n'
                    'monthly_rev.show()'
                ),
                check=(
                    'import re\n'
                    'assert monthly_rev.columns == ["year_month", "revenue"]\n'
                    'assert monthly_rev.count() >= 6\n'
                    'assert all(re.match(r"\\d{4}-\\d{2}$", r["year_month"]) for r in monthly_rev.collect())\n'
                    'print("✅ Task 5 passed")'
                ),
            ),
        ],
    )
