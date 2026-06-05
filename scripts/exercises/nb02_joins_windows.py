"""Notebook 02 — joins and window functions."""

from __future__ import annotations

from .common import Notebook, Task, setup_cell

INTRO = """Multi-table joins and window functions on enriched sales.

`sales` here is already typed and cleaned for you via `read_sales_typed` (you
built that by hand in notebook 03's territory). The store/category text is
normalized in setup so grouping behaves — text cleaning is covered in
notebook 03."""

SETUP_EXTRA = '''sales = read_sales_typed(spark)
# Normalize dimension text up front so groupBy keys collapse cleanly.
# (The casing/whitespace cleaning technique itself is taught in notebook 03.)
products = read_dim(spark, "dim_products").withColumn("category", F.initcap(F.trim("category")))
stores = read_dim(spark, "dim_stores").withColumn("region", F.initcap(F.trim("region")))
customers = read_dim(spark, "dim_customers")
print("typed sales rows:", sales.count())'''


def get() -> Notebook:
    return Notebook(
        slug="02_joins_and_windows",
        title="02 · Joins & Window Functions",
        intro=INTRO,
        setup=setup_cell("nb02", SETUP_EXTRA),
        tasks=[
            Task(
                title="Enrich sales with dimensions",
                prompt=(
                    "Inner-join `sales` to `products` (on `product_id`) and "
                    "`stores` (on `store_id`) to add `category`, `brand`, "
                    "`region`, `channel`. Name the result `enriched`.\n\n"
                    "Note: some sales reference non-existent products (orphan "
                    "FKs); an inner join drops them, which is fine here."
                ),
                stub='# TODO\nenriched = sales',
                solution=(
                    'enriched = (\n'
                    '    sales\n'
                    '    .join(products.select("product_id", "category", "brand"), "product_id")\n'
                    '    .join(stores.select("store_id", "region", "channel"), "store_id")\n'
                    ')\n'
                    'enriched.select("txn_id", "category", "brand", "region", "channel", "revenue").show(5)'
                ),
                check=(
                    'for _c in ["category", "brand", "region", "channel"]:\n'
                    '    assert _c in enriched.columns, _c\n'
                    'assert enriched.count() <= sales.count()  # inner join may drop orphans\n'
                    'print("✅ Task 1 passed — enriched rows:", enriched.count())'
                ),
            ),
            Task(
                title="Top 3 brands per region",
                prompt=(
                    "Using `enriched`, total revenue per `(region, brand)`, then "
                    "rank brands within each region by revenue and keep the top 3. "
                    "Name it `top_brands` with columns `region`, `brand`, "
                    "`brand_revenue`, `rnk`. Use a window + `row_number`."
                ),
                stub='# TODO\ntop_brands = enriched',
                solution=(
                    '_rev = enriched.groupBy("region", "brand").agg(\n'
                    '    F.round(F.sum("revenue"), 2).alias("brand_revenue")\n'
                    ')\n'
                    '_w = Window.partitionBy("region").orderBy(F.desc("brand_revenue"))\n'
                    'top_brands = (\n'
                    '    _rev.withColumn("rnk", F.row_number().over(_w))\n'
                    '    .filter(F.col("rnk") <= 3)\n'
                    '    .orderBy("region", "rnk")\n'
                    ')\n'
                    'top_brands.show()'
                ),
                check=(
                    'assert {"region", "brand", "brand_revenue", "rnk"} <= set(top_brands.columns)\n'
                    'assert top_brands.groupBy("region").count().filter("count > 3").count() == 0\n'
                    'print("✅ Task 2 passed")'
                ),
            ),
            Task(
                title="Broadcast join",
                prompt=(
                    "Re-do the product join but explicitly **broadcast** the small "
                    "`products` dimension to avoid a shuffle. Name it "
                    "`enriched_bc`. (Use `F.broadcast`.)"
                ),
                stub='# TODO\nenriched_bc = sales',
                solution=(
                    'enriched_bc = sales.join(\n'
                    '    F.broadcast(products.select("product_id", "category")), "product_id"\n'
                    ')\n'
                    'enriched_bc.show(5)'
                ),
                check=(
                    '_plan = enriched_bc._jdf.queryExecution().executedPlan().toString()\n'
                    'assert "Broadcast" in _plan, "expected a broadcast join in the physical plan"\n'
                    'print("✅ Task 3 passed — broadcast join confirmed")'
                ),
            ),
            Task(
                title="Rolling 7-day revenue",
                prompt=(
                    "Aggregate `enriched` to daily total revenue (`daily_revenue`), "
                    "then add a 7-day trailing sum `rolling_7d` using a window "
                    "ordered by date with `rowsBetween(-6, 0)`. Name the result "
                    "`daily` with columns `txn_date`, `daily_revenue`, "
                    "`rolling_7d`, ordered by date."
                ),
                stub='# TODO\ndaily = enriched',
                solution=(
                    'daily = enriched.groupBy("txn_date").agg(\n'
                    '    F.round(F.sum("revenue"), 2).alias("daily_revenue")\n'
                    ')\n'
                    '_w7 = Window.orderBy("txn_date").rowsBetween(-6, 0)\n'
                    'daily = daily.withColumn(\n'
                    '    "rolling_7d", F.round(F.sum("daily_revenue").over(_w7), 2)\n'
                    ').orderBy("txn_date")\n'
                    'daily.show(10)'
                ),
                check=(
                    'assert {"txn_date", "daily_revenue", "rolling_7d"} <= set(daily.columns)\n'
                    '_rows = daily.orderBy("txn_date").limit(7).collect()\n'
                    '_exp = round(sum(r["daily_revenue"] for r in _rows), 2)\n'
                    'assert abs(_rows[6]["rolling_7d"] - _exp) < 1.0, (_rows[6]["rolling_7d"], _exp)\n'
                    'print("✅ Task 4 passed")'
                ),
            ),
            Task(
                title="Week-over-week growth (lag)",
                prompt=(
                    "From `enriched`, compute weekly revenue. Build a `year_week` "
                    "key as `concat_ws(\"-W\", year(txn_date), lpad(weekofyear"
                    "(txn_date), 2, \"0\"))` (e.g. `2026-W23`). Then use `lag` to "
                    "get the previous week's revenue and a `wow_pct` growth "
                    "percentage. Name it `wow` with columns `year_week`, "
                    "`weekly_revenue`, `prev_week`, `wow_pct`, ordered by week."
                ),
                stub='# TODO\nwow = enriched',
                solution=(
                    '_year_week = F.concat_ws(\n'
                    '    "-W", F.year("txn_date"), F.lpad(F.weekofyear("txn_date").cast("string"), 2, "0")\n'
                    ')\n'
                    '_weekly = (\n'
                    '    enriched.withColumn("year_week", _year_week)\n'
                    '    .groupBy("year_week")\n'
                    '    .agg(F.round(F.sum("revenue"), 2).alias("weekly_revenue"))\n'
                    ')\n'
                    '_wl = Window.orderBy("year_week")\n'
                    'wow = (\n'
                    '    _weekly.withColumn("prev_week", F.lag("weekly_revenue").over(_wl))\n'
                    '    .withColumn(\n'
                    '        "wow_pct",\n'
                    '        F.round((F.col("weekly_revenue") - F.col("prev_week")) / F.col("prev_week") * 100, 2),\n'
                    '    )\n'
                    '    .orderBy("year_week")\n'
                    ')\n'
                    'wow.show()'
                ),
                check=(
                    'assert {"year_week", "weekly_revenue", "prev_week", "wow_pct"} <= set(wow.columns)\n'
                    'assert wow.orderBy("year_week").first()["prev_week"] is None  # first week has no previous\n'
                    'print("✅ Task 5 passed")'
                ),
            ),
            Task(
                title="Customer RFM scoring",
                prompt=(
                    "For sales with a non-null `customer_id`, compute per customer: "
                    "`recency_days` (days from last purchase to 2026-06-05), "
                    "`frequency` (#txns), `monetary` (total revenue). Then add "
                    "`m_quartile` = `ntile(4)` over monetary. Name it `rfm` with "
                    "columns `customer_id`, `recency_days`, `frequency`, "
                    "`monetary`, `m_quartile`. (Use `sales`, which still has "
                    "`customer_id`.)"
                ),
                stub='# TODO\nrfm = sales',
                solution=(
                    '_ref_date = F.lit("2026-06-05").cast("date")\n'
                    '_base = (\n'
                    '    sales.filter(F.col("customer_id").isNotNull())\n'
                    '    .groupBy("customer_id")\n'
                    '    .agg(\n'
                    '        F.max("txn_date").alias("_last"),\n'
                    '        F.count("*").alias("frequency"),\n'
                    '        F.round(F.sum("revenue"), 2).alias("monetary"),\n'
                    '    )\n'
                    ')\n'
                    'rfm = (\n'
                    '    _base.withColumn("recency_days", F.datediff(_ref_date, F.col("_last")))\n'
                    '    .withColumn("m_quartile", F.ntile(4).over(Window.orderBy("monetary")))\n'
                    '    .select("customer_id", "recency_days", "frequency", "monetary", "m_quartile")\n'
                    ')\n'
                    'rfm.show(5)'
                ),
                check=(
                    'assert {"customer_id", "recency_days", "frequency", "monetary", "m_quartile"} <= set(rfm.columns)\n'
                    'assert rfm.select("m_quartile").distinct().count() == 4\n'
                    'assert rfm.filter(F.col("recency_days") < 0).count() == 0\n'
                    'print("✅ Task 6 passed")'
                ),
            ),
        ],
    )
