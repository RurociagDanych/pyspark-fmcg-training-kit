"""Notebook 03 — data quality & cleaning."""

from __future__ import annotations

from .common import Notebook, Task, setup_cell

INTRO = """The raw `fact_sales` CSV is deliberately messy: nulls, duplicate
transactions, non-numeric quantities, negative quantities, out-of-range
discounts, and orphan product foreign keys. The dimension text (category,
region) also has inconsistent casing/whitespace.

Your job: audit and clean it into a trustworthy dataset."""

SETUP_EXTRA = '''sales_raw = read_sales_raw(spark)
products = read_dim(spark, "dim_products")
stores = read_dim(spark, "dim_stores")
print("raw sales rows:", sales_raw.count())
sales_raw.show(5, truncate=False)'''


def get() -> Notebook:
    return Notebook(
        slug="03_data_quality",
        title="03 · Data Quality & Cleaning",
        intro=INTRO,
        setup=setup_cell("nb03", SETUP_EXTRA),
        tasks=[
            Task(
                title="Audit the raw data",
                prompt=(
                    "Compute three integers on `sales_raw`:\n"
                    "- `n_null_customer`: rows where `customer_id` is null\n"
                    "- `n_bad_qty`: rows where `quantity` is **not** a valid "
                    "integer (hint: `try_cast(... as int)` is null)\n"
                    "- `n_dupes`: duplicate transaction rows "
                    "(total rows − distinct `txn_id`)"
                ),
                stub='# TODO compute n_null_customer, n_bad_qty, n_dupes\nn_null_customer = 0\nn_bad_qty = 0\nn_dupes = 0\nprint(n_null_customer, n_bad_qty, n_dupes)',
                solution=(
                    'n_null_customer = sales_raw.filter(F.col("customer_id").isNull()).count()\n'
                    'n_bad_qty = sales_raw.filter(F.expr("try_cast(quantity as int)").isNull()).count()\n'
                    'n_dupes = sales_raw.count() - sales_raw.select("txn_id").distinct().count()\n'
                    'print(n_null_customer, n_bad_qty, n_dupes)'
                ),
                check=(
                    'assert n_null_customer == sales_raw.filter(F.col("customer_id").isNull()).count()\n'
                    'assert n_bad_qty > 0, "expected some non-numeric quantities"\n'
                    'assert n_dupes > 0, "expected duplicate transactions"\n'
                    'print("✅ Task 1 passed")'
                ),
            ),
            Task(
                title="Deduplicate transactions",
                prompt=(
                    "Produce `deduped` so each `txn_id` appears exactly once."
                ),
                stub='# TODO\ndeduped = sales_raw',
                solution='deduped = sales_raw.dropDuplicates(["txn_id"])\nprint("rows:", deduped.count())',
                check=(
                    'assert deduped.count() == deduped.select("txn_id").distinct().count()\n'
                    'assert deduped.count() == sales_raw.select("txn_id").distinct().count()\n'
                    'print("✅ Task 2 passed — deduped rows:", deduped.count())'
                ),
            ),
            Task(
                title="Safely impose types",
                prompt=(
                    "From `deduped`, build `typed` with `quantity`→int, "
                    "`unit_price`→double, `discount`→double, `txn_date`→date, "
                    "using **safe** casts (bad quantities become null)."
                ),
                stub='# TODO\ntyped = deduped',
                solution=(
                    'typed = (\n'
                    '    deduped\n'
                    '    .withColumn("txn_date", F.to_date("txn_date"))\n'
                    '    .withColumn("quantity", F.expr("try_cast(quantity as int)"))\n'
                    '    .withColumn("unit_price", F.expr("try_cast(unit_price as double)"))\n'
                    '    .withColumn("discount", F.expr("try_cast(discount as double)"))\n'
                    ')\n'
                    'typed.printSchema()'
                ),
                check=(
                    '_t = dict(typed.dtypes)\n'
                    'assert _t["quantity"] == "int" and _t["discount"] == "double" and _t["txn_date"] == "date"\n'
                    'assert typed.filter(F.col("quantity").isNull()).count() > 0\n'
                    'print("✅ Task 3 passed")'
                ),
            ),
            Task(
                title="Normalize categorical text",
                prompt=(
                    "`products.category` has inconsistent casing/whitespace "
                    "(`'  beverages '`, `'BEVERAGES'`, …). Build `products_clean` "
                    "with a normalized `category` (trim + title-case) so each real "
                    "category collapses to a single value."
                ),
                stub='# TODO\nproducts_clean = products',
                solution=(
                    'products_clean = products.withColumn("category", F.initcap(F.trim(F.col("category"))))\n'
                    '_before = products.select("category").distinct().count()\n'
                    '_after = products_clean.select("category").distinct().count()\n'
                    'print("distinct categories before:", _before, "after:", _after)'
                ),
                check=(
                    'assert products_clean.select("category").distinct().count() < products.select("category").distinct().count()\n'
                    'assert products_clean.select("category").distinct().count() == 6\n'
                    'print("✅ Task 4 passed")'
                ),
            ),
            Task(
                title="Find orphan foreign keys",
                prompt=(
                    "Find sales whose `product_id` has no match in `products` "
                    "(orphan FKs). Produce `orphans` (distinct `product_id`) and "
                    "the integer `n_orphan_products`. Use a `left_anti` join from "
                    "`typed`."
                ),
                stub='# TODO\norphans = typed\nn_orphan_products = 0',
                solution=(
                    'orphans = typed.join(products, "product_id", "left_anti").select("product_id").distinct()\n'
                    'n_orphan_products = orphans.count()\n'
                    'print("orphan product_ids:", n_orphan_products)\n'
                    'orphans.show(5)'
                ),
                check=(
                    'assert n_orphan_products > 0\n'
                    'assert orphans.join(products, "product_id", "inner").count() == 0\n'
                    'print("✅ Task 5 passed — orphans:", n_orphan_products)'
                ),
            ),
            Task(
                title="Assemble the clean dataset",
                prompt=(
                    "Build the final `clean_sales`: from `typed`, drop rows with "
                    "non-positive/null quantity and discounts outside [0, 1], "
                    "inner-join to `products_clean` (dropping orphans and attaching "
                    "`category`, `brand`), and add a `revenue` column."
                ),
                stub='# TODO\nclean_sales = typed',
                solution=(
                    'clean_sales = (\n'
                    '    typed\n'
                    '    .filter((F.col("quantity") > 0) & F.col("discount").between(0, 1))\n'
                    '    .join(products_clean.select("product_id", "category", "brand"), "product_id")\n'
                    '    .withColumn(\n'
                    '        "revenue",\n'
                    '        F.col("quantity") * F.col("unit_price") * (1 - F.col("discount")),\n'
                    '    )\n'
                    ')\n'
                    'clean_sales.select("txn_id", "category", "quantity", "discount", "revenue").show(5)'
                ),
                check=(
                    'assert clean_sales.filter(~F.col("discount").between(0, 1)).count() == 0\n'
                    'assert clean_sales.filter(F.col("quantity") <= 0).count() == 0\n'
                    'assert clean_sales.join(products, "product_id", "left_anti").count() == 0\n'
                    'assert {"revenue", "category"} <= set(clean_sales.columns)\n'
                    'print("✅ Task 6 passed — clean rows:", clean_sales.count())'
                ),
            ),
        ],
    )
