"""Notebook 00 — environment & data sanity check (no graded tasks)."""

from __future__ import annotations

from .common import Notebook, Task, setup_cell

INTRO = """Welcome to the **PySpark FMCG training kit**.

This first notebook just confirms your environment works and the datasets are
present. If every cell runs, you're ready for notebooks 01–04.

**Before running:** make sure you've generated the data:

```bash
uv run python scripts/bootstrap_java.py   # one-time: downloads JDK 17
uv run python generate_data.py            # writes datasets into ./data
```
"""

OVERVIEW = '''datasets = {
    "dim_products": read_dim(spark, "dim_products"),
    "dim_stores": read_dim(spark, "dim_stores"),
    "dim_customers": read_dim(spark, "dim_customers"),
    "dim_suppliers": read_dim(spark, "dim_suppliers"),
    "dim_promotions": read_dim(spark, "dim_promotions"),
    "fact_inventory": read_dim(spark, "fact_inventory"),
    "fact_sales (raw csv)": read_sales_raw(spark),
}
for name, df in datasets.items():
    print(f"{name:24s} {df.count():>10,} rows")
'''

PEEK = '''# The raw sales CSV — every column is a string and the data is deliberately messy.
sales_raw = read_sales_raw(spark)
sales_raw.printSchema()
sales_raw.show(5, truncate=False)

print("A clean reference dimension (Parquet, typed):")
read_dim(spark, "dim_products").printSchema()
read_dim(spark, "dim_products").show(5, truncate=False)
'''


def get() -> Notebook:
    return Notebook(
        slug="00_environment_check",
        title="00 · Environment & Data Check",
        intro=INTRO,
        setup=setup_cell("nb00"),
        tasks=[
            Task(
                title="Dataset overview",
                prompt="Row counts for every generated dataset.",
                solution=OVERVIEW,
                given=True,
            ),
            Task(
                title="Peek at the data",
                prompt="Inspect the raw (dirty) sales CSV and a clean dimension.",
                solution=PEEK,
                given=True,
            ),
        ],
    )
