# PySpark FMCG Training Kit

A self-contained PySpark training & assessment kit built around a fictional
**FMCG** (fast-moving consumer goods) retailer. It ships a configurable data
generator that produces realistic, deliberately-messy datasets, plus mid-level
Jupyter exercises with auto-grading checks and full solutions.

Designed for evaluating or upskilling a **Data Engineer** at the mid level:
DataFrame fundamentals, joins & window functions, data cleaning, and
performance/advanced topics.

## Quick start

```bash
# 1. Install Python deps (uv reads pyproject.toml / uv.lock)
uv sync

# 2. One-time: download a portable JDK 17 (PySpark needs Java; no sudo required)
uv run python scripts/bootstrap_java.py

# 3. Generate the datasets into ./data
uv run python generate_data.py

# 4. Launch Jupyter and open notebooks/00_environment_check.ipynb first
uv run jupyter lab
```

> **Java note:** PySpark 4.1 requires a Java 17+ runtime. `bootstrap_java.py`
> downloads Eclipse Temurin into `.jdk/` (git-ignored) and `utils/spark.py`
> wires `JAVA_HOME` automatically. If you already have a JDK 17+, just set
> `JAVA_HOME` and skip step 2.

## The exercises

Each topic is a notebook under `notebooks/`. Every task has a markdown prompt, a
`# TODO` cell to fill in, and a **CHECK** cell that asserts on your answer
(prints ✅ when correct). Fully-worked versions live in `notebooks/solutions/`.

| Notebook | Focus |
|----------|-------|
| `00_environment_check` | Confirm Spark + data are working (no tasks) |
| `01_core_dataframe_ops` | Safe casting, derived columns, filtering, groupBy/agg, monthly trend |
| `02_joins_and_windows` | Multi-table joins, broadcast joins, ranking, rolling sums, lag/WoW, RFM |
| `03_data_quality` | Auditing, dedup, safe casts, text normalization, orphan FKs, clean assembly |
| `04_performance_advanced` | Partitioning & pruning, broadcast tuning, skew, caching, UDF vs native, Spark SQL |

Work the exercise notebooks; check yourself against `solutions/` when stuck.

## The data

A small FMCG star schema, generated reproducibly (default seed `42`).

| Dataset | Format | Notes |
|---------|--------|-------|
| `dim_products` | Parquet | product, category, subcategory, brand, unit_cost, supplier |
| `dim_stores` | Parquet | region, city, channel, size, open_date |
| `dim_customers` | Parquet | loyalty tier, signup date, region |
| `dim_suppliers` | Parquet | country, lead time |
| `dim_promotions` | Parquet | promo type, discount %, date window |
| `fact_inventory` | Parquet | per-store/product stock snapshots |
| `fact_sales` | **CSV** | the large, deliberately-dirty transaction log |

### Deliberately injected data problems

The raw `fact_sales` CSV and some dimension text are messy on purpose, so the
data-quality exercises have real targets:

- Null `customer_id` (anonymous sales) and `promo_id` (no promotion)
- Duplicate transactions
- Non-numeric (`"N/A"`) and negative quantities
- Out-of-range discounts (outside `[0, 1]`)
- Orphan foreign keys (sales referencing non-existent products)
- Inconsistent casing/whitespace in `category` and `region`
- One **skewed mega-store** (`ST001`) holding ~18% of all sales

> Spark 4 runs in **ANSI mode**, so a plain `.cast("int")` on `"N/A"` *throws*.
> The exercises use `try_cast` to handle this — a realistic modern-Spark skill.

## Generator options

```bash
uv run python generate_data.py --scale 2.0    # ~1M sales rows (perf exercises)
uv run python generate_data.py --scale 0.1    # tiny & fast
uv run python generate_data.py --clean        # no injected data issues
uv run python generate_data.py --seed 7       # different reproducible dataset
uv run python generate_data.py --out data2    # custom output directory
```

## Project layout

```
src/fmcg/            Data generator (catalog, generator, dirty-data injection)
utils/spark.py       SparkSession builder + dataset loaders (handles JAVA_HOME)
scripts/
  bootstrap_java.py  Downloads portable Temurin JDK 17 into .jdk/
  build_notebooks.py Renders notebooks from scripts/exercises/*
  exercises/         Exercise definitions (one module per notebook)
generate_data.py     CLI entry point for data generation
notebooks/           Exercise notebooks (+ solutions/)
data/                Generated datasets (git-ignored)
docs/superpowers/    Design spec
```

## Regenerating the notebooks

The notebooks are generated from `scripts/exercises/` so prompts, solutions, and
checks stay in sync. After editing an exercise module:

```bash
uv run python scripts/build_notebooks.py
```

## For the interviewer / trainer

- **As an assessment:** hand over the repo without `notebooks/solutions/`. A
  candidate passes a task when its CHECK cell prints ✅. Running every notebook
  top-to-bottom with all greens is the objective bar.
- **As self-study:** keep the solutions; learners compare approaches.
- All solution notebooks are verified to run end-to-end against freshly
  generated data.
