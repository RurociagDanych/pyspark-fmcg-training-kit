"""Generate the FMCG datasets.

Dimensions are small and built in pure Python (seeded) for full control over the
injected casing/whitespace issues, then turned into Spark DataFrames. The large
``fact_sales`` table is generated with Spark column expressions so it scales, and
is written as a deliberately-dirty CSV. ``fact_inventory`` is a clean Parquet
fact built from a store x product x snapshot-date grid.

Public entry point: :func:`generate`.
"""

from __future__ import annotations

import datetime as dt
import random
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from . import catalog
from .catalog import GenConfig
from .dirty import maybe, messy_casing

TODAY = dt.date(2026, 6, 5)


# --------------------------------------------------------------------------
# Dimension builders (pure Python -> Spark DataFrame)
# --------------------------------------------------------------------------


def _suppliers(spark: SparkSession, cfg: GenConfig, rng: random.Random) -> DataFrame:
    rows = []
    for i in range(1, cfg.n_suppliers + 1):
        rows.append(
            (
                f"SUP{i:03d}",
                f"Supplier {i:03d}",
                rng.choice(catalog.SUPPLIER_COUNTRIES),
                rng.randint(2, 21),
            )
        )
    schema = T.StructType(
        [
            T.StructField("supplier_id", T.StringType()),
            T.StructField("supplier_name", T.StringType()),
            T.StructField("country", T.StringType()),
            T.StructField("lead_time_days", T.IntegerType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _products(spark: SparkSession, cfg: GenConfig, rng: random.Random) -> DataFrame:
    # Flatten the catalog into (category, subcategory, brand) options.
    combos = [
        (cat, sub, brand)
        for cat, subs in catalog.CATEGORIES.items()
        for sub, brands in subs
        for brand in brands
    ]
    rows = []
    for i in range(1, cfg.n_products + 1):
        category, subcategory, brand = rng.choice(combos)
        unit_cost = round(rng.uniform(0.5, 8.0), 2)
        pack_size = rng.choice([1, 2, 4, 6, 250, 330, 500, 1000])
        rows.append(
            (
                f"P{i:04d}",
                f"{brand} {subcategory} {pack_size}",
                # Casing/whitespace dirtiness injected into category here.
                messy_casing(category, rng, cfg.bad_casing_rate),
                subcategory,
                brand,
                pack_size,
                unit_cost,
                f"SUP{rng.randint(1, cfg.n_suppliers):03d}",
            )
        )
    schema = T.StructType(
        [
            T.StructField("product_id", T.StringType()),
            T.StructField("product_name", T.StringType()),
            T.StructField("category", T.StringType()),
            T.StructField("subcategory", T.StringType()),
            T.StructField("brand", T.StringType()),
            T.StructField("pack_size", T.IntegerType()),
            T.StructField("unit_cost", T.DoubleType()),
            T.StructField("supplier_id", T.StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _stores(spark: SparkSession, cfg: GenConfig, rng: random.Random) -> DataFrame:
    rows = []
    for i in range(1, cfg.n_stores + 1):
        region = rng.choice(catalog.REGIONS)
        city = rng.choice(catalog.CITIES[region])
        channel = rng.choice(catalog.CHANNELS)
        open_date = TODAY - dt.timedelta(days=rng.randint(200, 3650))
        size = 0 if channel == "online" else rng.randint(120, 9000)
        rows.append(
            (
                f"ST{i:03d}",
                f"{city} {channel.title()} {i:03d}",
                # Casing/whitespace dirtiness injected into region here.
                messy_casing(region, rng, cfg.bad_casing_rate),
                city,
                channel,
                size,
                open_date,
            )
        )
    schema = T.StructType(
        [
            T.StructField("store_id", T.StringType()),
            T.StructField("store_name", T.StringType()),
            T.StructField("region", T.StringType()),
            T.StructField("city", T.StringType()),
            T.StructField("channel", T.StringType()),
            T.StructField("size_sqm", T.IntegerType()),
            T.StructField("open_date", T.DateType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _customers(spark: SparkSession, cfg: GenConfig, rng: random.Random) -> DataFrame:
    # Loyalty tiers are skewed toward lower tiers.
    tier_weights = [0.45, 0.25, 0.15, 0.10, 0.05]
    rows = []
    for i in range(1, cfg.n_customers + 1):
        name = f"{rng.choice(catalog.FIRST_NAMES)} {rng.choice(catalog.LAST_NAMES)}"
        tier = rng.choices(catalog.LOYALTY_TIERS, weights=tier_weights)[0]
        signup = TODAY - dt.timedelta(days=rng.randint(0, 2000))
        rows.append(
            (
                f"C{i:05d}",
                name,
                tier,
                signup,
                rng.choice(catalog.REGIONS),
            )
        )
    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType()),
            T.StructField("customer_name", T.StringType()),
            T.StructField("loyalty_tier", T.StringType()),
            T.StructField("signup_date", T.DateType()),
            T.StructField("region", T.StringType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


def _promotions(spark: SparkSession, cfg: GenConfig, rng: random.Random) -> DataFrame:
    rows = []
    for i in range(1, cfg.n_promotions + 1):
        start = TODAY - dt.timedelta(days=rng.randint(0, cfg.days))
        end = start + dt.timedelta(days=rng.randint(3, 30))
        rows.append(
            (
                f"PR{i:03d}",
                rng.choice(catalog.PROMO_TYPES),
                round(rng.uniform(0.05, 0.4), 2),
                start,
                end,
            )
        )
    schema = T.StructType(
        [
            T.StructField("promo_id", T.StringType()),
            T.StructField("promo_type", T.StringType()),
            T.StructField("discount_pct", T.DoubleType()),
            T.StructField("start_date", T.DateType()),
            T.StructField("end_date", T.DateType()),
        ]
    )
    return spark.createDataFrame(rows, schema)


# --------------------------------------------------------------------------
# Fact builders (Spark expressions)
# --------------------------------------------------------------------------


def _fact_sales(spark: SparkSession, cfg: GenConfig) -> DataFrame:
    """Large, deliberately-dirty sales fact built with column expressions."""
    seed = cfg.seed
    base_date = TODAY - dt.timedelta(days=cfg.days)

    def rnd(offset: int):
        return F.rand(seed + offset)

    df = spark.range(0, cfg.base_sales_rows).withColumnRenamed("id", "n")

    # Store: one mega-store (ST001) soaks up `skew_store_share` of all sales.
    store_idx = F.when(
        rnd(1) < cfg.skew_store_share, F.lit(0)
    ).otherwise(F.floor(rnd(2) * cfg.n_stores))
    store_id = F.concat(F.lit("ST"), F.lpad((store_idx + 1).cast("string"), 3, "0"))

    # Product: a small fraction reference non-existent products (orphan FKs).
    product_idx = F.when(
        rnd(3) < cfg.orphan_fk_rate,
        F.lit(cfg.n_products) + F.floor(rnd(4) * 50),
    ).otherwise(F.floor(rnd(4) * cfg.n_products))
    product_id = F.concat(F.lit("P"), F.lpad((product_idx + 1).cast("string"), 4, "0"))

    # Customer: many sales are anonymous -> null customer_id.
    customer_id = F.when(rnd(5) < cfg.null_customer_rate, F.lit(None)).otherwise(
        F.concat(
            F.lit("C"),
            F.lpad((F.floor(rnd(6) * cfg.n_customers) + 1).cast("string"), 5, "0"),
        )
    )

    # Quantity: integer, occasionally negative, occasionally non-numeric.
    # Written as a string so the cleaning notebook must cast it safely.
    qty_int = (F.floor(rnd(7) * 9) + 1).cast("int")
    qty_signed = F.when(rnd(8) < cfg.negative_qty_rate, -qty_int).otherwise(qty_int)
    qty = F.when(rnd(9) < 0.003, F.lit("N/A")).otherwise(qty_signed.cast("string"))

    # Unit price: stable per product (derived from product index).
    unit_price = F.round(F.lit(0.8) + (F.pmod(product_idx, F.lit(50)) * 0.2), 2)

    # Discount: mostly 0..0.4; a small fraction out of the valid [0,1] range.
    discount = F.when(
        rnd(10) < cfg.out_of_range_discount_rate,
        F.when(rnd(11) < 0.5, F.lit(1.5)).otherwise(F.lit(-0.2)),
    ).otherwise(F.round(rnd(12) * 0.4, 2))

    promo_id = F.when(rnd(13) < cfg.null_promo_rate, F.lit(None)).otherwise(
        F.concat(
            F.lit("PR"),
            F.lpad((F.floor(rnd(14) * cfg.n_promotions) + 1).cast("string"), 3, "0"),
        )
    )

    sale_date = F.date_add(F.lit(base_date), F.floor(rnd(15) * cfg.days).cast("int"))

    sales = df.select(
        F.concat(F.lit("T"), F.lpad(F.col("n").cast("string"), 9, "0")).alias("txn_id"),
        sale_date.alias("txn_date"),
        store_id.alias("store_id"),
        product_id.alias("product_id"),
        customer_id.alias("customer_id"),
        qty.alias("quantity"),
        unit_price.alias("unit_price"),
        discount.alias("discount"),
        promo_id.alias("promo_id"),
    )

    # Duplicate transactions: re-append a sample of rows verbatim.
    if cfg.duplicate_rate > 0:
        dupes = sales.sample(withReplacement=False, fraction=cfg.duplicate_rate, seed=seed)
        sales = sales.unionByName(dupes)

    return sales


def _fact_inventory(
    spark: SparkSession, cfg: GenConfig, stores: DataFrame, products: DataFrame
) -> DataFrame:
    """Clean inventory snapshots over a store x product grid."""
    seed = cfg.seed
    n_snaps = max(1, cfg.days // cfg.inventory_snapshot_days)
    base_date = TODAY - dt.timedelta(days=cfg.days)

    snap_offsets = spark.range(0, n_snaps).select(
        (F.col("id") * cfg.inventory_snapshot_days).cast("int").alias("offset")
    )
    snap_dates = snap_offsets.select(
        F.date_add(F.lit(base_date), F.col("offset")).alias("snapshot_date")
    )

    grid = (
        stores.select("store_id")
        .crossJoin(products.select("product_id"))
        .crossJoin(snap_dates)
    )
    return grid.select(
        "snapshot_date",
        "store_id",
        "product_id",
        F.floor(F.rand(seed + 20) * 500).cast("int").alias("stock_on_hand"),
        (F.floor(F.rand(seed + 21) * 80) + 20).cast("int").alias("reorder_point"),
    )


# --------------------------------------------------------------------------
# Orchestration
# --------------------------------------------------------------------------


def generate(spark: SparkSession, out_dir: str | Path, cfg: GenConfig | None = None,
             clean: bool = False) -> dict[str, int]:
    """Generate all datasets and write them under ``out_dir``.

    Args:
        spark: active SparkSession.
        out_dir: directory to write datasets into (created if missing).
        cfg: generation config; defaults to ``GenConfig()`` scaled by its scale.
        clean: if True, skip dirty-data injection (for sanity comparisons).

    Returns:
        Mapping of dataset name -> row count.
    """
    cfg = (cfg or GenConfig()).scaled()
    if clean:
        cfg = GenConfig(
            scale=cfg.scale, seed=cfg.seed, n_suppliers=cfg.n_suppliers,
            n_products=cfg.n_products, n_stores=cfg.n_stores,
            n_customers=cfg.n_customers, n_promotions=cfg.n_promotions,
            base_sales_rows=cfg.base_sales_rows, days=cfg.days,
            inventory_snapshot_days=cfg.inventory_snapshot_days,
            null_customer_rate=0.0, null_promo_rate=0.0, duplicate_rate=0.0,
            bad_casing_rate=0.0, negative_qty_rate=0.0,
            out_of_range_discount_rate=0.0, orphan_fk_rate=0.0,
        )

    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    rng = random.Random(cfg.seed)
    counts: dict[str, int] = {}

    # Build dimensions.
    dims = {
        "dim_suppliers": _suppliers(spark, cfg, rng),
        "dim_products": _products(spark, cfg, rng),
        "dim_stores": _stores(spark, cfg, rng),
        "dim_customers": _customers(spark, cfg, rng),
        "dim_promotions": _promotions(spark, cfg, rng),
    }
    products_df = dims["dim_products"]
    stores_df = dims["dim_stores"]

    # Inventory fact (clean parquet).
    inventory = _fact_inventory(spark, cfg, stores_df, products_df)

    # Write parquet datasets.
    for name, df in {**dims, "fact_inventory": inventory}.items():
        path = str(out / name)
        df.write.mode("overwrite").parquet(path)
        counts[name] = df.count()
        print(f"  wrote {name:16s} -> {counts[name]:>9,} rows (parquet)")

    # Sales fact (dirty CSV). Coalesce to a handful of files for tidy output.
    sales = _fact_sales(spark, cfg)
    sales_path = str(out / "fact_sales_csv")
    (
        sales.coalesce(4)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(sales_path)
    )
    counts["fact_sales"] = sales.count()
    print(f"  wrote {'fact_sales':16s} -> {counts['fact_sales']:>9,} rows (csv)")

    return counts
