"""Static reference data and tunable parameters for the FMCG generator.

Everything here is deterministic: curated value lists, the data model's sizing
ratios, and the knobs that control how much dirty data gets injected. The actual
row generation lives in :mod:`fmcg.generator`.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# --- Curated FMCG reference values -----------------------------------------

# category -> list of (subcategory, [brands]) tuples
CATEGORIES: dict[str, list[tuple[str, list[str]]]] = {
    "Beverages": [
        ("Carbonated Drinks", ["FizzCo", "PopMax", "Sparkle"]),
        ("Juices", ["PureSip", "OrchardGold", "Sparkle"]),
        ("Water", ["AquaPure", "ClearSpring"]),
        ("Coffee & Tea", ["MorningRoast", "LeafLux"]),
    ],
    "Snacks": [
        ("Chips", ["CrunchTime", "GoldenChip"]),
        ("Biscuits", ["SnapCookie", "GoldenChip"]),
        ("Chocolate", ["CocoaJoy", "SweetPeak"]),
        ("Nuts", ["NuttyByte"]),
    ],
    "Dairy": [
        ("Milk", ["DairyBest", "FarmFresh"]),
        ("Yogurt", ["DairyBest", "CremaViva"]),
        ("Cheese", ["FromageRoyal", "FarmFresh"]),
    ],
    "Household": [
        ("Cleaning", ["ShinePro", "SparkleHome"]),
        ("Laundry", ["FreshWash", "ShinePro"]),
        ("Paper Goods", ["SoftPly", "SparkleHome"]),
    ],
    "Personal Care": [
        ("Hair Care", ["SilkStrand", "PureGlow"]),
        ("Oral Care", ["BrightSmile", "PureGlow"]),
        ("Skin Care", ["DermaSoft", "PureGlow"]),
    ],
    "Frozen": [
        ("Frozen Meals", ["QuickHeat", "ChefFrost"]),
        ("Ice Cream", ["FrostyScoop", "CremaViva"]),
    ],
}

REGIONS: list[str] = ["North", "South", "East", "West", "Central"]

CITIES: dict[str, list[str]] = {
    "North": ["Aberdeen", "Newcastle", "Leeds"],
    "South": ["Brighton", "Southampton", "Exeter"],
    "East": ["Norwich", "Cambridge", "Ipswich"],
    "West": ["Bristol", "Cardiff", "Plymouth"],
    "Central": ["Birmingham", "Coventry", "Leicester"],
}

CHANNELS: list[str] = ["hypermarket", "supermarket", "convenience", "online"]

LOYALTY_TIERS: list[str] = ["none", "bronze", "silver", "gold", "platinum"]

SUPPLIER_COUNTRIES: list[str] = [
    "United Kingdom",
    "Germany",
    "France",
    "Netherlands",
    "Poland",
    "Ireland",
]

PROMO_TYPES: list[str] = [
    "price_discount",
    "bogo",
    "multibuy",
    "loyalty_bonus",
    "seasonal",
]

FIRST_NAMES: list[str] = [
    "Olivia", "Liam", "Emma", "Noah", "Ava", "James", "Sophia", "William",
    "Isabella", "Henry", "Mia", "Jack", "Amelia", "Oliver", "Harper",
    "George", "Ella", "Charlie", "Grace", "Leo",
]
LAST_NAMES: list[str] = [
    "Smith", "Jones", "Taylor", "Brown", "Williams", "Wilson", "Johnson",
    "Davies", "Patel", "Robinson", "Wright", "Thompson", "Evans", "Walker",
    "White", "Roberts", "Green", "Hall", "Wood", "Clarke",
]


# --- Sizing & dirtiness parameters -----------------------------------------


@dataclass(frozen=True)
class GenConfig:
    """Controls dataset sizes and the rate of injected data-quality issues.

    ``scale`` multiplies the base sales/inventory volume. scale=1.0 yields
    roughly ~500k sales rows; bump it up for performance exercises.
    """

    scale: float = 1.0
    seed: int = 42

    # Dimension sizes (scaled mildly with ``scale``).
    n_suppliers: int = 25
    n_products: int = 600
    n_stores: int = 120
    n_customers: int = 8_000
    n_promotions: int = 40

    # Fact sizing.
    base_sales_rows: int = 500_000
    days: int = 180  # trailing window of daily activity
    inventory_snapshot_days: int = 12  # snapshots taken every N days

    # Dirty-data injection rates (fraction of affected rows). Set the whole
    # block to 0 by passing ``clean=True`` to the generator.
    null_customer_rate: float = 0.18
    null_promo_rate: float = 0.75  # most sales have no promo
    duplicate_rate: float = 0.01
    bad_casing_rate: float = 0.12
    negative_qty_rate: float = 0.008
    out_of_range_discount_rate: float = 0.01
    orphan_fk_rate: float = 0.006

    # One store gets a disproportionate share of sales (skew exercise).
    skew_store_share: float = 0.18

    def scaled(self) -> "GenConfig":
        """Return a copy with fact volumes multiplied by ``scale``."""
        s = self.scale
        return GenConfig(
            scale=s,
            seed=self.seed,
            n_suppliers=self.n_suppliers,
            n_products=int(self.n_products * max(1.0, s ** 0.5)),
            n_stores=self.n_stores,
            n_customers=int(self.n_customers * max(1.0, s ** 0.5)),
            n_promotions=self.n_promotions,
            base_sales_rows=int(self.base_sales_rows * s),
            days=self.days,
            inventory_snapshot_days=self.inventory_snapshot_days,
        )


# Output dataset names (used for paths and the README table).
CLEAN_PARQUET_DATASETS = [
    "dim_products",
    "dim_stores",
    "dim_customers",
    "dim_suppliers",
    "dim_promotions",
    "fact_inventory",
]
# fact_sales is written as CSV (the deliberately-dirty raw source).
RAW_CSV_DATASETS = ["fact_sales"]
