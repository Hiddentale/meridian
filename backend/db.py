import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from product_transform import CLEAN_15_ITEMS

DB_DEFAULT = Path(__file__).parent / "db" / "catalogue.db"
MIGRATIONS_DIR = Path(__file__).parent / "migrations"


def _apply_migration(conn: sqlite3.Connection, mf: Path) -> None:
    """Apply a single migration file within an explicit transaction."""
    sql = mf.read_text(encoding="utf-8")
    # Strip comment-only lines before splitting on ";" to avoid false statement breaks.
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    statements = [s.strip() for s in "\n".join(lines).split(";") if s.strip()]
    timestamp = datetime.now(timezone.utc).isoformat()
    with conn:
        for stmt in statements:
            conn.execute(stmt)
        conn.execute(
            "INSERT INTO schema_migrations (filename, applied_at) VALUES (?, ?)",
            (mf.name, timestamp),
        )


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending migration files from the migrations directory in order.

    On an existing database that pre-dates migration tracking, all migration files
    are marked as applied without being executed (bootstrap). This prevents
    re-applying DDL that the DB already reflects.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filename    TEXT NOT NULL UNIQUE,
            applied_at  TEXT NOT NULL
        )
    """)
    conn.commit()

    migration_files = sorted(MIGRATIONS_DIR.glob("[0-9]*.sql"))
    applied = {r[0] for r in conn.execute("SELECT filename FROM schema_migrations")}

    if not applied:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name != 'schema_migrations'"
            )
        }
        if "products" in tables:
            timestamp = datetime.now(timezone.utc).isoformat()
            conn.executemany(
                "INSERT OR IGNORE INTO schema_migrations (filename, applied_at) VALUES (?, ?)",
                [(f.name, timestamp) for f in migration_files],
            )
            conn.commit()
            print(
                f"  Migrations bootstrapped: {len(migration_files)} file(s) marked as applied"
            )
            return

    pending = [f for f in migration_files if f.name not in applied]
    for mf in pending:
        print(f"  Applying migration: {mf.name}")
        _apply_migration(conn, mf)


def _seed_clean_15(conn: sqlite3.Connection) -> None:
    """Populate the clean_15 reference table (idempotent via INSERT OR IGNORE)."""
    conn.executemany(
        "INSERT OR IGNORE INTO clean_15 (name, name_en) VALUES (?, ?)",
        CLEAN_15_ITEMS,
    )
    conn.commit()


def init_db(db_path: Path) -> sqlite3.Connection:
    """Create or migrate the database, then seed reference data."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _run_migrations(conn)
    _seed_clean_15(conn)
    return conn


def upsert_products(conn: sqlite3.Connection, products: list[dict]) -> int:
    """Upsert transformed products into the database. Returns count inserted/updated."""
    sql = """
    INSERT INTO products (
        sku, title, brand, ean, root_category, pack_size, description, storage,
        ingredients, allergens_contains, allergens_may_contain,
        nutri_score, is_bio, has_nightshade, is_clean_15, is_available, in_assortment,
        price_cents, promo_price_cents, price_per_unit_cents, price_per_unit_unit,
        energy_kcal, protein_g, carbs_g, fat_g, saturated_fat_g, sugar_g, salt_g,
        nutritions_raw, categories, badges, is_medicine, retail_set, image_url, last_updated
    ) VALUES (
        :sku, :title, :brand, :ean, :root_category, :pack_size, :description, :storage,
        :ingredients, :allergens_contains, :allergens_may_contain,
        :nutri_score, :is_bio, :has_nightshade, :is_clean_15, :is_available, :in_assortment,
        :price_cents, :promo_price_cents, :price_per_unit_cents, :price_per_unit_unit,
        :energy_kcal, :protein_g, :carbs_g, :fat_g, :saturated_fat_g, :sugar_g, :salt_g,
        :nutritions_raw, :categories, :badges, :is_medicine, :retail_set, :image_url, :last_updated
    )
    ON CONFLICT(sku) DO UPDATE SET
        title=excluded.title, brand=excluded.brand, ean=excluded.ean,
        root_category=excluded.root_category, pack_size=excluded.pack_size,
        description=excluded.description, storage=excluded.storage,
        ingredients=excluded.ingredients,
        allergens_contains=excluded.allergens_contains,
        allergens_may_contain=excluded.allergens_may_contain,
        nutri_score=excluded.nutri_score, is_bio=excluded.is_bio,
        has_nightshade=excluded.has_nightshade, is_clean_15=excluded.is_clean_15,
        is_available=excluded.is_available,
        in_assortment=excluded.in_assortment, price_cents=excluded.price_cents,
        promo_price_cents=excluded.promo_price_cents,
        price_per_unit_cents=excluded.price_per_unit_cents,
        price_per_unit_unit=excluded.price_per_unit_unit,
        energy_kcal=excluded.energy_kcal, protein_g=excluded.protein_g,
        carbs_g=excluded.carbs_g, fat_g=excluded.fat_g,
        saturated_fat_g=excluded.saturated_fat_g, sugar_g=excluded.sugar_g,
        salt_g=excluded.salt_g, nutritions_raw=excluded.nutritions_raw,
        categories=excluded.categories, badges=excluded.badges,
        is_medicine=excluded.is_medicine, retail_set=excluded.retail_set,
        image_url=excluded.image_url, last_updated=excluded.last_updated
    """

    with conn:
        conn.executemany(sql, products)

    return len(products)
