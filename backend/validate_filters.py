"""
Milestone 2 validation: Allergen and Bio Filtering Layer.

Verifies that filtered_products and meal_safe_products satisfy all safety
and bio/Clean 15 criteria. Exits with code 0 on full pass, 1 on any failure.

Usage:
    python validate_filters.py [--db PATH]
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

from db import DB_DEFAULT
from product_transform import detect_nightshade

MILK_ALLERGEN_TERMS = {"melk", "lactose", "caseïne", "wei"}
PRODUCE_CATEGORY = "Aardappelen, groente en fruit"


def _check(label: str, passed: bool, detail: str, failures: list[str]) -> None:
    status = "PASS" if passed else "FAIL"
    print(f"  {status}  {label}")
    if not passed:
        for line in detail.splitlines():
            print(f"        {line}")
        failures.append(label)


def validate(db_path: Path) -> bool:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    failures: list[str] = []
    print("=== Milestone 2 Filter Validation ===\n")

    # ------------------------------------------------------------------ #
    # filtered_products, safety checks                                    #
    # ------------------------------------------------------------------ #

    n = conn.execute(
        "SELECT COUNT(*) FROM filtered_products WHERE has_nightshade = 1"
    ).fetchone()[0]
    _check(
        "filtered_products: no nightshade-flagged rows",
        n == 0,
        f"{n} nightshade-flagged products found",
        failures,
    )

    n = conn.execute("""
        SELECT COUNT(*) FROM filtered_products
        WHERE EXISTS (
            SELECT 1 FROM json_each(allergens_contains)
            WHERE lower(value) IN ('melk', 'lactose', 'caseïne', 'wei')
        )
    """).fetchone()[0]
    _check(
        "filtered_products: no milk-allergen rows",
        n == 0,
        f"{n} milk-allergen products found",
        failures,
    )

    n = conn.execute("""
        SELECT COUNT(*) FROM filtered_products
        WHERE json_array_length(ingredients) = 0
    """).fetchone()[0]
    _check(
        "filtered_products: no empty-ingredient rows",
        n == 0,
        f"{n} products with empty ingredient data",
        failures,
    )

    n = conn.execute("""
        SELECT COUNT(*) FROM filtered_products
        WHERE is_available = 0 OR in_assortment = 0
    """).fetchone()[0]
    _check(
        "filtered_products: all rows available",
        n == 0,
        f"{n} unavailable products",
        failures,
    )

    # Deep scan: run the Python nightshade detector on every row's ingredient text.
    # Catches any case where has_nightshade was set to 0 but ingredients contain a
    # nightshade term (e.g. if NIGHTSHADE_TERMS was updated without re-ingesting).
    rows = conn.execute(
        "SELECT sku, title, ingredients FROM filtered_products"
    ).fetchall()
    escapes = [
        f"{r['sku']}: {r['title'][:60]}"
        for r in rows
        if detect_nightshade(json.loads(r["ingredients"]))
    ]
    _check(
        "filtered_products: deep nightshade scan",
        len(escapes) == 0,
        f"{len(escapes)} product(s) with nightshade ingredients escaped the flag:\n"
        + "\n".join(escapes[:10])
        + ("\n  ..." if len(escapes) > 10 else ""),
        failures,
    )

    # ------------------------------------------------------------------ #
    # meal_safe_products, bio/Clean 15 checks                            #
    # ------------------------------------------------------------------ #

    # Clean 15 conventional produce must appear in meal_safe_products
    n_c15 = conn.execute(
        """
        SELECT COUNT(*) FROM meal_safe_products
        WHERE root_category = ? AND is_bio = 0 AND is_clean_15 = 1
    """,
        (PRODUCE_CATEGORY,),
    ).fetchone()[0]
    _check(
        "meal_safe_products: Clean 15 conventional produce present",
        n_c15 > 0,
        "No Clean 15 conventional produce found",
        failures,
    )
    if n_c15 > 0:
        print(f"        ({n_c15} Clean 15 conventional produce products included)")

    # Non-bio, non-Clean15 produce must NOT appear in meal_safe_products
    n_conv = conn.execute(
        """
        SELECT COUNT(*) FROM meal_safe_products
        WHERE root_category = ? AND is_bio = 0 AND is_clean_15 = 0
    """,
        (PRODUCE_CATEGORY,),
    ).fetchone()[0]
    _check(
        "meal_safe_products: non-Clean15 conventional produce excluded",
        n_conv == 0,
        f"{n_conv} conventional non-Clean15 produce products incorrectly included",
        failures,
    )

    n_excluded = conn.execute(
        """
        SELECT COUNT(*) FROM filtered_products
        WHERE root_category = ? AND is_bio = 0 AND is_clean_15 = 0
    """,
        (PRODUCE_CATEGORY,),
    ).fetchone()[0]
    if n_excluded > 0:
        print(
            f"        ({n_excluded} conventional non-Clean15 produce products correctly excluded)"
        )

    # ------------------------------------------------------------------ #
    # Summary                                                             #
    # ------------------------------------------------------------------ #
    total_fp = conn.execute("SELECT COUNT(*) FROM filtered_products").fetchone()[0]
    total_ms = conn.execute("SELECT COUNT(*) FROM meal_safe_products").fetchone()[0]

    produce_fp = conn.execute(
        "SELECT COUNT(*) FROM filtered_products WHERE root_category = ?",
        (PRODUCE_CATEGORY,),
    ).fetchone()[0]
    produce_ms = conn.execute(
        "SELECT COUNT(*) FROM meal_safe_products WHERE root_category = ?",
        (PRODUCE_CATEGORY,),
    ).fetchone()[0]

    print("\n=== Product Counts ===")
    print(f"  filtered_products (safety):          {total_fp:>6}")
    print(f"  meal_safe_products (bio + Clean 15): {total_ms:>6}")
    print(f"  Produce in filtered_products:        {produce_fp:>6}")
    print(f"  Produce in meal_safe_products:       {produce_ms:>6}")

    print()
    if failures:
        print(f"FAIL, {len(failures)} check(s) failed")
        return False

    print("PASS, all checks passed")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate Milestone 2 filters")
    parser.add_argument("--db", type=Path, default=DB_DEFAULT)
    args = parser.parse_args()

    ok = validate(args.db)
    sys.exit(0 if ok else 1)
