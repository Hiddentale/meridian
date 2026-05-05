"""
Jumbo product catalogue ingestion CLI.

Fetches the full Jumbo product catalogue via the Jumbo website GraphQL API
and populates the SQLite products table.

Usage:
    python ingest_catalogue.py [--db PATH] [--limit N] [--dry-run]
"""

import argparse
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from catalogue_fetcher import DETAIL_BATCH_SIZE, fetch_all_skus, fetch_product_details
from db import DB_DEFAULT, init_db, upsert_products
from product_transform import transform_product


def print_catalogue_stats(conn: sqlite3.Connection) -> None:
    """Print a summary of the ingested catalogue."""
    rows = conn.execute("""
        SELECT
            COUNT(*)                                                AS total,
            SUM(is_available)                                       AS available,
            SUM(in_assortment)                                      AS in_assortment,
            SUM(is_bio)                                             AS bio,
            SUM(has_nightshade)                                     AS nightshade,
            SUM(is_clean_15)                                        AS clean_15,
            SUM(CASE WHEN json_array_length(ingredients) > 0 THEN 1 ELSE 0 END) AS has_ingredients,
            SUM(CASE WHEN json_array_length(allergens_contains) > 0 THEN 1 ELSE 0 END) AS has_allergens,
            SUM(CASE WHEN protein_g IS NOT NULL THEN 1 ELSE 0 END) AS has_protein
        FROM products
    """).fetchone()

    filtered = conn.execute("SELECT COUNT(*) FROM filtered_products").fetchone()[0]
    meal_safe = conn.execute("SELECT COUNT(*) FROM meal_safe_products").fetchone()[0]

    print("\n=== Catalogue Statistics ===")
    print(f"  Total products:          {rows['total']:>6}")
    print(f"  Available:               {rows['available']:>6}")
    print(f"  In assortment:           {rows['in_assortment']:>6}")
    print(f"  Bio/organic:             {rows['bio']:>6}")
    print(f"  Contains nightshade:     {rows['nightshade']:>6}")
    print(f"  Clean 15 produce:        {rows['clean_15']:>6}")
    print(f"  Have ingredient data:    {rows['has_ingredients']:>6}")
    print(f"  Have allergen data:      {rows['has_allergens']:>6}")
    print(f"  Have protein value:      {rows['has_protein']:>6}")
    print(f"  Filtered (safety):       {filtered:>6}")
    print(f"  Meal-safe (bio+C15):     {meal_safe:>6}")

    print("\n=== Top Categories ===")
    cats = conn.execute("""
        SELECT root_category, COUNT(*) AS n
        FROM products
        WHERE is_available = 1
        GROUP BY root_category
        ORDER BY n DESC
        LIMIT 15
    """).fetchall()
    for c in cats:
        print(f"  {c['root_category'] or '(none)':<35} {c['n']:>5}")

    print("\n=== Allergen Distribution (top 10) ===")
    allergen_rows = conn.execute("""
        SELECT value AS allergen, COUNT(*) AS n
        FROM products, json_each(products.allergens_contains)
        GROUP BY allergen
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()
    for a in allergen_rows:
        print(f"  {a['allergen']:<25} {a['n']:>5}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Jumbo product catalogue into SQLite")
    parser.add_argument("--db", type=Path, default=DB_DEFAULT)
    parser.add_argument("--limit", type=int, default=None, help="Limit number of products (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but don't write to database")
    args = parser.parse_args()

    print(f"Database: {args.db}")
    print(f"Dry run:  {args.dry_run}")
    if args.limit:
        print(f"Limit:    {args.limit} products")

    t_start = time.time()
    timestamp = datetime.now(timezone.utc).isoformat()

    skus = fetch_all_skus(limit=args.limit)
    if not skus:
        print("No SKUs returned. Aborting.")
        return

    print(f"\nFetching product details ({len(skus)} products, {DETAIL_BATCH_SIZE}/batch)...")
    raw_products = fetch_product_details(skus)

    print("\nTransforming product data...")
    transformed = [transform_product(p, timestamp) for p in raw_products]

    if args.dry_run:
        print(f"\nDry run: would write {len(transformed)} products.")
        for p in transformed[:3]:
            print(f"\n  {p['sku']} | {p['title'][:50]}")
            print(f"    bio={p['is_bio']} nightshade={p['has_nightshade']}")
            print(f"    allergens_contains={p['allergens_contains']}")
            print(f"    ingredients[:80]={p['ingredients'][:80]}")
            print(f"    protein={p['protein_g']}g energy={p['energy_kcal']}kcal")
        return

    print(f"\nInitialising database at {args.db}...")
    conn = init_db(args.db)

    print(f"Writing {len(transformed)} products...")
    count = upsert_products(conn, transformed)
    print(f"Upserted {count} products.")

    print_catalogue_stats(conn)

    elapsed = time.time() - t_start
    print(f"\nIngestion complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
