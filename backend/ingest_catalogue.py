"""
Jumbo product catalogue ingestion script.

Fetches the full Jumbo product catalogue via the Jumbo website GraphQL API
and populates the SQLite products table.

Usage:
    python ingest_catalogue.py [--db PATH] [--limit N] [--dry-run]

API:  https://www.jumbo.com/api/graphql
Auth: x-source: JUMBO_WEB header (no account needed)
"""

import argparse
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
import ssl
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

GRAPHQL_URL = "https://www.jumbo.com/api/graphql"
DB_DEFAULT = Path(__file__).parent / "db" / "catalogue.db"
SCHEMA_FILE = Path(__file__).parent / "db" / "schema.sql"

SEARCH_PAGE_SIZE = 200      # products per search page (API accepts up to 200)
DETAIL_BATCH_SIZE = 100     # SKUs per products() detail query (API max is 100)
REQUEST_DELAY_S = 0.1       # polite delay between requests

GRAPHQL_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Origin": "https://www.jumbo.com",
    "Referer": "https://www.jumbo.com/",
    "x-source": "JUMBO_WEB",
    "apollographql-client-name": "JUMBO_WEB",
    "apollographql-client-version": "1.0.0",
}

# Nightshade ingredients (Dutch + common English names in Dutch products)
# All lowercase for case-insensitive matching
NIGHTSHADE_TERMS = {
    "tomaat", "tomaten", "tomatenpoeder", "tomatenpuree", "tomatensaus",
    "tomatenpasta", "tomaatpuree", "cherry tomaat",
    "paprika", "rode paprika", "groene paprika", "gele paprika", "paprikapoeder",
    "gerookte paprika", "zoete paprika", "capsicum",
    "chilipeper", "chili", "chilli", "cayennepeper", "cayenne",
    "jalapeño", "jalapen", "habanero", "serrano", "bird's eye",
    "tabasco", "sriracha", "sambal",
    "aubergine", "eggplant",
    "aardappel", "aardappelen", "aardappelzetmeel", "aardappelpoeder",
    "aardappelvlokken", "aardappelgranulaat",
    "goji", "gojibes",
}

# Dutch bio/organic indicators in product titles (lowercase)
BIO_TITLE_TERMS = {
    "biologisch", "biologische", "biologico", "biologique",
    "bio ", " bio", "eko", "ekologisch", "fairtrade bio",
    "organic",
}


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------

_ssl_ctx = ssl.create_default_context()


def _gql(operation_name: str, query: str, variables: dict) -> dict:
    """Execute a GraphQL request. Returns the parsed response body."""
    payload = json.dumps({
        "operationName": operation_name,
        "query": query,
        "variables": variables,
    }).encode("utf-8")

    req = urllib.request.Request(
        GRAPHQL_URL,
        data=payload,
        headers=GRAPHQL_HEADERS,
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, context=_ssl_ctx, timeout=30) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {e.code} from GraphQL: {body[:300]}") from e


# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------

SEARCH_QUERY = """\
query SearchSKUs($input: ProductSearchInput!) {
  searchProducts(input: $input) {
    count
    products {
      sku
    }
  }
}
"""

DETAIL_QUERY = """\
query GetProductDetail($skus: [String!]!) {
  products(skus: $skus) {
    sku
    title
    brand
    ean
    rootCategory
    packSizeDisplay
    description
    storage
    ingredients
    inAssortment
    isMedicine
    retailSet
    image
    productAllergens {
      contains
      mayContain
    }
    nutritionsTable {
      columns
      rows
    }
    nutriScore {
      value
    }
    availability {
      isAvailable
      availability
    }
    price {
      price
      promoPrice
      pricePerUnit {
        price
        unit
      }
    }
    categories {
      name
      path
      id
    }
    primaryProductBadges {
      alt
      image
    }
    secondaryProductBadges {
      alt
      image
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Catalogue fetching
# ---------------------------------------------------------------------------

def _gql_with_retry(operation_name: str, query: str, variables: dict,
                    max_attempts: int = 3) -> dict:
    """Execute a GraphQL request with exponential backoff retry."""
    for attempt in range(1, max_attempts + 1):
        try:
            result = _gql(operation_name, query, variables)
            if result.get("data") is not None:
                return result
            # GraphQL-level error with no data — may be transient
            errs = result.get("errors", [])
            if attempt < max_attempts:
                wait = 2 ** attempt
                print(f"\n  Retry {attempt}/{max_attempts} for {operation_name} "
                      f"({errs[0].get('message','')[:60] if errs else 'null data'}) "
                      f"— waiting {wait}s")
                time.sleep(wait)
            else:
                return result
        except Exception as exc:
            if attempt < max_attempts:
                wait = 2 ** attempt
                print(f"\n  Retry {attempt}/{max_attempts} for {operation_name}: "
                      f"{exc} — waiting {wait}s")
                time.sleep(wait)
            else:
                raise
    return {}


def fetch_all_skus(limit: int | None = None) -> list[str]:
    """Fetch every SKU from the full catalogue using paginated wildcard search."""
    skus: list[str] = []
    offset = 0

    print("Fetching SKUs from catalogue...")
    while True:
        batch_limit = min(SEARCH_PAGE_SIZE, limit - len(skus)) if limit else SEARCH_PAGE_SIZE
        result = _gql_with_retry("SearchSKUs", SEARCH_QUERY, {
            "input": {
                "searchTerms": "*",
                "searchType": "keyword",
                "limit": batch_limit,
                "offSet": offset,
            }
        })

        sp = (result.get("data") or {}).get("searchProducts") or {}
        total = sp.get("count", 0)
        page_skus = [p["sku"] for p in sp.get("products", [])]

        if not page_skus:
            break

        skus.extend(page_skus)
        offset += len(page_skus)

        print(f"  {len(skus):>6} / {total} SKUs fetched", end="\r")

        if limit and len(skus) >= limit:
            break
        if len(skus) >= total:
            break

        time.sleep(REQUEST_DELAY_S)

    print(f"\nTotal SKUs fetched: {len(skus)}")
    return skus


def fetch_product_details(skus: list[str]) -> list[dict]:
    """Fetch full product detail for a list of SKUs in batches of DETAIL_BATCH_SIZE."""
    products: list[dict] = []

    for i in range(0, len(skus), DETAIL_BATCH_SIZE):
        batch = skus[i : i + DETAIL_BATCH_SIZE]
        result = _gql_with_retry("GetProductDetail", DETAIL_QUERY, {"skus": batch})

        errors = result.get("errors", [])
        if errors:
            print(f"  Warning: GraphQL errors for batch {i}-{i+len(batch)}: "
                  f"{errors[0].get('message', '')[:120]}")

        batch_products = result.get("data", {}).get("products", [])
        products.extend(p for p in batch_products if p)

        done = min(i + DETAIL_BATCH_SIZE, len(skus))
        print(f"  Details fetched: {done:>6} / {len(skus)}", end="\r")

        time.sleep(REQUEST_DELAY_S)

    print(f"\nTotal product details fetched: {len(products)}")
    return products


# ---------------------------------------------------------------------------
# Data transformations
# ---------------------------------------------------------------------------

def detect_bio(title: str, ingredients: list[str]) -> bool:
    """Return True if the product is bio/organic based on title or ingredients."""
    title_lower = title.lower()
    for term in BIO_TITLE_TERMS:
        if term in title_lower:
            return True

    ingredients_text = " ".join(ingredients).lower()
    # Common ingredient annotations for bio origin — require explicit bio phrasing
    for phrase in ("biologische oorsprong", "van biologisch", "organic origin"):
        if phrase in ingredients_text:
            return True

    return False


def detect_nightshade(ingredients: list[str]) -> bool:
    """
    Return True if any nightshade ingredient is present.

    Scans the ingredient list for nightshade terms. Uses whole-word tokenisation
    so 'tomatenpoeder 2%' matches 'tomatenpoeder'.

    Sweet potato (zoete aardappel) is NOT a nightshade (family Convolvulaceae).
    We strip "zoete-aardappel*" occurrences before checking for potato terms.
    """
    ingredients_text = " ".join(ingredients).lower()

    # Remove "zoete aardappel*" and "sweet potato" before nightshade scanning
    # so that sweet potato doesn't trigger the aardappel/aardappelzetmeel checks
    cleaned = re.sub(r"zoete[-\s]aardappel\w*", "", ingredients_text)
    cleaned = re.sub(r"sweet[-\s]potato\w*", "", cleaned)

    words = re.findall(r"[a-zàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]+", cleaned)
    word_set = set(words)

    for term in NIGHTSHADE_TERMS:
        if " " in term:
            if term in cleaned:
                return True
        else:
            if term in word_set:
                return True

    return False


def normalise_allergens(raw: list | None) -> list[str]:
    """Lowercase and deduplicate allergen names."""
    if not raw:
        return []
    return sorted(set(a.lower().strip() for a in raw if a and a.strip()))


def parse_nutritions(table: dict | None) -> tuple[dict, dict | None]:
    """
    Parse the nutritionsTable into a dict of known macronutrients and
    return (parsed_values, raw_table_or_None).

    parsed_values keys: energy_kcal, protein_g, carbs_g, fat_g,
                        saturated_fat_g, sugar_g, salt_g
    All values are float or None.
    """
    if not table or not table.get("rows"):
        return {k: None for k in ("energy_kcal", "protein_g", "carbs_g",
                                   "fat_g", "saturated_fat_g", "sugar_g", "salt_g")}, None

    rows = table["rows"]

    # Flatten rows: some rows have a single element (continuation of previous)
    # Join consecutive single-element rows with their preceding row
    flat: list[tuple[str, str]] = []
    pending_label: str = ""
    pending_value: str = ""

    for row in rows:
        if len(row) == 0:
            continue
        elif len(row) == 1:
            # Continuation token — append to current pending
            pending_label += " " + row[0]
        elif len(row) >= 2:
            # Save previous pending pair
            if pending_label or pending_value:
                flat.append((pending_label.strip(), (pending_value or "").strip()))
            pending_label = row[0] or ""
            pending_value = row[1] or ""

    if pending_label or pending_value:
        flat.append((pending_label.strip(), (pending_value or "").strip()))

    def extract_number(s: str, unit_divisor: float = 1.0) -> float | None:
        """Pull the first decimal/integer number from a string, applying unit_divisor."""
        m = re.search(r"([\d]+[.,]?[\d]*)", s.replace(",", "."))
        return float(m.group(1)) / unit_divisor if m else None

    def value_unit_divisor(s: str) -> float:
        """Return 1000 if the value is in mg (so caller divides to get grams), else 1."""
        # Match 'mg' that isn't part of a longer unit like 'mcg'
        if re.search(r"\d\s*mg\b", s, re.IGNORECASE):
            return 1000.0
        return 1.0

    def extract_kcal(flat_pairs: list[tuple[str, str]]) -> float | None:
        """
        Extract the kcal value from the flattened nutrition rows.

        Handles three formats seen in the Jumbo API:
          - "kcal 579"       (Dutch label: kcal before number)
          - "kJ 2412 / kcal 579"  (combined on one line)
          - "44 kJ/(10 kcal)"  (bracketed)
          - "579 kcal"       (number before kcal, no kJ on same line)
        """
        for _, value in flat_pairs:
            v = value.replace(",", ".")
            # Priority 1: kcal followed by a number (Dutch format, unambiguous)
            m = re.search(r"kcal\s*([\d]+\.?[\d]*)", v, re.IGNORECASE)
            if m:
                return float(m.group(1))
            # Priority 2: number followed by kcal, but ONLY if kJ does not
            # appear anywhere in the same value string (avoids "kJ 2412 kcal" trap)
            if "kj" not in v.lower():
                m2 = re.search(r"([\d]+\.?[\d]*)\s*kcal", v, re.IGNORECASE)
                if m2:
                    return float(m2.group(1))
        return None

    parsed: dict[str, float | None] = {k: None for k in (
        "energy_kcal", "protein_g", "carbs_g", "fat_g",
        "saturated_fat_g", "sugar_g", "salt_g"
    )}

    parsed["energy_kcal"] = extract_kcal(flat)

    for label, value in flat:
        label_l = label.lower()

        if any(t in label_l for t in ("eiwit", "protein")):
            parsed["protein_g"] = extract_number(value, value_unit_divisor(value))

        elif any(t in label_l for t in ("koolhydrat", "carbohydr")):
            if parsed["carbs_g"] is None:
                parsed["carbs_g"] = extract_number(value, value_unit_divisor(value))

        elif "suiker" in label_l or "sugar" in label_l:
            if parsed["sugar_g"] is None:
                parsed["sugar_g"] = extract_number(value, value_unit_divisor(value))

        elif "verzadigd" in label_l or "saturated" in label_l:
            parsed["saturated_fat_g"] = extract_number(value, value_unit_divisor(value))

        elif any(t in label_l for t in ("vet", "fat", "lipid")):
            if parsed["fat_g"] is None:
                parsed["fat_g"] = extract_number(value, value_unit_divisor(value))

        elif "zout" in label_l or "salt" in label_l:
            # Prefer explicit zout/salt over sodium; first match wins
            if parsed["salt_g"] is None:
                parsed["salt_g"] = extract_number(value, value_unit_divisor(value))

        elif "sodium" in label_l or "natrium" in label_l:
            # Sodium is sometimes listed alongside or instead of zout.
            # Only use it if we haven't already set salt from a zout row.
            # Convert sodium to salt equivalent: salt = sodium × 2.5
            if parsed["salt_g"] is None:
                na = extract_number(value, value_unit_divisor(value))
                if na is not None:
                    parsed["salt_g"] = round(na * 2.5, 3)

    return parsed, table


def transform_product(raw: dict, timestamp: str) -> dict:
    """Transform a raw API product dict into a flat dict ready for SQLite INSERT."""
    ingredients: list[str] = raw.get("ingredients") or []
    allergens_contains = normalise_allergens(
        (raw.get("productAllergens") or {}).get("contains")
    )
    allergens_may_contain = normalise_allergens(
        (raw.get("productAllergens") or {}).get("mayContain")
    )

    nutri = raw.get("nutriScore")
    nutri_score = nutri.get("value") if nutri else None

    price_block = raw.get("price") or {}
    price_per_unit = price_block.get("pricePerUnit") or {}

    avail = raw.get("availability") or {}
    categories = raw.get("categories") or []
    all_badges = (raw.get("primaryProductBadges") or []) + (raw.get("secondaryProductBadges") or [])

    nutri_table = raw.get("nutritionsTable")
    parsed_nutri, raw_table = parse_nutritions(nutri_table)

    return {
        "sku": raw["sku"],
        "title": raw.get("title", ""),
        "brand": raw.get("brand"),
        "ean": raw.get("ean"),
        "root_category": raw.get("rootCategory"),
        "pack_size": raw.get("packSizeDisplay"),
        "description": raw.get("description"),
        "storage": raw.get("storage"),
        "ingredients": json.dumps(ingredients, ensure_ascii=False),
        "allergens_contains": json.dumps(allergens_contains, ensure_ascii=False),
        "allergens_may_contain": json.dumps(allergens_may_contain, ensure_ascii=False),
        "nutri_score": nutri_score,
        "is_bio": int(detect_bio(raw.get("title", ""), ingredients)),
        "has_nightshade": int(detect_nightshade(ingredients)),
        "is_available": int(bool(avail.get("isAvailable"))),
        "in_assortment": int(bool(raw.get("inAssortment"))),
        "price_cents": price_block.get("price"),
        "promo_price_cents": price_block.get("promoPrice"),
        "price_per_unit_cents": price_per_unit.get("price"),
        "price_per_unit_unit": price_per_unit.get("unit"),
        "energy_kcal": parsed_nutri["energy_kcal"],
        "protein_g": parsed_nutri["protein_g"],
        "carbs_g": parsed_nutri["carbs_g"],
        "fat_g": parsed_nutri["fat_g"],
        "saturated_fat_g": parsed_nutri["saturated_fat_g"],
        "sugar_g": parsed_nutri["sugar_g"],
        "salt_g": parsed_nutri["salt_g"],
        "nutritions_raw": json.dumps(raw_table, ensure_ascii=False) if raw_table else None,
        "categories": json.dumps(categories, ensure_ascii=False),
        "badges": json.dumps(all_badges, ensure_ascii=False),
        "is_medicine": int(bool(raw.get("isMedicine"))),
        "retail_set": int(bool(raw.get("retailSet"))),
        "image_url": raw.get("image"),
        "last_updated": timestamp,
    }


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db(db_path: Path) -> sqlite3.Connection:
    """Create the database and apply the schema if it doesn't exist."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")

    schema = SCHEMA_FILE.read_text(encoding="utf-8")
    conn.executescript(schema)
    conn.commit()
    return conn


def upsert_products(conn: sqlite3.Connection, products: list[dict]) -> int:
    """Upsert transformed products into the database. Returns count inserted/updated."""
    sql = """
    INSERT INTO products (
        sku, title, brand, ean, root_category, pack_size, description, storage,
        ingredients, allergens_contains, allergens_may_contain,
        nutri_score, is_bio, has_nightshade, is_available, in_assortment,
        price_cents, promo_price_cents, price_per_unit_cents, price_per_unit_unit,
        energy_kcal, protein_g, carbs_g, fat_g, saturated_fat_g, sugar_g, salt_g,
        nutritions_raw, categories, badges, is_medicine, retail_set, image_url, last_updated
    ) VALUES (
        :sku, :title, :brand, :ean, :root_category, :pack_size, :description, :storage,
        :ingredients, :allergens_contains, :allergens_may_contain,
        :nutri_score, :is_bio, :has_nightshade, :is_available, :in_assortment,
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
        has_nightshade=excluded.has_nightshade, is_available=excluded.is_available,
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


# ---------------------------------------------------------------------------
# Stats / validation
# ---------------------------------------------------------------------------

def print_catalogue_stats(conn: sqlite3.Connection) -> None:
    """Print a summary of the ingested catalogue."""
    rows = conn.execute("""
        SELECT
            COUNT(*)                                                AS total,
            SUM(is_available)                                       AS available,
            SUM(in_assortment)                                      AS in_assortment,
            SUM(is_bio)                                             AS bio,
            SUM(has_nightshade)                                     AS nightshade,
            SUM(CASE WHEN json_array_length(ingredients) > 0 THEN 1 ELSE 0 END) AS has_ingredients,
            SUM(CASE WHEN json_array_length(allergens_contains) > 0 THEN 1 ELSE 0 END) AS has_allergens,
            SUM(CASE WHEN protein_g IS NOT NULL THEN 1 ELSE 0 END) AS has_protein
        FROM products
    """).fetchone()

    filtered = conn.execute("SELECT COUNT(*) FROM filtered_products").fetchone()[0]

    print("\n=== Catalogue Statistics ===")
    print(f"  Total products:          {rows['total']:>6}")
    print(f"  Available:               {rows['available']:>6}")
    print(f"  In assortment:           {rows['in_assortment']:>6}")
    print(f"  Bio/organic:             {rows['bio']:>6}")
    print(f"  Contains nightshade:     {rows['nightshade']:>6}")
    print(f"  Have ingredient data:    {rows['has_ingredients']:>6}")
    print(f"  Have allergen data:      {rows['has_allergens']:>6}")
    print(f"  Have protein value:      {rows['has_protein']:>6}")
    print(f"  Filtered (meal-safe):    {filtered:>6}")

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Jumbo product catalogue into SQLite")
    parser.add_argument("--db", type=Path, default=DB_DEFAULT, help="SQLite database path")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of products (for testing)")
    parser.add_argument("--dry-run", action="store_true", help="Fetch data but don't write to database")
    args = parser.parse_args()

    print(f"Database: {args.db}")
    print(f"Dry run:  {args.dry_run}")
    if args.limit:
        print(f"Limit:    {args.limit} products")

    t_start = time.time()
    timestamp = datetime.now(timezone.utc).isoformat()

    # Phase 1: Get all SKUs
    skus = fetch_all_skus(limit=args.limit)
    if not skus:
        print("No SKUs returned. Aborting.")
        return

    # Phase 2: Fetch full product details in batches
    print(f"\nFetching product details ({len(skus)} products, {DETAIL_BATCH_SIZE}/batch)...")
    raw_products = fetch_product_details(skus)

    # Phase 3: Transform
    print("\nTransforming product data...")
    transformed = [transform_product(p, timestamp) for p in raw_products]

    if args.dry_run:
        print(f"\nDry run: would write {len(transformed)} products.")
        # Show a few samples
        for p in transformed[:3]:
            print(f"\n  {p['sku']} | {p['title'][:50]}")
            print(f"    bio={p['is_bio']} nightshade={p['has_nightshade']}")
            print(f"    allergens_contains={p['allergens_contains']}")
            print(f"    ingredients[:80]={p['ingredients'][:80]}")
            print(f"    protein={p['protein_g']}g energy={p['energy_kcal']}kcal")
        return

    # Phase 4: Write to database
    print(f"\nInitialising database at {args.db}...")
    conn = init_db(args.db)

    print(f"Writing {len(transformed)} products...")
    count = upsert_products(conn, transformed)
    print(f"Upserted {count} products.")

    # Phase 5: Stats
    print_catalogue_stats(conn)

    elapsed = time.time() - t_start
    print(f"\nIngestion complete in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
