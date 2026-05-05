-- Jumbo product catalogue schema
-- Designed from actual Jumbo GraphQL API field survey

CREATE TABLE IF NOT EXISTS products (
    sku                     TEXT PRIMARY KEY,
    title                   TEXT NOT NULL,
    brand                   TEXT,
    ean                     TEXT,
    root_category           TEXT,
    pack_size               TEXT,
    description             TEXT,
    storage                 TEXT,

    -- Ingredients as JSON array of strings; empty array [] means data is absent
    ingredients             TEXT NOT NULL DEFAULT '[]',

    -- Allergens as JSON arrays of lowercase Dutch names
    -- e.g. ["melk", "gluten", "tarwe"]
    allergens_contains      TEXT NOT NULL DEFAULT '[]',
    allergens_may_contain   TEXT NOT NULL DEFAULT '[]',

    -- Nutri-Score letter grade: A, B, C, D, E or NULL
    nutri_score             TEXT,

    -- Derived bio/organic flag: 1 if title/ingredients indicate biological origin
    is_bio                  INTEGER NOT NULL DEFAULT 0,

    -- Derived nightshade flag: 1 if ingredient scan finds nightshade content
    has_nightshade          INTEGER NOT NULL DEFAULT 0,

    -- Availability and assortment
    is_available            INTEGER NOT NULL DEFAULT 0,
    in_assortment           INTEGER NOT NULL DEFAULT 0,

    -- Pricing in euro cents (integer)
    price_cents             INTEGER,
    promo_price_cents       INTEGER,
    price_per_unit_cents    INTEGER,
    price_per_unit_unit     TEXT,

    -- Parsed nutritional values per 100g/100ml (NULL if absent or unparseable)
    energy_kcal             REAL,
    protein_g               REAL,
    carbs_g                 REAL,
    fat_g                   REAL,
    saturated_fat_g         REAL,
    sugar_g                 REAL,
    salt_g                  REAL,

    -- Full nutritional table stored as JSON for reference
    -- {"columns": [...], "rows": [[...], ...]}
    nutritions_raw          TEXT,

    -- Product categories as JSON array of {name, path, id}
    categories              TEXT NOT NULL DEFAULT '[]',

    -- Badges as JSON array of {alt, image}
    badges                  TEXT NOT NULL DEFAULT '[]',

    -- Product flags
    is_medicine             INTEGER NOT NULL DEFAULT 0,
    retail_set              INTEGER NOT NULL DEFAULT 0,

    -- Image URL
    image_url               TEXT,

    -- Ingestion timestamp (ISO 8601)
    last_updated            TEXT NOT NULL
);

-- Filtered view: products safe for meal planning
-- Excludes: nightshade, milk protein, no ingredient data, unavailable, non-food
CREATE VIEW IF NOT EXISTS filtered_products AS
SELECT *
FROM products
WHERE
    -- Only food categories relevant to cooking and meal planning
    root_category IN (
        'Vlees, vis en vega',
        'Aardappelen, groente en fruit',
        'Zuivel, boter en eieren',
        'Diepvries',
        'Brood en gebak',
        'Ontbijt, broodbeleg en bakproducten',
        'Vleeswaren, kaas en tapas',
        'Conserven, soepen, sauzen, oliën',
        'Wereldkeukens, kruiden, pasta en rijst',
        'Koek, snoep, chocolade en chips',
        'Frisdrank en sappen',
        'Koffie en thee',
        'Bier en wijn',
        'Kaas',
        'Groentenconserven'
    )

    -- Fail-safe: must have ingredient data
    AND json_array_length(ingredients) > 0

    -- Hard block: no nightshade
    AND has_nightshade = 0

    -- Hard block: no milk protein (contains melk, lactose, or dairy allergens)
    AND NOT EXISTS (
        SELECT 1 FROM json_each(allergens_contains)
        WHERE lower(value) IN ('melk', 'lactose', 'caseïne', 'wei')
    )

    AND is_available = 1
    AND in_assortment = 1
    AND is_medicine = 0
    AND retail_set = 0;

CREATE INDEX IF NOT EXISTS idx_products_root_category ON products(root_category);
CREATE INDEX IF NOT EXISTS idx_products_is_bio ON products(is_bio);
CREATE INDEX IF NOT EXISTS idx_products_is_available ON products(is_available);
CREATE INDEX IF NOT EXISTS idx_products_has_nightshade ON products(has_nightshade);
CREATE INDEX IF NOT EXISTS idx_products_last_updated ON products(last_updated);
