-- Milestone 3 prep: Add CHECK constraints to the products table.
-- SQLite does not support ALTER TABLE ADD CONSTRAINT, so the table must be recreated.
-- Views that depend on products are dropped before and recreated after.

CREATE TABLE products_new (
    sku                     TEXT PRIMARY KEY,
    title                   TEXT NOT NULL,
    brand                   TEXT,
    ean                     TEXT,
    root_category           TEXT,
    pack_size               TEXT,
    description             TEXT,
    storage                 TEXT,
    ingredients             TEXT NOT NULL DEFAULT '[]',
    allergens_contains      TEXT NOT NULL DEFAULT '[]',
    allergens_may_contain   TEXT NOT NULL DEFAULT '[]',
    nutri_score             TEXT,
    is_bio                  INTEGER NOT NULL DEFAULT 0 CHECK (is_bio IN (0, 1)),
    has_nightshade          INTEGER NOT NULL DEFAULT 0 CHECK (has_nightshade IN (0, 1)),
    is_clean_15             INTEGER NOT NULL DEFAULT 0 CHECK (is_clean_15 IN (0, 1)),
    is_available            INTEGER NOT NULL DEFAULT 0 CHECK (is_available IN (0, 1)),
    in_assortment           INTEGER NOT NULL DEFAULT 0 CHECK (in_assortment IN (0, 1)),
    price_cents             INTEGER CHECK (price_cents IS NULL OR price_cents >= 0),
    promo_price_cents       INTEGER CHECK (promo_price_cents IS NULL OR promo_price_cents >= 0),
    price_per_unit_cents    INTEGER CHECK (price_per_unit_cents IS NULL OR price_per_unit_cents >= 0),
    price_per_unit_unit     TEXT,
    energy_kcal             REAL,
    protein_g               REAL,
    carbs_g                 REAL,
    fat_g                   REAL,
    saturated_fat_g         REAL,
    sugar_g                 REAL,
    salt_g                  REAL,
    nutritions_raw          TEXT,
    categories              TEXT NOT NULL DEFAULT '[]',
    badges                  TEXT NOT NULL DEFAULT '[]',
    is_medicine             INTEGER NOT NULL DEFAULT 0 CHECK (is_medicine IN (0, 1)),
    retail_set              INTEGER NOT NULL DEFAULT 0 CHECK (retail_set IN (0, 1)),
    image_url               TEXT,
    last_updated            TEXT NOT NULL
);

INSERT INTO products_new (
    sku, title, brand, ean, root_category, pack_size, description, storage,
    ingredients, allergens_contains, allergens_may_contain,
    nutri_score, is_bio, has_nightshade, is_clean_15, is_available, in_assortment,
    price_cents, promo_price_cents, price_per_unit_cents, price_per_unit_unit,
    energy_kcal, protein_g, carbs_g, fat_g, saturated_fat_g, sugar_g, salt_g,
    nutritions_raw, categories, badges, is_medicine, retail_set, image_url, last_updated
)
SELECT
    sku, title, brand, ean, root_category, pack_size, description, storage,
    ingredients, allergens_contains, allergens_may_contain,
    nutri_score, is_bio, has_nightshade, is_clean_15, is_available, in_assortment,
    price_cents, promo_price_cents, price_per_unit_cents, price_per_unit_unit,
    energy_kcal, protein_g, carbs_g, fat_g, saturated_fat_g, sugar_g, salt_g,
    nutritions_raw, categories, badges, is_medicine, retail_set, image_url, last_updated
FROM products;

DROP VIEW IF EXISTS meal_safe_products;
DROP VIEW IF EXISTS filtered_products;
DROP TABLE products;
ALTER TABLE products_new RENAME TO products;

CREATE INDEX IF NOT EXISTS idx_products_root_category ON products(root_category);
CREATE INDEX IF NOT EXISTS idx_products_is_bio ON products(is_bio);
CREATE INDEX IF NOT EXISTS idx_products_is_available ON products(is_available);
CREATE INDEX IF NOT EXISTS idx_products_has_nightshade ON products(has_nightshade);
CREATE INDEX IF NOT EXISTS idx_products_is_clean_15 ON products(is_clean_15);
CREATE INDEX IF NOT EXISTS idx_products_last_updated ON products(last_updated);

CREATE VIEW filtered_products AS
SELECT *
FROM products
WHERE
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
    AND json_array_length(ingredients) > 0
    AND has_nightshade = 0
    AND NOT EXISTS (
        SELECT 1 FROM json_each(allergens_contains)
        WHERE lower(value) IN ('melk', 'lactose', 'caseïne', 'wei')
    )
    AND is_available = 1
    AND in_assortment = 1
    AND is_medicine = 0
    AND retail_set = 0;

CREATE VIEW meal_safe_products AS
SELECT *
FROM filtered_products
WHERE
    root_category != 'Aardappelen, groente en fruit'
    OR is_bio = 1
    OR is_clean_15 = 1;
