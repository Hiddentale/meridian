-- Milestone 2: Allergen and Bio Filtering Layer
-- Adds Clean 15 detection, reference table, and meal-safe view

ALTER TABLE products ADD COLUMN is_clean_15 INTEGER NOT NULL DEFAULT 0;

-- EWG Clean 15 reference list
CREATE TABLE IF NOT EXISTS clean_15 (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    name    TEXT NOT NULL UNIQUE,
    name_en TEXT
);

-- Meal-safe view: filtered_products + bio/Clean 15 filter for fresh produce
-- For 'Aardappelen, groente en fruit': only bio or Clean 15 conventional
-- All other food categories: no bio requirement
CREATE VIEW IF NOT EXISTS meal_safe_products AS
SELECT *
FROM filtered_products
WHERE
    root_category != 'Aardappelen, groente en fruit'
    OR is_bio = 1
    OR is_clean_15 = 1;

CREATE INDEX IF NOT EXISTS idx_products_is_clean_15 ON products(is_clean_15);
