import json
import re

NIGHTSHADE_TERMS = {
    "tomaat",
    "tomaten",
    "tomatenpoeder",
    "tomatenpuree",
    "tomatensaus",
    "tomatenpasta",
    "tomaatpuree",
    "cherry tomaat",
    "paprika",
    "rode paprika",
    "groene paprika",
    "gele paprika",
    "paprikapoeder",
    "gerookte paprika",
    "zoete paprika",
    "capsicum",
    "chilipeper",
    "chili",
    "chilli",
    "cayennepeper",
    "cayenne",
    "jalapeño",
    "jalapen",
    "habanero",
    "serrano",
    "bird's eye",
    "tabasco",
    "sriracha",
    "sambal",
    "aubergine",
    "eggplant",
    "aardappel",
    "aardappelen",
    "aardappelzetmeel",
    "aardappelpoeder",
    "aardappelvlokken",
    "aardappelgranulaat",
    "goji",
    "gojibes",
}

BIO_TITLE_TERMS = {
    "biologisch",
    "biologische",
    "biologico",
    "biologique",
    "bio ",
    " bio",
    "eko",
    "ekologisch",
    "fairtrade bio",
    "organic",
}

CLEAN_15_TERMS: frozenset[str] = frozenset(
    {
        "avocado",
        "ananas",
        "ui",
        "uien",
        "sjalot",
        "sjalotten",
        "zilverui",
        "papaja",
        "suikererwten",
        "doperwten",
        "asperge",
        "asperges",
        "meloen",
        "honingmeloen",
        "galia",
        "cantaloupe",
        "kiwi",
        "champignons",
        "champignon",
        "paddenstoelen",
        "paddenstoel",
        "oesterzwam",
        "shiitake",
        "portobello",
        "mango",
        "watermeloen",
        "wortel",
        "wortelen",
        "worteltjes",
        "winterwortel",
        "mais",
        "maïs",
        "spitskool",
        "wittekool",
        "rodekool",
        "savooienkool",
        "boerenkool",
        "zoete aardappel",
    }
)

CLEAN_15_ITEMS: list[tuple[str, str]] = [
    ("avocado", "Avocado"),
    ("zoete maïs", "Sweet corn"),
    ("ananas", "Pineapple"),
    ("ui", "Onion"),
    ("papaja", "Papaya"),
    ("suikererwten", "Sweet peas (frozen)"),
    ("asperge", "Asparagus"),
    ("honingmeloen", "Honeydew melon"),
    ("kiwi", "Kiwi"),
    ("kool", "Cabbage"),
    ("champignons", "Mushrooms"),
    ("mango", "Mango"),
    ("zoete aardappel", "Sweet potato"),
    ("watermeloen", "Watermelon"),
    ("wortel", "Carrot"),
]

_LETTER_PATTERN = re.compile(r"[a-zàáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]+")
_NUMBER_PATTERN = re.compile(r"([\d]+[.,]?[\d]*)")
_MG_PATTERN = re.compile(r"\d\s*mg\b", re.IGNORECASE)
_KCAL_AFTER_PATTERN = re.compile(r"kcal\s*([\d]+\.?[\d]*)", re.IGNORECASE)
_KCAL_BEFORE_PATTERN = re.compile(r"([\d]+\.?[\d]*)\s*kcal", re.IGNORECASE)


def detect_clean_15(title: str) -> bool:
    """Return True if the product title matches a Clean 15 produce item."""
    title_lower = title.lower()
    words = set(_LETTER_PATTERN.findall(title_lower))
    for term in CLEAN_15_TERMS:
        if " " in term:
            if term in title_lower:
                return True
        else:
            if term in words:
                return True
    return False


def detect_bio(title: str, ingredients: list[str]) -> bool:
    """Return True if the product is bio/organic based on title or ingredients."""
    title_lower = title.lower()
    for term in BIO_TITLE_TERMS:
        if term in title_lower:
            return True

    ingredients_text = " ".join(ingredients).lower()
    for phrase in ("biologische oorsprong", "van biologisch", "organic origin"):
        if phrase in ingredients_text:
            return True

    return False


def detect_nightshade(ingredients: list[str]) -> bool:
    """Return True if any nightshade ingredient is present.

    Sweet potato (zoete aardappel) is NOT a nightshade; it is stripped before scanning
    so it does not trigger the aardappel/aardappelzetmeel checks.
    """
    ingredients_text = " ".join(ingredients).lower()

    cleaned = re.sub(r"zoete[-\s]aardappel\w*", "", ingredients_text)
    cleaned = re.sub(r"sweet[-\s]potato\w*", "", cleaned)

    words = set(_LETTER_PATTERN.findall(cleaned))

    for term in NIGHTSHADE_TERMS:
        if " " in term:
            if term in cleaned:
                return True
        else:
            if term in words:
                return True

    return False


def normalise_allergens(raw: list | None) -> list[str]:
    """Lowercase and deduplicate allergen names."""
    if not raw:
        return []
    return sorted(set(a.lower().strip() for a in raw if a and a.strip()))


def _value_unit_divisor(s: str) -> float:
    """Return 1000 if the value is in mg (divide to convert to grams), else 1."""
    return 1000.0 if _MG_PATTERN.search(s) else 1.0


def _extract_number(s: str, unit_divisor: float = 1.0) -> float | None:
    """Pull the first decimal/integer number from a string, applying unit_divisor."""
    m = _NUMBER_PATTERN.search(s.replace(",", "."))
    return float(m.group(1)) / unit_divisor if m else None


def _extract_kcal(flat_pairs: list[tuple[str, str]]) -> float | None:
    # Priority: Dutch "kcal N" format first; fall back to "N kcal" only when no kJ is on the same line.
    for _, value in flat_pairs:
        v = value.replace(",", ".")
        m = _KCAL_AFTER_PATTERN.search(v)
        if m:
            return float(m.group(1))
        if "kj" not in v.lower():
            m2 = _KCAL_BEFORE_PATTERN.search(v)
            if m2:
                return float(m2.group(1))
    return None


def _flatten_nutrition_rows(rows: list) -> list[tuple[str, str]]:
    """Flatten the API nutrition rows, merging single-element continuation tokens."""
    flat: list[tuple[str, str]] = []
    pending_label = ""
    pending_value = ""

    for row in rows:
        if len(row) == 0:
            continue
        elif len(row) == 1:
            pending_label += " " + row[0]
        else:
            if pending_label or pending_value:
                flat.append((pending_label.strip(), (pending_value or "").strip()))
            pending_label = row[0] or ""
            pending_value = row[1] or ""

    if pending_label or pending_value:
        flat.append((pending_label.strip(), (pending_value or "").strip()))

    return flat


def parse_nutritions(table: dict | None) -> tuple[dict, dict | None]:
    """Parse the nutritionsTable into a dict of known macronutrients.

    Returns (parsed_values, raw_table_or_None).
    Keys: energy_kcal, protein_g, carbs_g, fat_g, saturated_fat_g, sugar_g, salt_g.
    All values are float or None.
    """
    empty = {
        k: None
        for k in (
            "energy_kcal",
            "protein_g",
            "carbs_g",
            "fat_g",
            "saturated_fat_g",
            "sugar_g",
            "salt_g",
        )
    }
    if not table or not table.get("rows"):
        return empty, None

    flat = _flatten_nutrition_rows(table["rows"])

    parsed: dict[str, float | None] = {k: None for k in empty}
    parsed["energy_kcal"] = _extract_kcal(flat)

    for label, value in flat:
        label_l = label.lower()
        divisor = _value_unit_divisor(value)

        if any(t in label_l for t in ("eiwit", "protein")):
            parsed["protein_g"] = _extract_number(value, divisor)

        elif any(t in label_l for t in ("koolhydrat", "carbohydr")):
            if parsed["carbs_g"] is None:
                parsed["carbs_g"] = _extract_number(value, divisor)

        elif "suiker" in label_l or "sugar" in label_l:
            if parsed["sugar_g"] is None:
                parsed["sugar_g"] = _extract_number(value, divisor)

        elif "verzadigd" in label_l or "saturated" in label_l:
            parsed["saturated_fat_g"] = _extract_number(value, divisor)

        elif any(t in label_l for t in ("vet", "fat", "lipid")):
            if parsed["fat_g"] is None:
                parsed["fat_g"] = _extract_number(value, divisor)

        elif "zout" in label_l or "salt" in label_l:
            if parsed["salt_g"] is None:
                parsed["salt_g"] = _extract_number(value, divisor)

        elif "sodium" in label_l or "natrium" in label_l:
            # Convert sodium to salt: salt = sodium × 2.5
            if parsed["salt_g"] is None:
                na = _extract_number(value, divisor)
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
    all_badges = (raw.get("primaryProductBadges") or []) + (
        raw.get("secondaryProductBadges") or []
    )

    parsed_nutri, raw_table = parse_nutritions(raw.get("nutritionsTable"))

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
        "is_clean_15": int(detect_clean_15(raw.get("title", ""))),
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
        "nutritions_raw": (
            json.dumps(raw_table, ensure_ascii=False) if raw_table else None
        ),
        "categories": json.dumps(categories, ensure_ascii=False),
        "badges": json.dumps(all_badges, ensure_ascii=False),
        "is_medicine": int(bool(raw.get("isMedicine"))),
        "retail_set": int(bool(raw.get("retailSet"))),
        "image_url": raw.get("image"),
        "last_updated": timestamp,
    }
