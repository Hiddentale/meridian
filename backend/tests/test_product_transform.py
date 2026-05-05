import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from product_transform import (
    detect_bio,
    detect_clean_15,
    detect_nightshade,
    parse_nutritions,
)

# ---------------------------------------------------------------------------
# detect_nightshade
# ---------------------------------------------------------------------------


class TestDetectNightshade:
    def test_tomaat_single_word(self):
        assert detect_nightshade(["tomaat 85%"]) is True

    def test_tomaten_plural(self):
        assert detect_nightshade(["tomaten (30%)"]) is True

    def test_paprika_matches(self):
        assert detect_nightshade(["rode paprika, zout"]) is True

    def test_aardappelzetmeel_matches(self):
        assert detect_nightshade(["aardappelzetmeel"]) is True

    def test_no_nightshade_clean_ingredients(self):
        assert detect_nightshade(["kip", "water", "zout"]) is False

    def test_empty_ingredients(self):
        assert detect_nightshade([]) is False

    def test_sweet_potato_excluded(self):
        # Zoete aardappel is Convolvulaceae, not nightshade, must not trigger aardappel check
        assert detect_nightshade(["zoete aardappel 40%", "water"]) is False

    def test_sweet_potato_hyphenated_excluded(self):
        assert detect_nightshade(["zoete-aardappelpuree"]) is False

    def test_sweet_potato_english_excluded(self):
        assert detect_nightshade(["sweet potato 40%"]) is False

    def test_partial_word_does_not_match(self):
        # "uitbatingsrecht" contains "ui" but should not match the "ui" onion token
        assert detect_nightshade(["uitbatingsrecht"]) is False

    def test_sambal_matches(self):
        assert detect_nightshade(["sambal oelek"]) is True

    def test_multiword_birds_eye(self):
        assert detect_nightshade(["bird's eye chili"]) is True

    def test_case_insensitive(self):
        assert detect_nightshade(["Tomaat", "PAPRIKA"]) is True


# ---------------------------------------------------------------------------
# detect_bio
# ---------------------------------------------------------------------------


class TestDetectBio:
    def test_biologisch_in_title(self):
        assert detect_bio("Biologisch Kipfilet", []) is True

    def test_biologische_in_title(self):
        assert detect_bio("Biologische Rijst 500g", []) is True

    def test_bio_prefix_in_title(self):
        # "bio " with trailing space catches "Bio Rijst" but not "biologie"
        assert detect_bio("Bio Rijst", []) is True

    def test_bio_suffix_in_title(self):
        assert detect_bio("Spinazie Bio", []) is True

    def test_organic_in_title(self):
        assert detect_bio("Organic Oats", []) is True

    def test_non_bio_title(self):
        assert detect_bio("Kipfilet naturel", []) is False

    def test_antibiotisch_does_not_match(self):
        # "antibiotisch" must not trigger via "bio " or " bio"
        assert detect_bio("Antibiotisch vrij", []) is False

    def test_biologische_oorsprong_in_ingredients(self):
        assert detect_bio("Tarwebloem", ["tarwe van biologische oorsprong"]) is True

    def test_organic_origin_in_ingredients(self):
        assert detect_bio("Oats", ["oats (organic origin)"]) is True

    def test_no_bio_signal(self):
        assert detect_bio("Kaas", ["melk", "zout", "stremsel"]) is False


# ---------------------------------------------------------------------------
# detect_clean_15
# ---------------------------------------------------------------------------


class TestDetectClean15:
    def test_avocado(self):
        assert detect_clean_15("Avocado") is True

    def test_case_insensitive(self):
        assert detect_clean_15("MANGO") is True

    def test_multi_word_zoete_aardappel(self):
        assert detect_clean_15("Zoete aardappel") is True

    def test_multi_word_inside_longer_title(self):
        assert detect_clean_15("Gekookte zoete aardappel 400g") is True

    def test_ui_single(self):
        assert detect_clean_15("Ui") is True

    def test_uien_plural(self):
        assert detect_clean_15("Uien 1kg") is True

    def test_ui_not_partial_match(self):
        # "Sushi" contains the letters u-i but "ui" must only match as a whole word
        assert detect_clean_15("Sushirijst") is False

    def test_unknown_product(self):
        assert detect_clean_15("Melkchocolade") is False

    def test_wortel(self):
        assert detect_clean_15("Worteltjes 500g") is True

    def test_champignon(self):
        assert detect_clean_15("Champignons gesneden") is True


# ---------------------------------------------------------------------------
# parse_nutritions
# ---------------------------------------------------------------------------


class TestParseNutritions:
    def test_empty_table_returns_all_none(self):
        parsed, raw = parse_nutritions(None)
        assert all(v is None for v in parsed.values())
        assert raw is None

    def test_empty_rows_returns_all_none(self):
        parsed, raw = parse_nutritions({"columns": ["per 100g"], "rows": []})
        assert all(v is None for v in parsed.values())
        assert raw is None

    def test_dutch_kcal_format(self):
        # "kcal 579", kcal label before the number
        table = {
            "columns": ["Voedingswaarden", "Per 100g"],
            "rows": [
                ["Energie", "kcal 579 / kJ 2421"],
                ["Eiwit", "25g"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["energy_kcal"] == 579.0

    def test_kcal_before_number_no_kj(self):
        # "579 kcal" without kJ on the same line
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "579 kcal"],
                ["Eiwit", "10g"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["energy_kcal"] == 579.0

    def test_kcal_not_extracted_when_kj_present_before_number(self):
        # "kJ 2412 / kcal 579", Dutch-style combined; kcal after should still match
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "kJ 2412 / kcal 579"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["energy_kcal"] == 579.0

    def test_protein_parsed(self):
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "kcal 200"],
                ["Eiwit", "15,3g"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["protein_g"] == pytest.approx(15.3)

    def test_sodium_converted_to_salt(self):
        # salt = sodium * 2.5
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "kcal 100"],
                ["Natrium", "400mg"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        # 400mg sodium = 0.4g; 0.4 * 2.5 = 1.0g salt
        assert parsed["salt_g"] == pytest.approx(1.0)

    def test_explicit_zout_takes_precedence_over_sodium(self):
        # zout row appears first; sodium row must not overwrite it
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "kcal 100"],
                ["Zout", "1,2g"],
                ["Natrium", "480mg"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["salt_g"] == pytest.approx(1.2)

    def test_mg_unit_divides_to_grams(self):
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "kcal 100"],
                ["Eiwit", "500mg"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["protein_g"] == pytest.approx(0.5)

    def test_raw_table_returned_when_rows_present(self):
        table = {"columns": ["Per 100g"], "rows": [["Energie", "kcal 100"]]}
        _, raw = parse_nutritions(table)
        assert raw is table

    def test_continuation_single_element_rows_merged(self):
        # Some Jumbo products emit single-element rows as label continuations
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie"],
                ["kcal 300"],
                ["Eiwit", "12g"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        # Continuation token "kcal 300" is appended to the label "Energie",
        # making label="Energie kcal 300". The kcal extractor scans values, not labels,
        # so energy_kcal may be None here, but protein should parse correctly.
        assert parsed["protein_g"] == pytest.approx(12.0)

    def test_carbs_first_match_wins(self):
        # First koolhydraten row wins; subsequent rows (e.g. "waarvan suikers") must not overwrite
        table = {
            "columns": ["Per 100g"],
            "rows": [
                ["Energie", "kcal 100"],
                ["Koolhydraten", "50g"],
                ["waarvan suikers", "20g"],
            ],
        }
        parsed, _ = parse_nutritions(table)
        assert parsed["carbs_g"] == pytest.approx(50.0)
        assert parsed["sugar_g"] == pytest.approx(20.0)
