"""
Unit tests for Bronze -> Silver transformation logic.

Tests pure Python/logic functions only — no Spark or AWS dependency.
Run with: python -m pytest transform/tests/test_bronze_to_silver.py -v
"""
import pytest
import re
import itertools


# =============================================================================
# Replicate parse_jsonstat for isolated testing (no Spark/Glue imports needed)
# =============================================================================
def parse_jsonstat(raw: dict) -> tuple:
    data = raw.get("result", raw)
    dimension_ids = data["id"]
    dimension_sizes = data["size"]
    values = data["value"]

    dims = []
    dim_labels = {}

    for dim_id in dimension_ids:
        dim_info = data["dimension"][dim_id]
        category = dim_info["category"]
        labels = category.get("label", {})
        index = category.get("index", {})

        dim_labels[dim_id] = dim_info.get("label", dim_id)

        if isinstance(index, dict) and len(index) > 0:
            codes = [k for k, v in sorted(index.items(), key=lambda x: x[1])]
        elif isinstance(index, list) and len(index) > 0:
            codes = index
        else:
            codes = list(labels.keys())

        dims.append({"id": dim_id, "codes": codes, "labels": labels})

    rows = []
    for idx, combo in enumerate(
        itertools.product(*[range(s) for s in dimension_sizes])
    ):
        if idx >= len(values):
            break
        row = {}
        for d, pos in zip(dims, combo):
            code = d["codes"][pos]
            row[d["id"]] = d["labels"].get(code, code)
        row["value"] = values[idx]
        rows.append(row)

    return rows, dim_labels


# =============================================================================
# Replicate date normalisation logic for isolated testing
# =============================================================================
def normalise_ppr_date(date_str: str) -> str:
    """
    Mirror of the Spark withColumn logic in process_ppr.
    Converts space-separated dates to dd/MM/yyyy.
    Slash-separated dates are returned unchanged.
    Returns None if input is None or empty.
    """
    if not date_str:
        return None
    date_str_stripped = date_str.strip()
    if not date_str_stripped:
        return None

    # space-separated: "1 01 2021", "01 1 2021", "1 1 2021", " 1 01 2021"
    space_match = re.match(r"^\s*(\d{1,2})\s+(\d{1,2})\s+(\d{4})\s*$", date_str)
    if space_match:
        day, month, year = space_match.groups()
        return f"{int(day):02d}/{int(month):02d}/{year}"

    # slash-separated: "02/01/2015", "3/06/2020" — return as-is
    slash_match = re.match(r"^\d{1,2}/\d{1,2}/\d{4}$", date_str_stripped)
    if slash_match:
        return date_str_stripped

    return None


def is_valid_normalised_date(normalised: str) -> bool:
    """Check a normalised date string is parseable as dd/MM/yyyy."""
    if not normalised:
        return False
    try:
        from datetime import datetime
        datetime.strptime(normalised, "%d/%m/%Y")
        return True
    except ValueError:
        return False


# ==========================
# 1. parse_jsonstat tests
# ==========================
@pytest.fixture
def minimal_jsonstat():
    """Minimal valid JSON-stat2 response mimicking CSO structure."""
    return {
        "result": {
            "id":   ["STATISTIC", "TLIST(M1)"],
            "size": [2, 2],
            "dimension": {
                "STATISTIC": {
                    "label": "Statistic",
                    "category": {
                        "index": ["HPM06C01", "HPM06C02"],
                        "label": {
                            "HPM06C01": "Residential Property Price Index",
                            "HPM06C02": "Percentage Change over 1 month"
                        }
                    }
                },
                "TLIST(M1)": {
                    "label": "Month",
                    "category": {
                        "index": {},
                        "label": {
                            "2024M01": "2024 January",
                            "2024M02": "2024 February"
                        }
                    }
                }
            },
            "value": [100.0, 102.5, 0.5, 1.2]
        }
    }


@pytest.fixture
def dict_index_jsonstat():
    """JSON-stat where index is a dict (most CSO dimensions)."""
    return {
        "id":   ["C02803V03373"],
        "size": [3],
        "dimension": {
            "C02803V03373": {
                "label": "Type of Residential Property",
                "category": {
                    "index": {"01": 0, "02": 1, "03": 2},
                    "label": {"01": "National", "02": "Dublin", "03": "Cork"}
                }
            }
        },
        "value": [150.0, 160.0, 140.0]
    }


class TestParseJsonstat:

    def test_row_count(self, minimal_jsonstat):
        rows, _ = parse_jsonstat(minimal_jsonstat)
        assert len(rows) == 4  # 2 statistics × 2 months

    def test_column_names_present(self, minimal_jsonstat):
        rows, _ = parse_jsonstat(minimal_jsonstat)
        assert "STATISTIC" in rows[0]
        assert "TLIST(M1)" in rows[0]
        assert "value" in rows[0]

    def test_values_correct(self, minimal_jsonstat):
        rows, _ = parse_jsonstat(minimal_jsonstat)
        assert [r["value"] for r in rows] == [100.0, 102.5, 0.5, 1.2]

    def test_labels_applied(self, minimal_jsonstat):
        rows, _ = parse_jsonstat(minimal_jsonstat)
        statistics = [r["STATISTIC"] for r in rows]
        assert "Residential Property Price Index" in statistics

    def test_dim_labels_returned(self, minimal_jsonstat):
        _, dim_labels = parse_jsonstat(minimal_jsonstat)
        assert dim_labels["STATISTIC"] == "Statistic"
        assert dim_labels["TLIST(M1)"] == "Month"

    def test_tlist_empty_index_uses_label_keys(self, minimal_jsonstat):
        """TLIST dimensions have empty index — should fall back to label keys."""
        rows, _ = parse_jsonstat(minimal_jsonstat)
        # TLIST(M1) values should be "2024 January" or "2024 February" (label values)
        # or "2024M01"/"2024M02" (label keys as fallback)
        period_values = [r["TLIST(M1)"] for r in rows]
        assert len(period_values) == 4
        assert all(p is not None for p in period_values)

    def test_dict_index_sorted_by_position(self, dict_index_jsonstat):
        """Dict index should be sorted by position value, not key order."""
        rows, _ = parse_jsonstat(dict_index_jsonstat)
        locations = [r["C02803V03373"] for r in rows]
        assert locations == ["National", "Dublin", "Cork"]

    def test_list_index_preserves_order(self, minimal_jsonstat):
        """List index (HPM06 STATISTIC style) should preserve order."""
        rows, _ = parse_jsonstat(minimal_jsonstat)
        first_stats = [r["STATISTIC"] for r in rows[:2]]
        assert first_stats[0] == first_stats[1] == "Residential Property Price Index"

    def test_no_result_wrapper(self):
        """Should work even without the JSON-RPC 'result' wrapper."""
        raw = {
            "id":   ["TLIST(A1)"],
            "size": [2],
            "dimension": {
                "TLIST(A1)": {
                    "label": "Year",
                    "category": {
                        "index": {},
                        "label": {"2023": "2023", "2024": "2024"}
                    }
                }
            },
            "value": [100.0, 105.0]
        }
        rows, _ = parse_jsonstat(raw)
        assert len(rows) == 2
        assert rows[0]["value"] == 100.0

    def test_total_rows_equals_product_of_sizes(self, minimal_jsonstat):
        rows, _ = parse_jsonstat(minimal_jsonstat)
        data = minimal_jsonstat["result"]
        expected = 1
        for s in data["size"]:
            expected *= s
        assert len(rows) == expected


# =================================
# 2. PPR date normalisation tests
# =================================
class TestNormalisePprDate:

    # space-separated format (2021 file)
    def test_space_single_digit_day_double_month(self):
        assert normalise_ppr_date("1 01 2021") == "01/01/2021"

    def test_space_double_digit_day(self):
        assert normalise_ppr_date("28 02 2021") == "28/02/2021"

    def test_space_single_digit_both(self):
        assert normalise_ppr_date("1 1 2021") == "01/01/2021"

    def test_space_double_digit_both(self):
        assert normalise_ppr_date("31 12 2021") == "31/12/2021"

    def test_space_leading_whitespace(self):
        assert normalise_ppr_date(" 1 01 2021") == "01/01/2021"

    def test_space_trailing_whitespace(self):
        assert normalise_ppr_date("1 01 2021 ") == "01/01/2021"

    def test_space_single_digit_month(self):
        assert normalise_ppr_date("15 3 2021") == "15/03/2021"

    # slash-separated format (2015/2016/2020 files)
    def test_slash_standard_format(self):
        assert normalise_ppr_date("02/01/2015") == "02/01/2015"

    def test_slash_single_digit_day(self):
        assert normalise_ppr_date("3/06/2020") == "3/06/2020"

    def test_slash_end_of_year(self):
        assert normalise_ppr_date("31/12/2016") == "31/12/2016"

    # null / invalid inputs
    def test_none_input(self):
        assert normalise_ppr_date(None) is None

    def test_empty_string(self):
        assert normalise_ppr_date("") is None

    def test_whitespace_only(self):
        assert normalise_ppr_date("   ") is None

    def test_year_only(self):
        assert normalise_ppr_date("2021") is None

    def test_garbage_string(self):
        assert normalise_ppr_date("not a date") is None

    def test_address_bleed(self):
        assert normalise_ppr_date("1, Main Street") is None

    # round-trip validity: normalised output must be parseable
    @pytest.mark.parametrize("raw_date", [
        "1 01 2021", "28 02 2021", "15 3 2021", "31 12 2021",
        "02/01/2015", "01/01/2016", "15/12/2020"
    ])
    def test_normalised_is_valid_date(self, raw_date):
        normalised = normalise_ppr_date(raw_date)
        assert normalised is not None
        assert is_valid_normalised_date(normalised), \
            f"'{raw_date}' normalised to '{normalised}' which is not a valid date"


# ===============================================================
# 3. Year extraction tests (mirrors Spark regexp_extract logic)
# ================================================================
class TestYearExtraction:

    def _extract_year(self, period: str) -> int:
        """Mirror of regexp_extract(col, r'(\d{4})', 1).cast(IntegerType())"""
        if not period:
            return None
        match = re.search(r"(\d{4})", period)
        if match:
            return int(match.group(1))
        return None

    def test_monthly_format(self):
        assert self._extract_year("2024M01") == 2024

    def test_annual_format(self):
        assert self._extract_year("2024") == 2024

    def test_quarterly_format(self):
        assert self._extract_year("2024Q1") == 2024

    def test_long_month_format(self):
        assert self._extract_year("2005 January") == 2005

    def test_none_input(self):
        assert self._extract_year(None) is None

    def test_no_year_in_string(self):
        assert self._extract_year("January") is None


# ===============================
# 4. RTB column cleaning tests
# ===============================
class TestRtbColumnCleaning:

    def _clean_col_name(self, col: str) -> str:
        """Mirror of RTB column standardisation logic."""
        return (col.lower().strip()
                   .replace(" ", "_")
                   .replace("(", "")
                   .replace(")", ""))

    def _is_code_column(self, col: str) -> bool:
        """Mirror of code column detection logic."""
        return col.startswith("C0") and "V" in col

    def test_statistic_label_cleaned(self):
        assert self._clean_col_name("Statistic Label") == "statistic_label"

    def test_number_of_bedrooms_cleaned(self):
        assert self._clean_col_name("Number of Bedrooms") == "number_of_bedrooms"

    def test_value_cleaned(self):
        assert self._clean_col_name("VALUE") == "value"

    def test_property_type_cleaned(self):
        assert self._clean_col_name("Property Type") == "property_type"

    def test_code_column_detected(self):
        assert self._is_code_column("C02970V03592") is True
        assert self._is_code_column("C02969V03591") is True

    def test_label_column_not_detected_as_code(self):
        assert self._is_code_column("Number of Bedrooms") is False
        assert self._is_code_column("Location") is False
        assert self._is_code_column("STATISTIC") is False