"""
Unit tests for Silver -> Gold transformation logic.

Tests pure Python logic only — no Spark, Glue, or AWS dependency.
Mirrors the transformation functions in silver_to_gold.py.

Run with: python -m pytest transform/tests/test_silver_to_gold.py -v
"""
import re
from datetime import datetime


# =========================================================================
# Replicate pure-Python logic from silver_to_gold.py for isolated testing
# =========================================================================

# --- Month name -> number map (mirrors build_price_index) ---
MONTH_MAP = {
    "January": 1,  
    "February": 2,  
    "March": 3,    
    "April": 4,
    "May": 5,      
    "June": 6,      
    "July": 7,      
    "August": 8,
    "September": 9,
    "October": 10,  
    "November": 11, 
    "December": 12
}

def extract_month_name(period: str) -> str:
    """Extract month name from '2005 January' -> 'January'"""
    if not period:
        return None
    m = re.match(r"^\d{4}\s+(.+)$", period.strip())
    return m.group(1).strip() if m else None

def period_to_month(period: str) -> int:
    """Convert '2005 January' -> 1"""
    name = extract_month_name(period)
    return MONTH_MAP.get(name) if name else None

def period_to_year(period: str) -> int:
    """Extract year from '2005 January' -> 2005"""
    if not period:
        return None
    m = re.match(r"^(\d{4})", period.strip())
    return int(m.group(1)) if m else None


# --- Statistic -> column name map (mirrors pivot in build_price_index) ---
STAT_MAP = {
    "Residential Property Price Index": "price_index",
    "Percentage Change over 1 month for Residential Property Price Index": "pct_change_1m",
    "Percentage Change over 3 months for Residential Property Price Index": "pct_change_3m",
    "Percentage Change over 12 months for Residential Property Price Index": "pct_change_12m",
}

def map_statistic(statistic: str) -> str:
    return STAT_MAP.get(statistic)


# --- year-on-year change (mirrors LAG window function) ---
def compute_yoy_change_pct(current: float, previous: float) -> float:
    """Compute year-on-year percentage change. Returns None if previous is None or 0."""
    if previous is None or previous == 0:
        return None
    return round((current - previous) / previous * 100, 2)


# --- sale type classification (mirrors build_ppr_sales) ---
def classify_sale_type(property_description: str) -> str:
    """Classify a PPR property description as New or Second-Hand."""
    if not property_description:
        return "Second-Hand"
    return "New" if "new" in property_description.lower() else "Second-Hand"


# --- year spine logic (mirrors build_crisis_summary) ---
def build_year_spine(*year_lists) -> list:
    """
    Build sorted list of all unique years across multiple sources.
    Mirrors the union().distinct().orderBy() pattern in build_crisis_summary.
    """
    all_years = set()
    for years in year_lists:
        all_years.update(years)
    return sorted(all_years)


# --- LEFT JOIN simulation (mirrors Spark left join in build_crisis_summary) ---
def left_join_on_year(spine: list, *source_dicts) -> list:
    """
    Simulate a LEFT JOIN on year.
    spine: sorted list of years
    source_dicts: list of {year: {col: val}} dicts
    Returns: list of merged row dicts, one per year in spine
    """
    rows = []
    for year in spine:
        row = {"year": year}
        for source in source_dicts:
            if year in source:
                row.update(source[year])
        rows.append(row)
    return rows


# --- key property type filter (mirrors build_price_index filter) ---
KEY_PROPERTY_TYPES = {
    "National - all residential properties",
    "National - houses",
    "National - apartments",
    "Dublin - all residential properties",
    "Dublin - houses",
    "Dublin - apartments",
}

def is_key_property_type(prop_type: str) -> bool:
    return prop_type in KEY_PROPERTY_TYPES


# ===============================================
# 1. Period parsing tests (build_price_index)
# ===============================================
class TestPeriodParsing:

    def test_month_january(self):
        assert period_to_month("2005 January") == 1

    def test_month_december(self):
        assert period_to_month("2024 December") == 12

    def test_month_june(self):
        assert period_to_month("2019 June") == 6

    def test_month_september(self):
        assert period_to_month("2010 September") == 9

    def test_year_extraction(self):
        assert period_to_year("2005 January") == 2005

    def test_year_extraction_recent(self):
        assert period_to_year("2025 March") == 2025

    def test_all_twelve_months(self):
        months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
        for i, month in enumerate(months, 1):
            assert period_to_month(f"2020 {month}") == i, f"Failed for {month}"

    def test_none_period(self):
        assert period_to_month(None) is None
        assert period_to_year(None) is None

    def test_annual_period_no_month(self):
        """BHA04 uses plain year '2024' — month extraction returns None."""
        assert period_to_month("2024") is None
        assert period_to_year("2024") == 2024

    def test_month_name_extracted(self):
        assert extract_month_name("2005 January") == "January"
        assert extract_month_name("2024 December") == "December"


# ========================================================
# 2. Statistic mapping tests (build_price_index pivot)
# ========================================================
class TestStatisticMapping:

    def test_price_index_maps_correctly(self):
        assert map_statistic("Residential Property Price Index") == "price_index"

    def test_pct_1m_maps_correctly(self):
        stat = "Percentage Change over 1 month for Residential Property Price Index"
        assert map_statistic(stat) == "pct_change_1m"

    def test_pct_3m_maps_correctly(self):
        stat = "Percentage Change over 3 months for Residential Property Price Index"
        assert map_statistic(stat) == "pct_change_3m"

    def test_pct_12m_maps_correctly(self):
        stat = "Percentage Change over 12 months for Residential Property Price Index"
        assert map_statistic(stat) == "pct_change_12m"

    def test_all_four_statistics_covered(self):
        """Every statistic in HPM06 Silver must map to a column name."""
        assert len(STAT_MAP) == 4
        assert set(STAT_MAP.values()) == {
            "price_index", "pct_change_1m", "pct_change_3m", "pct_change_12m"
        }

    def test_unknown_statistic_returns_none(self):
        assert map_statistic("Unknown statistic") is None


# =======================================================
# 3. Year-on-year change tests (window function logic)
# =======================================================
class TestYoyChange:

    def test_simple_increase(self):
        assert compute_yoy_change_pct(1100.0, 1000.0) == 10.0

    def test_simple_decrease(self):
        assert compute_yoy_change_pct(900.0, 1000.0) == -10.0

    def test_no_previous_year(self):
        """First year in series has no previous — should return None."""
        assert compute_yoy_change_pct(1000.0, None) is None

    def test_zero_previous_prevents_division(self):
        assert compute_yoy_change_pct(100.0, 0.0) is None

    def test_rounded_to_two_decimals(self):
        result = compute_yoy_change_pct(1516.98, 1406.27)
        assert result == round((1516.98 - 1406.27) / 1406.27 * 100, 2)

    def test_real_dublin_rent_2016_to_2017(self):
        """Dublin avg rent: 1406.27 (2016) -> 1516.98 (2017)"""
        result = compute_yoy_change_pct(1516.98, 1406.27)
        assert 7.0 < result < 8.5

    def test_real_price_crash_2008_to_2009(self):
        """National price index: 111.9 (2008) -> 91.5 (2009) — should be negative"""
        result = compute_yoy_change_pct(91.5, 111.9)
        assert result < 0
        assert abs(result) > 15  # was an 18% drop

    def test_no_change(self):
        assert compute_yoy_change_pct(1000.0, 1000.0) == 0.0


# ======================================================
# 4. Sale type classification tests (build_ppr_sales)
# ======================================================
class TestSaleTypeClassification:

    def test_new_dwelling(self):
        assert classify_sale_type("New Dwelling house /Apartment") == "New"

    def test_second_hand_dwelling(self):
        assert classify_sale_type("Second-Hand Dwelling house /Apartment") == "Second-Hand"

    def test_case_insensitive(self):
        assert classify_sale_type("NEW DWELLING HOUSE") == "New"
        assert classify_sale_type("new dwelling house") == "New"

    def test_none_defaults_to_second_hand(self):
        assert classify_sale_type(None) == "Second-Hand"

    def test_empty_defaults_to_second_hand(self):
        assert classify_sale_type("") == "Second-Hand"

    def test_ambiguous_defaults_to_second_hand(self):
        assert classify_sale_type("Unknown property type") == "Second-Hand"


# ====================================================
# 5. Property type filter tests (build_price_index)
# ====================================================
class TestPropertyTypeFilter:

    def test_national_all_included(self):
        assert is_key_property_type("National - all residential properties")

    def test_dublin_all_included(self):
        assert is_key_property_type("Dublin - all residential properties")

    def test_national_houses_included(self):
        assert is_key_property_type("National - houses")

    def test_national_apartments_included(self):
        assert is_key_property_type("National - apartments")

    def test_dublin_houses_included(self):
        assert is_key_property_type("Dublin - houses")

    def test_dublin_apartments_included(self):
        assert is_key_property_type("Dublin - apartments")

    def test_six_key_types_total(self):
        assert len(KEY_PROPERTY_TYPES) == 6

    def test_sub_regional_excluded(self):
        """Fingal, South Dublin etc are too granular for Gold."""
        assert not is_key_property_type("Fingal - houses")
        assert not is_key_property_type("South Dublin - houses")
        assert not is_key_property_type("Dun Laoghaire-Rathdown - houses")
        assert not is_key_property_type("Border excluding Louth - houses")


# ===========================================================
# 6. Year spine and LEFT JOIN tests (build_crisis_summary)
# ===========================================================
class TestYearSpineAndJoin:

    def test_spine_merges_all_years(self):
        price_years = [2005, 2006, 2007]
        rent_years = [2008, 2009]
        ppr_years = [2015, 2016]
        spine = build_year_spine(price_years, rent_years, ppr_years)
        assert spine == [2005, 2006, 2007, 2008, 2009, 2015, 2016]

    def test_spine_deduplicates(self):
        spine = build_year_spine([2015, 2016, 2020], [2016, 2020, 2021])
        assert spine == [2015, 2016, 2020, 2021]
        assert len(spine) == 4

    def test_spine_sorted(self):
        spine = build_year_spine([2021, 2015, 2020], [2008])
        assert spine == sorted(spine)

    def test_left_join_fills_nulls_for_missing_years(self):
        """
        If rent data starts in 2008 but spine starts in 2005,
        years 2005-2007 should have null rent values.
        """
        spine = [2005, 2006, 2007, 2008]
        rent_data = {2008: {"avg_national_rent": 973.19}}
        rows = left_join_on_year(spine, rent_data)

        assert rows[0]["year"] == 2005
        assert "avg_national_rent" not in rows[0]  # null — not in rent_data

        assert rows[3]["year"] == 2008
        assert rows[3]["avg_national_rent"] == 973.19

    def test_left_join_all_years_present(self):
        spine = [2015, 2016, 2020, 2021]
        ppr_data = {
            2015: {"avg_price": 356275.0},
            2016: {"avg_price": 411795.0},
            2020: {"avg_price": 321459.0},
            2021: {"avg_price": 341278.0},
        }
        rows = left_join_on_year(spine, ppr_data)
        assert len(rows) == 4
        assert all("avg_price" in r for r in rows)

    def test_left_join_multiple_sources(self):
        spine = [2015, 2016]
        price_data = {2015: {"price_index": 82.5}, 2016: {"price_index": 89.9}}
        rent_data  = {2016: {"avg_rent": 977.16}}  # rent starts 2016

        rows = left_join_on_year(spine, price_data, rent_data)

        assert rows[0]["year"] == 2015
        assert rows[0]["price_index"] == 82.5
        assert "avg_rent" not in rows[0]   # rent is null for 2015

        assert rows[1]["year"] == 2016
        assert rows[1]["price_index"] == 89.9
        assert rows[1]["avg_rent"] == 977.16

    def test_real_crisis_summary_spine(self):
        """
        Mirrors actual data: BHA04 from 2001, HPM06 from 2005,
        RTB from 2008, PPR from 2015. Spine should start at 2001.
        """
        bha04_years = list(range(2001, 2025))
        hpm06_years = list(range(2005, 2026))
        rtb_years   = list(range(2008, 2025))
        ppr_years   = [2015, 2016, 2020, 2021]

        spine = build_year_spine(bha04_years, hpm06_years, rtb_years, ppr_years)
        assert spine[0] == 2001
        assert spine[-1] == 2025
        assert len(spine) == 25  # 2001 through 2025


# ===========================================
# 7. Integration: full pipeline logic test
# ===========================================
class TestPipelineIntegration:

    def test_price_index_pipeline(self):
        """
        Simulate a single HPM06 Silver row going through the Gold pipeline:
        - statistic maps to column name
        - period parses to year and month
        - key property type filter passes
        """
        row = {
            "type_of_residential_property": "National - all residential properties",
            "statistic": "Residential Property Price Index",
            "period": "2024 December",
            "metric_value": 163.6,
        }
        assert is_key_property_type(row["type_of_residential_property"])
        assert map_statistic(row["statistic"]) == "price_index"
        assert period_to_year(row["period"]) == 2024
        assert period_to_month(row["period"]) == 12

    def test_rent_yoy_chain(self):
        """
        Simulate RTB rent rows for one location across three years,
        computing YoY change for each.
        """
        rents = [
            {"year": 2016, "avg_monthly_rent": 1406.27},
            {"year": 2017, "avg_monthly_rent": 1516.98},
            {"year": 2018, "avg_monthly_rent": 1638.98},
        ]
        changes = [None]  # first year has no previous
        for i in range(1, len(rents)):
            pct = compute_yoy_change_pct(
                rents[i]["avg_monthly_rent"],
                rents[i-1]["avg_monthly_rent"]
            )
            changes.append(pct)

        assert changes[0] is None
        assert 7.0 < changes[1] < 8.5   # 2016->2017: ~7.9%
        assert 7.0 < changes[2] < 8.5   # 2017->2018: ~8.0%

    def test_ppr_sale_type_aggregation(self):
        """
        Simulate PPR rows being classified and aggregated by sale type.
        """
        sales = [
            {"price_eur": 350000, "property_description": "New Dwelling house /Apartment"},
            {"price_eur": 280000, "property_description": "Second-Hand Dwelling house /Apartment"},
            {"price_eur": 420000, "property_description": "New Dwelling house /Apartment"},
            {"price_eur": 310000, "property_description": "Second-Hand Dwelling house /Apartment"},
        ]
        new_sales = [s for s in sales if classify_sale_type(s["property_description"]) == "New"]
        sh_sales  = [s for s in sales if classify_sale_type(s["property_description"]) == "Second-Hand"]

        assert len(new_sales) == 2
        assert len(sh_sales) == 2
        assert sum(s["price_eur"] for s in new_sales) / len(new_sales) == 385000.0

    def test_crisis_summary_null_handling(self):
        """
        Years before RTB data (pre-2008) should have null rent values
        in the crisis summary — verified through the left join logic.
        """
        spine = [2005, 2006, 2007, 2008]
        rent_source = {
            2008: {"avg_national_rent": 973.19, "avg_dublin_rent": 1317.73}
        }
        price_source = {
            2005: {"national_price_index": 112.6},
            2006: {"national_price_index": 128.6},
            2007: {"national_price_index": 129.2},
            2008: {"national_price_index": 111.9},
        }
        rows = left_join_on_year(spine, price_source, rent_source)

        # 2005-2007: price present, rent absent
        for row in rows[:3]:
            assert "national_price_index" in row
            assert "avg_national_rent" not in row

        # 2008: both present
        assert rows[3]["national_price_index"] == 111.9
        assert rows[3]["avg_national_rent"] == 973.19