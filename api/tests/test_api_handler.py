"""
Unit tests for the API Lambda handler.

Tests pure logic only — no AWS, Athena, or network calls.
Mirrors the logic in api/lambda/api_handler.py.

Run with: python -m pytest api/tests/test_api_handler.py -v
"""
import json


# ===============================================================
# Replicate pure logic from api_handler.py for isolated testing
# ===============================================================

CORS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
}

def ok(data, meta=None):
    body = {"status": "ok", "data": data}
    if meta:
        body["meta"] = meta
    return {
        "statusCode": 200,
        "headers": CORS,
        "body": json.dumps(body, default=str)
    }

def error(status_code, message):
    return {
        "statusCode": status_code,
        "headers": CORS,
        "body": json.dumps({"status": "error", "message": message})
    }

def coerce_numbers(rows):
    coerced = []
    for row in rows:
        new_row = {}
        for k, v in row.items():
            if v is None or v == "":
                new_row[k] = None
                continue
            try:
                if "." not in v:
                    new_row[k] = int(v)
                else:
                    new_row[k] = float(v)
            except (ValueError, TypeError):
                new_row[k] = v
        coerced.append(new_row)
    return coerced

def sanitise_string_param(value):
    """Strip quotes and whitespace from a string parameter."""
    if value is None:
        return None
    return value.strip().strip('"\'')

def sanitise_year_param(value, default):
    """Strip quotes/whitespace and convert to int."""
    cleaned = sanitise_string_param(value) if value else default
    return int(cleaned)

def sanitise_location(location):
    """Allow only alphanumeric, spaces, commas, hyphens."""
    if not location:
        return None
    return "".join(c for c in location if c.isalnum() or c in " ,-")

def get_route_key(event):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    path   = event.get("rawPath", "/")
    return f"{method} {path}"

def get_params(event):
    return event.get("queryStringParameters") or {}

ROUTES = {
    "GET /health",
    "GET /summary",
    "GET /rents",
    "GET /prices",
    "GET /sales",
}

def is_valid_route(route_key):
    return route_key in ROUTES


# ==============================
# 1. Response formatting tests
# ==============================
class TestResponseFormatting:

    def test_ok_response_structure(self):
        response = ok([{"year": 2024, "value": 100}])
        assert response["statusCode"] == 200
        assert "Access-Control-Allow-Origin" in response["headers"]
        body = json.loads(response["body"])
        assert body["status"] == "ok"
        assert body["data"] == [{"year": 2024, "value": 100}]

    def test_ok_response_with_meta(self):
        response = ok([], meta={"row_count": 0, "table": "gold_summary"})
        body = json.loads(response["body"])
        assert body["meta"]["row_count"] == 0
        assert body["meta"]["table"] == "gold_summary"

    def test_error_response_structure(self):
        response = error(404, "Route not found")
        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert body["status"] == "error"
        assert body["message"] == "Route not found"

    def test_error_400(self):
        response = error(400, "Invalid parameter")
        assert response["statusCode"] == 400

    def test_error_500(self):
        response = error(500, "Internal server error")
        assert response["statusCode"] == 500

    def test_error_504(self):
        response = error(504, "Query timed out")
        assert response["statusCode"] == 504

    def test_cors_headers_on_ok(self):
        response = ok([])
        assert response["headers"]["Access-Control-Allow-Origin"] == "*"
        assert "GET" in response["headers"]["Access-Control-Allow-Methods"]

    def test_cors_headers_on_error(self):
        response = error(400, "bad")
        assert response["headers"]["Access-Control-Allow-Origin"] == "*"

    def test_empty_data_list(self):
        response = ok([])
        body = json.loads(response["body"])
        assert body["data"] == []
        assert body["status"] == "ok"


# =================================================================
# 2. Number coercion tests (Athena returns all values as strings)
# =================================================================
class TestCoerceNumbers:

    def test_integer_string_becomes_int(self):
        rows = [{"year": "2024", "count": "150"}]
        result = coerce_numbers(rows)
        assert result[0]["year"] == 2024
        assert isinstance(result[0]["year"], int)

    def test_float_string_becomes_float(self):
        rows = [{"price": "356275.5", "pct": "7.8"}]
        result = coerce_numbers(rows)
        assert result[0]["price"] == 356275.5
        assert isinstance(result[0]["price"], float)

    def test_null_string_stays_none(self):
        rows = [{"value": None}]
        result = coerce_numbers(rows)
        assert result[0]["value"] is None

    def test_empty_string_becomes_none(self):
        rows = [{"value": ""}]
        result = coerce_numbers(rows)
        assert result[0]["value"] is None

    def test_text_stays_string(self):
        rows = [{"location": "Dublin", "type": "houses"}]
        result = coerce_numbers(rows)
        assert result[0]["location"] == "Dublin"
        assert isinstance(result[0]["location"], str)

    def test_negative_number(self):
        rows = [{"change": "-18.2"}]
        result = coerce_numbers(rows)
        assert result[0]["change"] == -18.2

    def test_zero(self):
        rows = [{"pct": "0"}]
        result = coerce_numbers(rows)
        assert result[0]["pct"] == 0
        assert isinstance(result[0]["pct"], int)

    def test_multiple_rows(self):
        rows = [
            {"year": "2020", "price": "321459.0"},
            {"year": "2021", "price": "341278.0"},
        ]
        result = coerce_numbers(rows)
        assert result[0]["year"] == 2020
        assert result[1]["year"] == 2021
        assert result[0]["price"] == 321459.0

    def test_real_athena_summary_row(self):
        """Simulates a real Athena response row from gold_housing_crisis_summary."""
        row = {
            "year": "2021",
            "national_price_index": "125.5",
            "national_price_yoy_pct": "14.2",
            "avg_national_rent": "1261.21",
            "avg_dublin_rent": "1788.16",
            "dublin_total_sales": "54856",
            "dublin_avg_sale_price": "341278.0",
            "gold_updated": "2026-06-14",
        }
        result = coerce_numbers([row])[0]
        assert result["year"] == 2021
        assert result["national_price_index"] == 125.5
        assert result["dublin_total_sales"] == 54856
        assert isinstance(result["dublin_total_sales"], int)
        assert result["gold_updated"] == "2026-06-14"  # date stays string


# ==================================
# 3. Parameter sanitisation tests
# ==================================
class TestParameterSanitisation:

    def test_year_strips_trailing_quote(self):
        assert sanitise_year_param('2016"', "2016") == 2016

    def test_year_strips_leading_quote(self):
        assert sanitise_year_param('"2016', "2016") == 2016

    def test_year_strips_both_quotes(self):
        assert sanitise_year_param('"2016"', "2016") == 2016

    def test_year_strips_whitespace(self):
        assert sanitise_year_param(" 2016 ", "2016") == 2016

    def test_year_uses_default_when_none(self):
        assert sanitise_year_param(None, "2016") == 2016

    def test_year_valid_value(self):
        assert sanitise_year_param("2024", "2016") == 2024

    def test_location_allows_letters(self):
        assert sanitise_location("Dublin") == "Dublin"

    def test_location_allows_spaces(self):
        assert sanitise_location("South Dublin") == "South Dublin"

    def test_location_allows_commas(self):
        assert sanitise_location("Carlow, Ireland") == "Carlow, Ireland"

    def test_location_allows_hyphens(self):
        assert sanitise_location("Dun-Laoghaire") == "Dun-Laoghaire"

    def test_location_strips_sql_injection(self):
        """SQL injection attempt should be stripped to safe chars only."""
        malicious = "Dublin'; DROP TABLE users; --"
        result = sanitise_location(malicious)
        assert "DROP" not in result
        assert ";" not in result
        assert "'" not in result
        assert "--" not in result

    def test_location_strips_quotes(self):
        result = sanitise_location('Dublin"')
        assert '"' not in result

    def test_location_none_returns_none(self):
        assert sanitise_location(None) is None

    def test_location_empty_returns_none(self):
        assert sanitise_location("") is None


# =============================
# 4. Route resolution tests
# =============================
class TestRouteResolution:

    def test_get_health_resolves(self):
        assert is_valid_route("GET /health")

    def test_get_summary_resolves(self):
        assert is_valid_route("GET /summary")

    def test_get_rents_resolves(self):
        assert is_valid_route("GET /rents")

    def test_get_prices_resolves(self):
        assert is_valid_route("GET /prices")

    def test_get_sales_resolves(self):
        assert is_valid_route("GET /sales")

    def test_unknown_route_invalid(self):
        assert not is_valid_route("GET /unknown")

    def test_post_method_invalid(self):
        assert not is_valid_route("POST /summary")

    def test_delete_method_invalid(self):
        assert not is_valid_route("DELETE /rents")

    def test_five_routes_total(self):
        assert len(ROUTES) == 5

    def test_route_key_from_event(self):
        event = {
            "rawPath": "/rents",
            "requestContext": {
                "http": {"method": "GET"}
            }
        }
        assert get_route_key(event) == "GET /rents"

    def test_route_key_health(self):
        event = {
            "rawPath": "/health",
            "requestContext": {
                "http": {"method": "GET"}
            }
        }
        assert get_route_key(event) == "GET /health"

    def test_params_extracted_from_event(self):
        event = {
            "queryStringParameters": {
                "location": "Dublin",
                "from_year": "2016"
            }
        }
        params = get_params(event)
        assert params["location"] == "Dublin"
        assert params["from_year"] == "2016"

    def test_params_empty_when_none(self):
        event = {"queryStringParameters": None}
        assert get_params(event) == {}

    def test_params_empty_when_missing(self):
        event = {}
        assert get_params(event) == {}


# ==========================================
# 5. Integration: full request simulation
# ==========================================
class TestRequestSimulation:

    def test_health_event_structure(self):
        """Simulate a GET /health request event from API Gateway."""
        event = {
            "version": "2.0",
            "routeKey": "GET /health",
            "rawPath": "/health",
            "rawQueryString": "",
            "requestContext": {
                "http": {"method": "GET", "path": "/health"}
            },
            "queryStringParameters": None,
        }
        route = get_route_key(event)
        params = get_params(event)
        assert route == "GET /health"
        assert params == {}
        assert is_valid_route(route)

    def test_rents_event_with_params(self):
        event = {
            "rawPath": "/rents",
            "requestContext": {
                "http": {"method": "GET"}
            },
            "queryStringParameters": {
                "location": "Dublin", 
                "from_year": "2016"
            },
        }
        route  = get_route_key(event)
        params = get_params(event)
        assert route == "GET /rents"
        assert sanitise_location(params.get("location")) == "Dublin"
        assert sanitise_year_param(params.get("from_year"), "2016") == 2016

    def test_unknown_route_returns_404(self):
        response = error(404, "Route not found: GET /unknown")
        assert response["statusCode"] == 404
        body = json.loads(response["body"])
        assert "Route not found" in body["message"]

    def test_coercion_on_real_rents_response(self):
        """Simulate Athena returning rent data as strings, then coercing."""
        athena_rows = [
            {
                "location": "Dublin", 
                "year": "2016", 
                "avg_monthly_rent": "1406.27", 
                "yoy_change_pct": ""
            },
            {
                "location": "Dublin", 
                "year": "2017", 
                "avg_monthly_rent": "1516.98", 
                "yoy_change_pct": "7.88"
            },
        ]
        result = coerce_numbers(athena_rows)
        assert result[0]["avg_monthly_rent"] == 1406.27
        assert result[0]["yoy_change_pct"] is None   # empty string -> None
        assert result[1]["yoy_change_pct"] == 7.88
        assert result[1]["year"] == 2017

    def test_summary_response_wraps_correctly(self):
        data = [
            {
                "year": 2024, 
                "national_price_index": 153.1
            }
        ]
        meta = {
            "table": "gold_housing_crisis_summary", 
            "row_count": 1
        }
        response = ok(data, meta=meta)
        body = json.loads(response["body"])
        assert body["data"][0]["national_price_index"] == 153.1
        assert body["meta"]["row_count"] == 1
        assert response["statusCode"] == 200