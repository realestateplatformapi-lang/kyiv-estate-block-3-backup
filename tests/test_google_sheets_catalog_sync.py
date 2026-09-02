import importlib.util
import json
import sys
import types
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path


try:
    import gspread  # noqa: F401
    import psycopg2  # noqa: F401
except ModuleNotFoundError:
    class FakeJson:
        def __init__(self, adapted, dumps=None):
            self.adapted = adapted
            self.dumps = dumps or json.dumps

    gspread_module = types.ModuleType("gspread")
    psycopg2_module = types.ModuleType("psycopg2")
    extras_module = types.ModuleType("psycopg2.extras")
    extras_module.Json = FakeJson
    extras_module.execute_values = None
    psycopg2_module.extras = extras_module
    google_module = types.ModuleType("google")
    oauth2_module = types.ModuleType("google.oauth2")
    service_account_module = types.ModuleType("google.oauth2.service_account")
    service_account_module.Credentials = object
    sys.modules.update({
        "gspread": gspread_module,
        "psycopg2": psycopg2_module,
        "psycopg2.extras": extras_module,
        "google": google_module,
        "google.oauth2": oauth2_module,
        "google.oauth2.service_account": service_account_module,
    })


SCRIPT = Path(__file__).parents[1] / "parser_v2" / "scripts" / "sync_google_sheets_catalog.py"
SPEC = importlib.util.spec_from_file_location("sync_google_sheets_catalog", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class FakeWorksheet:
    row_count = 2501

    def __init__(self):
        self.calls = []

    def get(self, range_name, **kwargs):
        self.calls.append((range_name, kwargs))
        return [[range_name, "ext"]]


class SheetsCatalogSyncTests(unittest.TestCase):
    def test_column_names_cover_wide_commercial_sheet(self):
        self.assertEqual(MODULE.column_name(1), "A")
        self.assertEqual(MODULE.column_name(26), "Z")
        self.assertEqual(MODULE.column_name(27), "AA")
        self.assertEqual(MODULE.column_name(56), "BD")

    def test_worksheet_is_read_in_bounded_ranges(self):
        worksheet = FakeWorksheet()
        batches = list(MODULE.worksheet_batches(worksheet, ["ID", "Ext ID"], 1000))
        self.assertEqual([item[0] for item in batches], [2, 1002, 2002])
        self.assertEqual([call[0] for call in worksheet.calls], ["A2:B1001", "A1002:B2001", "A2002:B2501"])
        self.assertTrue(all(call[1]["value_render_option"] == "FORMULA" for call in worksheet.calls))

    def test_row_keeps_text_and_extracts_urls_without_downloading_media(self):
        headers = ["ID", "Ext ID", "Фото", "Source", "URL", "Коментарі", "Telegraph UA", "Telegraph EN"]
        values = ["abc", "123", '=IMAGE("https://img.example/1.jpg")', "olx", "https://olx.ua/d/123", "manual", "ua", "en"]
        row = MODULE.normalize_row(headers, values)
        result = MODULE.row_tuple(
            "apartments", "rent", "sheet", "Оренда", 2, row,
            str(uuid.uuid4()), datetime.now(timezone.utc),
        )
        self.assertEqual(result[5], "abc")
        self.assertEqual(result[8], "https://olx.ua/d/123")
        self.assertEqual(result[9], "https://img.example/1.jpg")
        self.assertEqual(result[12], "manual")
        self.assertEqual(json.loads(result[13].dumps(row)), row)

    def test_selected_scopes_do_not_mix_catalogs_or_operations(self):
        self.assertEqual(
            list(MODULE.selected_scopes("houses", "sale")),
            [("houses", MODULE.SHEETS["houses"], "Продаж")],
        )


if __name__ == "__main__":
    unittest.main()
