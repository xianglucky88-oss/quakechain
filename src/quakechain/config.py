
from types import SimpleNamespace

ROOT = __import__("pathlib").Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "quakechain.db"

SPEC = SimpleNamespace(
    slug="quakechain",
    fmt="json",
    engine="crawl",
    endpoint="items",
    columns=["id", "name", "value", "status", "ts", "magnitude"],
    page_size=4,
    int_fields=["value"],
    ts_field="ts",
)
