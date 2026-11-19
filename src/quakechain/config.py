import os
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
# Default location; an explicit override keeps the offline run reproducible in
# tests and secondary environments without changing the command line.
DB_PATH = Path(os.environ.get("QUAKECHAIN_DB", str(ROOT / "quakechain.db")))

# Provenance defaults for the bundled offline source.
SOURCE_ID = "local-fixture"
SCHEMA_VERSION = "2"
# Version of the local rule layer (identity / derivation rules).
RULE_VERSION = "2"
# Version shipped before rule updates; used as the baseline when an existing
# database has no recorded rule version.
BASE_RULE_VERSION = "2"
# Logical transaction-time base.  Reception timestamps are produced by a
# deterministic, monotonic logical clock so an offline full re-run against a
# fresh database yields the same evidence as the first run.
RECEIVE_BASE = "2030-01-01T00:00:00Z"

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
