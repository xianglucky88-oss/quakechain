"""Compatibility of the offline entry points and unchanged content."""
import json
import sqlite3
import subprocess
import sys

from quakechain.config import ROOT
from quakechain.store import Store


def test_cli_full_and_incremental_are_safe_and_consistent(tmp_path):
    db = tmp_path / "cli.db"
    import quakechain.config as config
    original = config.DB_PATH
    config.DB_PATH = db
    try:
        from quakechain import cli
        first = json.loads(_capture(cli.main, []))
        second = json.loads(_capture(cli.main, ["--full"]))
        third = json.loads(_capture(cli.main, ["--incremental"]))
    finally:
        config.DB_PATH = original
    assert first["ingested"] > 0
    assert second["ingested"] == 0
    assert third["ingested"] == 0
    for key in ("count", "events", "revisions", "watermark"):
        assert first[key] == second[key] == third[key]


def _capture(main, argv):
    import io
    from contextlib import redirect_stdout
    buf = io.StringIO()
    with redirect_stdout(buf):
        main(argv)
    return buf.getvalue()


def test_cli_runs_as_a_module():
    result = subprocess.run(
        [sys.executable, "-m", "quakechain.cli", "--full"],
        cwd=str(ROOT), capture_output=True, text=True,
        env={"PYTHONPATH": "%s:%s" % (ROOT / "src", ROOT), "PATH": "/usr/bin:/bin",
             "QUAKECHAIN_DB": str(ROOT / ".pytest-cli.db")})
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["engine"] == "crawl"
    (ROOT / ".pytest-cli.db").unlink(missing_ok=True)


def test_payload_content_is_not_rewritten_by_upgrade():
    """Stored payloads keep exactly the source content that was received."""
    con = sqlite3.connect(":memory:")
    st = Store(con)
    st.reset()
    record = {"id": "E1", "name": "alpha", "ts": "2030-01-01T00:00:00Z",
              "value": 7, "status": "new", "magnitude": 5.0}
    assert st.upsert(record) is True
    assert st.get(st.hash(record)) == record
    # Derived metadata is returned by views, never injected into the payload.
    assert st.all_records() == [record]
    con.close()


def test_store_public_api_is_backward_compatible():
    con = sqlite3.connect(":memory:")
    st = Store(con)
    st.reset()
    record = {"id": "sample", "ts": "t1", "v": 1}
    assert st.upsert(record) is True
    assert st.upsert(record) is False
    st.set_watermark("checkpoint")
    assert st.watermark() == "checkpoint"
    assert st.count() == 1
    assert st.all_records() == [record]
    con.close()


def test_cli_views(tmp_path):
    import quakechain.config as config
    from quakechain import cli
    original = config.DB_PATH
    config.DB_PATH = tmp_path / "views.db"
    try:
        run = json.loads(_capture(cli.main, []))
        latest = json.loads(_capture(cli.main, ["--latest"]))
        initial = json.loads(_capture(cli.main, ["--initial"]))
        evidence = json.loads(_capture(cli.main, ["--explain", latest[0]["logical_id"]]))
        snapshot = json.loads(_capture(cli.main, ["--as-of", run["receive_time"]]))
    finally:
        config.DB_PATH = original
    assert len(latest) == len(initial) == run["events"]
    assert snapshot == latest
    assert evidence["logical_id"] == latest[0]["logical_id"]
    assert evidence["history"][-1]["revision_id"] == latest[0]["revision_id"]
