"""RPA engine parity: the session path uses the same revision evidence."""
import sqlite3

import pytest

import quakechain.config as config
import quakechain.pipeline as pipeline
from quakechain.store import Store


@pytest.fixture(autouse=True)
def rpa_engine(monkeypatch):
    monkeypatch.setattr(config.SPEC, "engine", "rpa")
    yield


@pytest.fixture()
def store():
    con = sqlite3.connect(":memory:")
    st = Store(con)
    st.reset()
    yield st
    con.close()


def test_rpa_run_records_revisions_and_views(store):
    import quakechain.framework as framework
    result = framework.run_rpa(con=store.con)
    assert result["engine"] == "rpa"
    assert result["logged_in"] is True
    assert result["captcha_solved"] is True
    assert result["ingested"] > 0
    latest = store.latest()
    initial = store.initial()
    assert len(latest) == len(initial) == result["events"]
    receipt = result["receive_time"]
    assert store.as_of(receipt) == latest
    evidence = store.explain(latest[0]["logical_id"])
    assert evidence["sources"] == ["local-fixture"]
    assert evidence["history"][-1]["revision_id"] == latest[0]["revision_id"]

    # A second session run is idempotent.
    again = framework.run_rpa(con=store.con)
    assert again["ingested"] == 0
    assert store.count() == result["count"]
    assert store.latest() == latest


def test_rpa_pipeline_entry_point(store):
    result = pipeline.run(con=store.con)
    assert result["engine"] == "rpa"
    assert result["revisions"] == result["count"]
    assert len(pipeline.latest(con=store.con)) == result["events"]
    assert len(pipeline.initial(con=store.con)) == result["events"]
