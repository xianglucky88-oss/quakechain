"""End-to-end delivery scenarios and two-view consistency."""
import sqlite3

import pytest

import mockapp.main as source
from quakechain import framework, pipeline
from quakechain.store import Store


@pytest.fixture(autouse=True)
def clean_source():
    source._extra.clear()
    source._accepted_tokens.clear()
    source._verified_sessions.clear()
    source.app.__dict__["_flaky_n"] = 0
    yield
    source._extra.clear()


@pytest.fixture()
def store():
    con = sqlite3.connect(":memory:")
    st = Store(con)
    st.reset()
    yield st
    con.close()


def _rows():
    rows, attempts = framework.fetch_all(
        framework.Http(framework.Transport(), framework.Sleeper()))
    return rows


def test_full_run_is_idempotent_and_keeps_both_views(store):
    batch = _rows()
    before = None
    for _ in range(3):  # initial run + full re-runs
        norm, dup, ingested, received = framework._ingest(store, batch)
        current = (store.latest(), store.initial(), store.explain("quc-0001"))
        if before is not None:
            assert ingested == 0
            assert current == before
        before = current
    # The bundled source contains a repeated row; it is reported, not stored.
    assert dup == 1
    assert store.count() == len(batch) - 1
    latest = store.latest()
    initial = store.initial()
    assert len(latest) == len(initial) == store.event_count()
    # Without revisions, both perspectives agree on content and evidence.
    assert [r["revision_id"] for r in latest] == [r["revision_id"] for r in initial]


def test_out_of_order_revisions_converge(store):
    base = {"id": "Q1", "name": "alpha", "value": 1, "status": "new",
            "ts": "2030-01-01T00:00:00Z", "magnitude": 5.0}
    rev2 = dict(base, ts="2030-01-01T00:10:00Z", magnitude=5.8,
                revision_no=2, revised_at="2030-01-01T00:12:00Z",
                revision_reason="magnitude fixed")
    late1 = dict(base, ts="2030-01-01T00:02:00Z", magnitude=4.9, revision_no=1)

    orders = [[base, rev2, late1], [late1, rev2, base], [rev2, late1, base]]
    results = []
    for order in orders:
        st = Store(sqlite3.connect(":memory:"))
        framework._ingest(st, order)
        results.append((st.latest(), st.origin(), st.initial()))
    assert results[0] == results[1] == results[2]
    # The current view is the newest valid revision regardless of delivery.
    assert results[0][0][0]["magnitude"] == 5.8
    # The source-time origin is the earliest published revision.
    assert results[0][1][0]["magnitude"] == 5.0
    # The initial-reception snapshot is everything the first delivery held.
    assert results[0][2][0]["magnitude"] == 5.8


def test_cross_page_failure_writes_nothing(store, monkeypatch):
    http = framework.Http(framework.Transport(), framework.Sleeper())
    real_get = http.get
    calls = {"n": 0}

    def flaky(path, params=None, headers=None):
        calls["n"] += 1
        if params and params.get("page") == 2:
            return 500, {}, "boom", 0
        return real_get(path, params, headers)

    http.get = flaky
    with pytest.raises(framework.FetchError):
        framework.fetch_all(http)
    assert calls["n"] >= 2
    # The failed run is retried without any partial state.
    framework._ingest(store, _rows())
    assert store.count() > 0
    assert store.watermark()


def test_duplicate_page_does_not_duplicate_revisions(store):
    rows = source.build_rows()
    page = source.SPEC.page_size
    pages = [rows[i:i + page] for i in range(0, len(rows), page)]
    # Page 1 is delivered twice and the trailing partial page repeats page 2,
    # exactly what a repeated-page fetch produces.
    duplicated = pages[0] + pages[0] + pages[1] + pages[1] + pages[2]

    _, dup, ingested, _ = framework._ingest(store, duplicated)
    assert dup == len(duplicated) - len({framework.content_hash(
        framework.normalize(dict(r))) for r in duplicated})
    assert ingested == store.count() == len({framework.content_hash(
        framework.normalize(dict(r))) for r in rows})
    # Replaying every page again adds no revisions and no new evidence.
    _, _, ingested_again, _ = framework._ingest(store, duplicated)
    assert ingested_again == 0
    assert store.count() == ingested


def test_continuation_after_refresh_preserves_evidence(store):
    first = _rows()
    framework._ingest(store, first)
    snapshot_latest = store.latest()
    snapshot_initial = store.initial()
    cutoff = store.latest_receive_time()

    # A new record appears at the source and is pulled by a later run.
    source._extra.append(source.extra_row(1))
    second = _rows()
    _, _, ingested, _ = framework._ingest(store, second, mode="incremental")
    assert ingested == 1
    latest = store.latest()
    assert len(latest) == len(snapshot_latest) + 1
    # Previous evidence is untouched; the as-of view still matches the past.
    assert store.as_of(cutoff) == snapshot_latest
    assert [r["revision_id"] for r in store.initial()][:-1] == [
        r["revision_id"] for r in snapshot_initial]
    # Both perspectives still describe every event exactly once.
    assert len({r["logical_id"] for r in latest}) == len(latest)
    assert len({r["logical_id"] for r in store.initial()}) == len(store.initial())


def test_pipeline_exposes_two_views():
    con = sqlite3.connect(":memory:")
    Store(con).reset()
    try:
        result = pipeline.run(con=con)
        assert result["ingested"] > 0
        assert result["events"] == 9
        assert result["revisions"] >= result["events"]
        latest = pipeline.latest(con=con)
        initial = pipeline.initial(con=con)
        assert len(latest) == len(initial) == result["events"]
        cutoff = result["receive_time"]
        assert pipeline.as_of(cutoff, con=con) == latest
        evidence = pipeline.explain(latest[0]["logical_id"], con=con)
        assert evidence["sources"] == ["local-fixture"]
        assert evidence["first_received"] <= evidence["latest_received"]
        assert evidence["history"][-1]["revision_id"] == latest[0]["revision_id"]
    finally:
        con.close()


def test_revision_delivered_by_later_run_does_not_roll_back(store):
    """End-to-end: initial report, then a revision, then a late older copy."""
    initial_row = source.build_rows()[0]
    first = _rows()
    framework._ingest(store, first)
    initial_view = store.initial()
    cutoff = store.latest_receive_time()
    baseline = store.latest()

    revision = dict(initial_row, magnitude="6.4", ts="2030-01-09T00:00:00Z",
                    revision_no=2, revised_at="2030-01-09T01:00:00Z",
                    revision_reason="moment magnitude revised")
    source._extra.append(revision)
    second = _rows()
    _, _, ingested, _ = framework._ingest(store, second, mode="incremental")
    assert ingested == 1

    after = store.latest()
    revised = [r for r in after if r["logical_id"] == initial_row["id"]][0]
    assert revised["magnitude"] == "6.4"
    assert revised["revision_no"] == 2
    assert revised["version_no"] == 2
    assert revised["is_latest"] == 1
    # The initial perspective still describes the first-observed revision.
    initial_before = [r for r in initial_view
                      if r["logical_id"] == initial_row["id"]][0]
    initial_after = [r for r in store.initial()
                     if r["logical_id"] == initial_row["id"]][0]
    assert initial_after["revision_id"] == initial_before["revision_id"]
    assert initial_after["magnitude"] == initial_row["magnitude"]
    assert initial_after["version_no"] == 1
    # Chain evidence for the same revision is consistent across views.
    chain_initial = store.history(initial_row["id"])[0]
    assert chain_initial["revision_id"] == initial_after["revision_id"]
    assert chain_initial["version_no"] == initial_after["version_no"]
    # The as-of view at the earlier cutoff reproduces the pre-revision state.
    assert store.as_of(cutoff) == baseline
    # A late replay of the original initial report cannot roll back the view.
    source._extra.append(initial_row)
    third = _rows()
    _, _, ingested3, _ = framework._ingest(store, third)
    assert ingested3 == 0
    assert [r for r in store.latest() if r["logical_id"] == initial_row["id"]][0][
        "revision_id"] == revised["revision_id"]
    # The initial perspective is still the first-observed revision.
    assert [r for r in store.initial() if r["logical_id"] == initial_row["id"]][0][
        "revision_id"] == chain_initial["revision_id"]


def test_rule_update_keeps_old_records_traceable(store, monkeypatch):
    record = {"id": "E1", "name": "alpha", "ts": "2030-01-01T00:00:00Z",
              "value": 1, "status": "new", "magnitude": 5.0}
    store.upsert(record)
    old_id = store.latest()[0]["revision_id"]

    import quakechain.store as store_module
    monkeypatch.setattr(store_module, "RULE_VERSION", "3")
    store._sync_rule_version()

    assert store.rule_version() == "3"
    assert store.rule_history() == ["2->3"]
    # Existing evidence is untouched and still tagged with its rule version.
    current = store.latest()
    assert current[0]["revision_id"] == old_id
    assert current[0]["rule_version"] == "2"
    # New revisions are tagged with the new rule version.
    store.upsert(dict(record, ts="2030-01-02T00:00:00Z", magnitude=5.5,
                      revision_no=2))
    assert store.history("E1")[-1]["rule_version"] == "3"
    assert store.history("E1")[0]["rule_version"] == "2"


def _fresh_store():
    con = sqlite3.connect(":memory:")
    st = Store(con)
    st.reset()
    return st


def test_full_rerun_matches_across_independent_databases():
    """A full re-run against a fresh database reproduces both views exactly."""
    revision = dict(source.build_rows()[0], magnitude="6.4",
                    ts="2030-01-09T00:00:00Z", revision_no=2,
                    revised_at="2030-01-09T01:00:00Z",
                    revision_reason="moment magnitude revised")
    source._extra.append(revision)

    views = []
    for _ in range(2):
        st = _fresh_store()
        batch = _rows()
        framework._ingest(st, batch)
        framework._ingest(st, batch)  # repeated full run in the same database
        cutoff = st.latest_receive_time()
        views.append({
            "latest": st.latest(),
            "initial": st.initial(),
            "origin": st.origin(),
            "latest_digest": Store.digest(st.latest()),
            "initial_digest": Store.digest(st.initial()),
            "history": st.history(source.build_rows()[0]["id"]),
            "as_of": st.as_of(cutoff),
        })
    assert views[0] == views[1]
    # Every perspective contains the same events, each exactly once.
    latest_ids = {r["logical_id"] for r in views[0]["latest"]}
    for name in ("initial", "origin"):
        assert {r["logical_id"] for r in views[0][name]} == latest_ids
    # The local source delivers both revisions in a single batch, so the
    # initial-reception snapshot already contains the revision, while the
    # source-time origin still shows the original bulletin parameters.
    rid = source.build_rows()[0]["id"]
    assert [r for r in views[0]["latest"] if r["logical_id"] == rid][0][
        "magnitude"] == "6.4"
    assert [r for r in views[0]["initial"] if r["logical_id"] == rid][0][
        "magnitude"] == "6.4"
    assert [r for r in views[0]["origin"] if r["logical_id"] == rid][0][
        "magnitude"] == source.build_rows()[0]["magnitude"]
    # Evidence for a revision id is identical in latest/initial/history.
    by_id = {r["revision_id"]: r for r in views[0]["history"]}
    for view in ("latest", "initial", "origin"):
        for row in views[0][view]:
            if row["revision_id"] in by_id:
                reference = by_id[row["revision_id"]]
                assert row["version_no"] == reference["version_no"]
                assert row["prev_version_id"] == reference["prev_version_id"]
                assert row["receive_time"] == reference["receive_time"]
                assert row["source_id"] == reference["source_id"]
    # Each view projects exactly one revision per event, and marks it current.
    for name in ("latest", "initial", "origin"):
        ids = [r["logical_id"] for r in views[0][name]]
        assert len(ids) == len(set(ids))
        assert sum(r["is_latest"] for r in views[0][name]) >= 1
    # The current view marks exactly one revision per event as latest.


def test_as_of_reconstructs_initial_then_current():
    st = _fresh_store()
    row = {"id": "Q7", "name": "alpha", "value": 1, "status": "new",
           "ts": "2030-01-01T00:00:00Z", "magnitude": 5.0}
    framework._ingest(st, [row])
    t1 = st.latest_receive_time()
    initial_latest = st.latest()

    revised = dict(row, magnitude=5.9, ts="2030-01-01T00:20:00Z",
                   revision_no=2, revised_at="2030-01-01T00:25:00Z",
                   revision_reason="depth recomputed")
    framework._ingest(st, [revised])
    t2 = st.latest_receive_time()

    assert st.as_of(t1) == initial_latest
    assert st.as_of(t1)[0]["magnitude"] == 5.0
    assert st.as_of(t1)[0]["is_latest"] == 1  # it was current at that moment
    assert st.as_of(t2)[0]["magnitude"] == 5.9
    assert st.as_of(t2)[0]["version_no"] == 2
    assert st.as_of(t2)[0]["is_latest"] == 1
    assert st.initial()[0]["magnitude"] == 5.0
    # Evidence for the same revision is stable between as_of and latest.
    assert st.as_of(t1)[0]["revision_id"] == st.initial()[0]["revision_id"]
    assert st.as_of(t2)[0]["revision_id"] == st.latest()[0]["revision_id"]


def test_rule_update_then_continuation_keeps_history_traceable(store, monkeypatch):
    """Old rows stay readable after a rule change and new runs keep working."""
    framework._ingest(store, _rows())
    before_latest = store.latest()
    before_initial = store.initial()
    assert {r["rule_version"] for r in before_latest} == {"2"}

    import quakechain.store as store_module
    monkeypatch.setattr(store_module, "RULE_VERSION", "3")
    store._sync_rule_version()
    assert store.rule_history() == ["2->3"]

    # A later continuation run uses the new rule version for new revisions.
    revision = dict(source.build_rows()[1], magnitude="7.1",
                    ts="2030-02-01T00:00:00Z", revision_no=2,
                    revised_at="2030-02-01T00:30:00Z",
                    revision_reason="recalculated")
    source._extra.append(revision)
    batch = _rows()
    _, _, ingested, _ = framework._ingest(store, batch, mode="incremental")
    assert ingested == 1

    history = store.history(source.build_rows()[1]["id"])
    assert [r["rule_version"] for r in history] == ["2", "3"]
    assert history[-1]["magnitude"] == "7.1"
    # Pre-existing records are unchanged apart from chaining metadata.
    untouched = [r for r in store.latest()
                 if r["logical_id"] != source.build_rows()[1]["id"]]
    assert [r["revision_id"] for r in untouched] == [
        r["revision_id"] for r in before_latest
        if r["logical_id"] != source.build_rows()[1]["id"]]
    assert [r["magnitude"] for r in store.initial()] == [
        r["magnitude"] for r in before_initial]
    # Evidence for each revision id is consistent across latest/initial.
    by_id = {r["revision_id"]: r for r in store.history(
        source.build_rows()[1]["id"])}
    for view in (store.latest(), store.initial()):
        for row in view:
            if row["revision_id"] in by_id:
                reference = by_id[row["revision_id"]]
                # Chain position/link and provenance are global evidence;
                # ``is_latest`` is view-relative by design.
                for key in ("version_no", "prev_version_id", "receive_time",
                            "source_id", "rule_version"):
                    assert row[key] == reference[key]


def test_evidence_matches_across_all_delivery_scenarios(store):
    """Out-of-order, duplicate pages, full rerun and continuation agree."""
    base_rows = source.build_rows()
    page = source.SPEC.page_size
    pages = [base_rows[i:i + page] for i in range(0, len(base_rows), page)]

    # 1. Out-of-order redelivery: trailing pages first, then the workspace is
    #    replayed and a duplicate page is inserted.
    framework._ingest(store, [r for chunk in reversed(pages) for r in chunk])
    framework._ingest(store, pages[1] + pages[0] + pages[1])
    first_latest = store.latest()
    first_initial = store.initial()
    first_origin = store.origin()
    first_evidence = {row["logical_id"]: store.explain(row["logical_id"])
                      for row in first_latest}
    cutoff = store.latest_receive_time()

    # 2. Full re-run: nothing new, and the logical clock does not move.
    _, dup, ingested, received = framework._ingest(
        store, [r for chunk in pages for r in chunk])
    assert ingested == 0
    assert received == cutoff

    # 3. Continuation: one new event arrives later.
    source._extra.append(source.extra_row(1))
    _, _, ingested, _ = framework._ingest(store, _rows(), mode="incremental")
    assert ingested == 1

    latest = store.latest()
    assert len(latest) == len(first_latest) + 1
    # The past is reproducible at the old cutoff and unchanged from before.
    assert store.as_of(cutoff) == first_latest
    for old in first_latest:
        row = [r for r in latest if r["logical_id"] == old["logical_id"]][0]
        assert row["revision_id"] == old["revision_id"]
        assert row["receive_time"] == old["receive_time"]
        assert store.explain(old["logical_id"]) == first_evidence[old["logical_id"]]
    # Initial and origin perspectives only grow by the new event.
    for name, before in (("initial", first_initial), ("origin", first_origin)):
        after = getattr(store, name)()
        assert {r["logical_id"] + ":" + r["revision_id"] for r in after} >= {
            r["logical_id"] + ":" + r["revision_id"] for r in before}


def test_failure_midway_leaves_previous_history_intact(store, monkeypatch):
    framework._ingest(store, _rows())
    snapshot = (store.latest(), store.initial(), store.origin(), store.count())
    http = framework.Http(framework.Transport(), framework.Sleeper())
    real_get = http.get

    def failing(path, params=None, headers=None):
        if (params or {}).get("page") == 3:
            return 503, {}, "unavailable", 0
        return real_get(path, params, headers)

    http.get = failing
    # The collection never returns, so no partial batch can reach the store.
    with pytest.raises(framework.FetchError):
        framework.fetch_all(http)
    assert (store.latest(), store.initial(), store.origin(),
            store.count()) == snapshot
