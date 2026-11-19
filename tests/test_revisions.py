"""Bi-temporal revision semantics: current view, initial view, provenance."""
import sqlite3

from quakechain.store import Store


def _store():
    con = sqlite3.connect(":memory:")
    return Store(con), con


def _rev(mag, *, rev=0, ts="2030-01-01T00:00:00Z", revised=None, reason=None,
         eid="E1", name="alpha"):
    row = {"id": eid, "name": name, "ts": ts, "magnitude": mag,
           "value": 1, "status": "new"}
    if rev:
        row["revision_no"] = rev
    if revised:
        row["revised_at"] = revised
    if reason:
        row["revision_reason"] = reason
    return row


def test_late_revision_does_not_regress_current_view():
    st, con = _store()
    try:
        assert st.upsert(_rev(5.0)) is True
        assert st.upsert(_rev(5.8, rev=3, ts="2030-01-01T00:10:00Z",
                              revised="2030-01-01T00:12:00Z", reason="magnitude fixed"))
        # Delivered afterwards, but describes an older revision.
        assert st.upsert(_rev(4.9, rev=1, ts="2030-01-01T00:02:00Z"))
        current = st.latest()
        assert len(current) == 1
        assert current[0]["magnitude"] == 5.8
        assert current[0]["revision_no"] == 3
        assert current[0]["version_no"] == 3
        assert current[0]["is_latest"] == 1
        # Initial view keeps what the first delivery held, not the newest.
        initial = st.initial()
        assert initial[0]["magnitude"] == 5.0
        assert initial[0]["version_no"] == 1
        assert initial[0]["is_latest"] == 1
    finally:
        con.close()


def test_same_instant_conflict_resolved_deterministically():
    st, con = _store()
    try:
        a = _rev(5.0, rev=2, ts="2030-01-01T00:05:00Z")
        b = _rev(6.1, rev=4, ts="2030-01-01T00:05:00Z")
        st.upsert(a)
        st.upsert(b)
        first = st.latest()[0]["revision_id"]
        # Rebuilding in the opposite delivery order must yield the same winner.
        st2, con2 = _store()
        try:
            st2.upsert(b)
            st2.upsert(a)
            assert st2.latest()[0]["revision_id"] == first
        finally:
            con2.close()
        # Two revisions with equal rank keys resolve by content hash, and the
        # outcome is stable regardless of arrival order.
        st.upsert(_rev(6.1, rev=4, ts="2030-01-01T00:05:00Z", reason="dup"))
        assert st.count() == 3
    finally:
        con.close()


def test_duplicate_delivery_is_idempotent():
    st, con = _store()
    try:
        row = _rev(5.0)
        assert st.upsert(row) is True
        assert st.upsert(dict(row)) is False
        assert st.count() == 1
        assert st.latest()[0]["revision_id"] == st.hash(row)
    finally:
        con.close()


def test_as_of_reconstructs_each_receive_moment():
    st, con = _store()
    try:
        st.upsert(_rev(5.0))
        t1 = st.latest_receive_time()
        st.upsert(_rev(5.8, rev=2, ts="2030-01-01T00:10:00Z"))
        t2 = st.latest_receive_time()
        st.upsert(_rev(4.9, rev=1, ts="2030-01-01T00:02:00Z"))
        t3 = st.latest_receive_time()

        assert st.as_of(t1)[0]["magnitude"] == 5.0
        assert st.as_of(t2)[0]["magnitude"] == 5.8
        assert st.as_of(t3)[0]["magnitude"] == 5.8  # late revision lost
        # is_latest is relative to what the view could see: at t1 the first
        # report was the latest known revision.  Version/prev-link evidence is
        # global chain metadata.
        assert st.as_of(t1)[0]["is_latest"] == 1
        assert st.as_of(t1)[0]["version_no"] == 1
        assert st.latest()[0]["is_latest"] == 1
        assert st.latest()[0]["version_no"] == 3
        assert st.as_of(t1)[0]["revision_id"] != st.latest()[0]["revision_id"]
        # as_of never exposes rows received after the requested moment: the
        # latest revision known at t2 is the rev-2 row received at t2, not the
        # rev-1 row that arrives later.
        latest_at_t2 = st.as_of(t2)
        assert len(latest_at_t2) == 1
        assert latest_at_t2[0]["receive_time"] == t2
        assert latest_at_t2[0]["revision_no"] == 2
    finally:
        con.close()


def test_history_and_explain_preserve_provenance():
    st, con = _store()
    try:
        st.upsert(_rev(5.0))
        st.upsert(_rev(5.8, rev=2, ts="2030-01-01T00:10:00Z",
                      revised="2030-01-01T00:12:00Z", reason="magnitude fixed"))
        history = st.history("E1")
        assert [r["magnitude"] for r in history] == [5.0, 5.8]
        assert [r["version_no"] for r in history] == [1, 2]
        assert history[0]["prev_version_id"] is None
        assert history[1]["prev_version_id"] == history[0]["revision_id"]
        assert [r["is_latest"] for r in history] == [0, 1]
        assert all(r["source_id"] == "local-fixture" for r in history)
        assert all(r["receive_time"] for r in history)
        assert history[1]["revised_time"] == "2030-01-01T00:12:00Z"
        assert history[1]["basis"] == "magnitude fixed"

        evidence = st.explain("E1")
        assert evidence["revision_count"] == 2
        assert evidence["initial_revision_id"] == history[0]["revision_id"]
        assert evidence["latest_revision_id"] == history[1]["revision_id"]
        assert evidence["first_received"] == history[0]["receive_time"]
        assert evidence["latest_received"] == history[1]["receive_time"]
    finally:
        con.close()


def test_rerun_produces_identical_evidence():
    rows = [_rev(5.0), _rev(5.8, rev=2, ts="2030-01-01T00:10:00Z")]
    snapshots = []
    for _ in range(2):
        st, con = _store()
        try:
            for row in reversed(rows):
                st.upsert(row)
            # Re-run the same batch: nothing new is written, evidence stable.
            for row in rows:
                assert st.upsert(row) is False
            snapshots.append((st.latest(), st.initial(), st.explain("E1")))
        finally:
            con.close()
    assert snapshots[0] == snapshots[1]


def test_legacy_database_upgrades_in_place(tmp_path):
    import json
    path = tmp_path / "legacy.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT,"
                " ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    legacy = {"id": "E9", "name": "legacy", "ts": "2030-01-02T00:00:00Z",
              "magnitude": 4.2, "value": 3, "status": "new"}
    old_hash = __import__("hashlib").sha256(
        json.dumps(legacy, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]
    con.execute("INSERT INTO records VALUES(?,?,?,?)",
                (old_hash, "E9", legacy["ts"], json.dumps(legacy)))
    con.commit()
    con.close()

    con = sqlite3.connect(path)
    st = Store(con)
    try:
        assert st.schema_version() == 2
        assert st.count() == 1
        current = st.latest()
        assert current[0]["magnitude"] == 4.2
        assert current[0]["revision_id"] == old_hash  # identity preserved
        assert current[0]["rule_version"] == "1"
        assert current[0]["basis"] == "legacy"
        assert current[0]["source_id"] == "local-fixture"
        assert current[0]["receive_time"]
        assert st.initial()[0]["revision_id"] == old_hash
        # Re-ingesting the exact same content after upgrade is still a no-op.
        assert st.upsert(legacy) is False
        # A genuine revision after the upgrade chains onto the legacy row.
        assert st.upsert(_rev(4.9, rev=2, eid="E9", name="legacy")) is True
        history = st.history("E9")
        assert len(history) == 2
        assert history[0]["revision_id"] == old_hash
        assert history[1]["prev_version_id"] == old_hash
    finally:
        con.close()


def test_same_rank_conflict_is_flagged_not_silently_resolved():
    st, con = _store()
    try:
        a = _rev(5.0, rev=2, ts="2030-01-01T00:05:00Z")
        b = _rev(6.1, rev=2, ts="2030-01-01T00:05:00Z")
        st.upsert(a)
        st.upsert(b)
        evidence = st.explain("E1")
        assert evidence["revision_count"] == 2
        assert len(evidence["conflicts"]) == 1
        pair = evidence["conflicts"][0]
        assert set(pair) == {st.hash(a), st.hash(b)}
        history = st.history("E1")
        assert all(r["conflict"] for r in history)
        # The winner is deterministic (highest content hash) and stable.
        winner = st.latest()[0]["revision_id"]
        assert winner == max(st.hash(a), st.hash(b))
        st2, con2 = _store()
        try:
            st2.upsert(b)
            st2.upsert(a)
            assert st2.latest()[0]["revision_id"] == winner
        finally:
            con2.close()
    finally:
        con.close()


def test_migration_is_idempotent_when_reopened():
    import json
    import tempfile
    from pathlib import Path

    directory = tempfile.mkdtemp()
    path = Path(directory) / "legacy.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT,"
                " ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    legacy = {"id": "E3", "name": "legacy", "ts": "2030-01-03T00:00:00Z",
              "magnitude": 3.3, "value": 1, "status": "new"}
    digest = __import__("hashlib").sha256(
        json.dumps(legacy, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:24]
    con.execute("INSERT INTO records VALUES(?,?,?,?)",
                (digest, "E3", legacy["ts"], json.dumps(legacy)))
    # Simulate an upgrade that stopped after part of the work: new columns
    # exist and one row already has provenance, but the version is still old.
    for column in ("logical_id", "source_id", "event_time", "revised_time",
                   "receive_time", "revision_no", "basis", "rule_version",
                   "first_seen", "last_seen"):
        con.execute("ALTER TABLE records ADD COLUMN %s TEXT" % column)
    con.execute("UPDATE records SET logical_id='E3', event_time=?,"
                " receive_time=?, basis='legacy', rule_version='1'",
                (legacy["ts"], "2030-01-01T00:00:09Z"))
    con.execute("INSERT INTO meta VALUES('schema_version','1')")
    con.commit()
    con.close()

    con = sqlite3.connect(path)
    first = Store(con)
    snapshot = (first.latest(), first.initial(), first.explain("E3"))
    con.close()

    con = sqlite3.connect(path)
    second = Store(con)
    try:
        assert (second.latest(), second.initial(), second.explain("E3")) == snapshot
        assert second.migration_history() == ["v1->v2"]
    finally:
        con.close()


def _legacy_db(rows):
    import json
    import tempfile
    from pathlib import Path

    path = Path(tempfile.mkdtemp()) / "old.db"
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT,"
                " ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    seen = set()
    for row in rows:
        stored = dict(row)
        stored["logical_id"] = str(stored.get("name", "")).lower()
        stored["version_no"] = 1
        stored["is_latest"] = 1
        stored["prev_version_id"] = None
        digest = __import__("hashlib").sha256(
            json.dumps(stored, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:24]
        if digest in seen:
            continue
        seen.add(digest)
        con.execute("INSERT INTO records VALUES(?,?,?,?)",
                    (digest, str(stored["id"]), str(stored["ts"]),
                     json.dumps(stored)))
    con.execute("INSERT INTO meta VALUES('watermark','2030-01-05T00:00:00Z')")
    con.commit()
    con.close()
    return path


def test_upgrade_with_old_derived_fields_does_not_duplicate():
    from quakechain import framework
    from quakechain.mockdata import build_rows

    path = _legacy_db(build_rows())
    con = sqlite3.connect(path)
    st = Store(con)
    try:
        assert st.count() == 9
        assert st.event_count() == 9
        assert st.migration_history() == ["v1->v2"]
        # Old derived fields are stripped from the live payload but preserved
        # verbatim for traceability.
        for revision_id in [row["content_hash"] for row in st._rows()]:
            original = st.legacy_payload(revision_id)
            assert original is not None
            assert "version_no" in original
            assert "version_no" not in st.get(revision_id)
        batch, _ = framework.fetch_all(
            framework.Http(framework.Transport(), framework.Sleeper()))
        _, _, ingested, _ = framework._ingest(st, batch)
        assert ingested == 0
        assert st.count() == 9
        assert st.event_count() == 9
        assert len(st.latest()) == len(st.initial()) == 9
    finally:
        con.close()


def test_reindex_applies_rule_without_touching_identity_or_time():
    st, con = _store()
    try:
        row = {"id": "R1", "name": "alpha", "ts": "2030-01-01T00:00:00Z",
               "value": 1, "status": "new", "magnitude": 5.0}
        st.upsert(row)
        before = st.history("R1")[0]
        assert st.reindex() is None
        after = st.history("R1")[0]
        assert after["revision_id"] == before["revision_id"]
        assert after["receive_time"] == before["receive_time"]
        assert after["event_time"] == before["event_time"]
        assert after["rule_version"] == before["rule_version"]
        assert st.get(before["revision_id"]) == {
            k: v for k, v in row.items() if not k.startswith("logical_")}
    finally:
        con.close()


def test_rule_history_only_records_real_transitions():
    st, con = _store()
    try:
        assert st.rule_history() == []  # no data yet, no phantom transition
        st.upsert(_rev(5.0))
        st._sync_rule_version()
        assert st.rule_history() == []
        assert st.rule_version() == "2"
    finally:
        con.close()


def test_legacy_duplicate_content_collapses_to_one_revision():
    import json
    base = {"id": "E4", "name": "alpha", "ts": "2030-01-04T00:00:00Z",
            "value": 1, "status": "new", "magnitude": 5.0}
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE records(content_hash TEXT PRIMARY KEY, id TEXT,"
                " ts TEXT, payload TEXT NOT NULL)")
    con.execute("CREATE TABLE meta(k TEXT PRIMARY KEY, v TEXT)")
    for extra in ({"logical_id": "alpha"}, {"logical_id": "alpha", "version_no": 1}):
        stored = dict(base, **extra)
        digest = __import__("hashlib").sha256(
            json.dumps(stored, sort_keys=True, ensure_ascii=False).encode()
        ).hexdigest()[:24]
        con.execute("INSERT INTO records VALUES(?,?,?,?)",
                    (digest, "E4", base["ts"], json.dumps(stored)))
    con.commit()
    st = Store(con)
    try:
        # Both raw rows stay as evidence, but they are one revision of one
        # event, not a same-instant conflict.
        assert st.count() == 2
        assert st.event_count() == 1
        current = st.latest()
        assert len(current) == 1
        assert current[0]["magnitude"] == 5.0
        assert current[0]["version_no"] == 1
        assert current[0]["conflict"] == 0
        assert st.explain("E4")["conflicts"] == []
        assert len(st.history("E4")) == 1
        # The content is still known, so re-ingesting it is a no-op.
        assert st.upsert(base) is False
    finally:
        con.close()
