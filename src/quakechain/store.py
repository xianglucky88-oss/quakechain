"""SQLite store with append-only revisions and bi-temporal views.

Design notes
------------
* One row is stored per distinct *source revision content*.  Rows are never
  updated or deleted by ingestion, so a late re-delivery can add evidence but
  cannot rewrite what was already observed.
* ``content_hash`` is a content identity.  Fields produced by the local rule
  layer (``logical_id``, ``version_no``, ...) are excluded from the hash so a
  rule change never alters the identity of an already stored revision.
* Ordering between revisions of one logical event only uses source facts
  (``revision_no``, revision/event time and, as a last resort, the content
  hash).  Arrival order is deliberately excluded.  Consequently out-of-order
  delivery, same-instant conflicts and duplicate fetching cannot make a newer
  valid revision regress.
* Two time perspectives are exposed:

  ``event_time``
      When the described state occurred / the source timestamps its report.

  ``receive_time``
      When this process first ingested the revision.  It comes from a
      deterministic logical clock, so an offline full re-run against a fresh
      database reproduces exactly the same evidence.

  ``latest()`` is the current view (highest-ranked revision per event).
  ``origin()`` is the source-time view (earliest published revision).
  ``initial()`` is the reception-time reconstruction of the first report per
  event (``as_of`` at that event's first reception).
  ``as_of(t)`` reconstructs the current view as it stood at receive time ``t``.
  ``history(id)``/``explain(id)`` expose the ordered chain plus its evidence.

  ``version_no``/``prev_version_id`` are global chain evidence.  ``is_latest``
  is scoped to what the view could see, so ``as_of(t)`` reproduces exactly what
  ``latest()`` returned at receive time ``t``.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from .config import (BASE_RULE_VERSION, RECEIVE_BASE, RULE_VERSION,
                     SCHEMA_VERSION, SOURCE_ID)

# Fields written by the batch-local kernel transform or by this store.  They
# describe processing state, not source content, so they must never influence
# revision identity or ordering.
_DERIVED_FIELDS = {
    "version_no",
    "prev_version_id",
    "is_latest",
    "logical_id",
    "revision_id",
    "receive_time",
}

_REVISION_KEYS = ("revision_no", "revision", "rev", "version")
_BASIS_KEYS = ("revision_reason", "basis", "reason")
_EVENT_KEYS = ("event_time", "occurred_at", "ts")
_REVISED_KEYS = ("revised_at", "revision_time", "updated_at", "update_time")
_SOURCE_KEYS = ("source_id", "source", "feed")

_COLUMNS = (
    "content_hash", "id", "ts", "payload", "logical_id", "source_id",
    "event_time", "revised_time", "receive_time", "revision_no", "basis",
    "rule_version", "legacy_payload", "canonical_hash",
)


def _canonical(value):
    # Same formatting as the original content hash implementation so hashes
    # computed before an upgrade stay valid.
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def canonical_content(record):
    """Source content of a revision without processing-derived fields."""
    return {k: v for k, v in record.items() if k not in _DERIVED_FIELDS}


def _parse_time(value):
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _format_time(value):
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_time(value):
    """Best-effort canonical UTC form; opaque values are preserved as-is."""
    if value is None:
        return ""
    parsed = _parse_time(value)
    return _format_time(parsed) if parsed is not None else str(value)


def _as_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


class Store:
    def __init__(self, con=None):
        self.con = con
        self._batch_depth = 0
        if con is not None:
            self._init()

    # ------------------------------------------------------------------ #
    # Connection / schema / migration.
    # ------------------------------------------------------------------ #
    def connect(self):
        # Resolved at call time so the configured database location is honored.
        from .config import DB_PATH
        con = sqlite3.connect(DB_PATH)
        self.__init__(con)
        return con

    def _meta_get(self, key):
        row = self.con.execute("SELECT v FROM meta WHERE k=?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_set(self, key, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES(?,?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (key, str(value)))

    def schema_version(self):
        value = self._meta_get("schema_version")
        return int(value) if value else 1

    def _add_missing_columns(self):
        existing = {row[1] for row in
                    self.con.execute("PRAGMA table_info(records)").fetchall()}
        additions = [
            ("logical_id", "TEXT"), ("source_id", "TEXT"),
            ("event_time", "TEXT"), ("revised_time", "TEXT"),
            ("receive_time", "TEXT"), ("revision_no", "INTEGER DEFAULT 0"),
            ("basis", "TEXT"), ("rule_version", "TEXT"),
            ("legacy_payload", "TEXT"), ("canonical_hash", "TEXT"),
        ]
        for name, kind in additions:
            if name not in existing:
                self.con.execute("ALTER TABLE records ADD COLUMN %s %s" % (name, kind))

    def _init(self):
        self.con.execute("CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        tables = {row[0] for row in self.con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        had_records = "records" in tables
        had_rows = had_records and self.con.execute(
            "SELECT 1 FROM records LIMIT 1").fetchone() is not None
        previous_version = self.schema_version() if "meta" in tables else 1
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS records("
            "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL,"
            "logical_id TEXT, source_id TEXT, event_time TEXT, revised_time TEXT,"
            "receive_time TEXT, revision_no INTEGER DEFAULT 0, basis TEXT,"
            "rule_version TEXT, legacy_payload TEXT, canonical_hash TEXT)")
        self._add_missing_columns()
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_logical ON records(logical_id)")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_received ON records(receive_time)")
        self.con.execute(
            "CREATE INDEX IF NOT EXISTS idx_records_canonical ON records(canonical_hash)")
        if had_rows and previous_version < int(SCHEMA_VERSION):
            self._migrate_legacy()
        if previous_version < int(SCHEMA_VERSION):
            self._meta_set("schema_version", SCHEMA_VERSION)
            if had_rows:
                self._record_migration(previous_version)
        self._sync_rule_version()
        self._sync_clock()
        self.con.commit()

    def _sync_rule_version(self):
        """Record a rule-layer change without rewriting existing evidence.

        Rows keep the ``rule_version`` they were produced under, so old records
        remain traceable after a rule update.  Only new revisions are tagged
        with the current version.
        """
        stored = self._meta_get("rule_version")
        current = stored or BASE_RULE_VERSION
        if current == RULE_VERSION:
            return
        has_rows = self.con.execute(
            "SELECT 1 FROM records LIMIT 1").fetchone() is not None
        self._meta_set("rule_version", RULE_VERSION)
        if not has_rows:
            return
        history = self._meta_get("rule_history") or ""
        entry = "%s->%s" % (current, RULE_VERSION)
        if entry not in history.split(","):
            history = (history + "," + entry).lstrip(",")
            self._meta_set("rule_history", history)
        # Derived columns are pure functions of the stored source content.
        # Recomputing them applies the new rule uniformly while leaving
        # revision identity, reception times and payloads untouched.
        self.reindex()

    def rule_history(self):
        history = self._meta_get("rule_history") or ""
        return [part for part in history.split(",") if part]

    def _record_migration(self, previous_version):
        """Keep a deterministic audit trail of schema upgrades in ``meta``."""
        history = self._meta_get("migration_history") or ""
        entry = "v%s->v%s" % (previous_version, SCHEMA_VERSION)
        if entry not in history.split(","):
            history = (history + "," + entry).lstrip(",")
            self._meta_set("migration_history", history)

    def reindex(self):
        """Recompute rule-derived columns from stored payloads.

        Idempotent.  It never touches ``content_hash``, ``receive_time`` or the
        payload, so past revisions remain traceable and duplicate detection is
        unaffected; only ``logical_id``/source/time metadata is refreshed.
        """
        rows = self.con.execute(
            "SELECT content_hash, payload, id, ts FROM records").fetchall()
        for content_hash, payload, rid, ts in rows:
            try:
                record = json.loads(payload)
            except (TypeError, ValueError):
                record = {"id": rid, "ts": ts}
            self.con.execute(
                "UPDATE records SET logical_id=?, source_id=?, event_time=?,"
                " revised_time=?, revision_no=? WHERE content_hash=?",
                (self._logical_id(record), self._source_id(record),
                 self._event_time(record) or normalize_time(ts),
                 self._revised_time(record), self._revision_no(record),
                 content_hash))
        self.con.commit()

    def migration_history(self):
        history = self._meta_get("migration_history") or ""
        return [part for part in history.split(",") if part]

    def _migrate_legacy(self):
        """Upgrade pre-revision rows in place without changing their identity.

        Existing ``content_hash`` primary keys are preserved, so external
        references and duplicate detection keep working.  Missing provenance is
        filled from the stored payload and marked as rule version 1 / basis
        ``legacy``; nothing is dropped, so the old history stays traceable.
        """
        rows = self.con.execute(
            "SELECT content_hash, id, ts, payload, logical_id, source_id,"
            " event_time, receive_time, basis, rule_version, legacy_payload"
            " FROM records").fetchall()
        # Re-opening an already upgraded database must be a no-op: if every row
        # has provenance there is nothing to do and no clock tick is consumed,
        # so later reception timestamps stay identical.
        pending = [row for row in rows
                   if not (row[4] and row[5] is not None and row[6]
                           and row[7] and row[8])]
        if not pending:
            return
        # A pre-upgrade database ran under the original rule layer; record that
        # baseline so the rule-version transition is auditable.
        if self._meta_get("rule_version") is None:
            self._meta_set("rule_version", "1")
        # All untouched pre-upgrade rows belonged to the same prior history, so
        # one reception timestamp is reserved for the migration.  Rows that
        # already carry provenance are left exactly as they are, which makes a
        # partially upgraded database safe to open again.
        received = normalize_time(self._next_receive_time_on(self.con))
        for (content_hash, rid, ts, payload, logical_id, source_id, event_time,
             receive_time, basis, rule_version, legacy_payload) in pending:
            try:
                record = json.loads(payload)
            except (TypeError, ValueError):
                record = {"id": rid, "ts": ts}
            effective_event = event_time or self._event_time(record) or normalize_time(ts)
            effective_receive = normalize_time(receive_time or received)
            # Older pipeline versions wrote rule-derived fields into the
            # payload.  They are processing metadata, not received content, so
            # they are stripped before identity is recomputed; otherwise a stale
            # derived ``logical_id`` would keep two records of one event apart.
            # The untouched original payload is kept in ``legacy_payload``, so
            # the pre-upgrade record stays fully traceable.
            cleaned, changed = self._strip_derived(record)
            original_payload = legacy_payload
            if changed and original_payload is None:
                original_payload = payload
            recomputed = self._logical_id(cleaned)
            self.con.execute(
                "UPDATE records SET payload=?, legacy_payload=?, logical_id=?,"
                " source_id=?, event_time=?, revised_time=?, receive_time=?,"
                " revision_no=?, basis=?, rule_version=?, canonical_hash=?"
                " WHERE content_hash=?",
                (json.dumps(cleaned, ensure_ascii=False), original_payload,
                 recomputed,
                 source_id or self._source_id(record),
                 normalize_time(effective_event), self._revised_time(record),
                 effective_receive, self._revision_no(cleaned),
                 basis or "legacy", rule_version or "1",
                 self.hash(cleaned), content_hash))
        self.con.commit()

    @staticmethod
    def _strip_derived(record):
        """Return (record without rule-derived fields, whether any were found)."""
        found = [k for k in _DERIVED_FIELDS if k in record]
        if not found:
            return record, False
        return canonical_content(record), True

    def legacy_payload(self, content_hash):
        """Original payload of a row upgraded from a pre-revision database."""
        row = self.con.execute(
            "SELECT legacy_payload FROM records WHERE content_hash=?",
            (content_hash,)).fetchone()
        if not row or row[0] is None:
            return None
        return json.loads(row[0])

    def _sync_clock(self):
        """Keep the logical clock ahead of every stored reception time."""
        base = _parse_time(RECEIVE_BASE)
        highest = 0
        for (value,) in self.con.execute(
                "SELECT receive_time FROM records WHERE receive_time IS NOT NULL"):
            parsed = _parse_time(value)
            if parsed is not None:
                highest = max(highest, int((parsed - base).total_seconds()))
        if highest > _as_int(self._meta_get("clock"), 0):
            self._meta_set("clock", highest)

    # ------------------------------------------------------------------ #
    # Time / provenance helpers.
    # ------------------------------------------------------------------ #
    @staticmethod
    def _next_receive_time_on(con):
        row = con.execute("SELECT v FROM meta WHERE k='clock'").fetchone()
        clock = _as_int(row[0], 0) + 1 if row else 1
        con.execute(
            "INSERT INTO meta(k,v) VALUES('clock',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(clock),))
        return _format_time(_parse_time(RECEIVE_BASE) + timedelta(seconds=clock))

    def allocate_receive_time(self):
        """Reserve one deterministic reception timestamp for a delivery batch."""
        return self._next_receive_time_on(self.con)

    def latest_receive_time(self):
        row = self.con.execute("SELECT MAX(receive_time) FROM records").fetchone()
        return row[0] if row else None

    @staticmethod
    def _logical_id(record):
        for key in ("logical_id", "event_id", "earthquake_id", "id"):
            value = record.get(key)
            if value not in (None, ""):
                return str(value)
        return str(record.get("name", "")).lower()

    @staticmethod
    def _event_time(record):
        for key in _EVENT_KEYS:
            value = record.get(key)
            if value not in (None, ""):
                return normalize_time(value)
        return ""

    @staticmethod
    def _revised_time(record):
        for key in _REVISED_KEYS:
            value = record.get(key)
            if value not in (None, ""):
                return normalize_time(value)
        return ""

    @staticmethod
    def _revision_no(record):
        for key in _REVISION_KEYS:
            if key in record and record[key] not in (None, ""):
                return _as_int(record[key], 0)
        return 0

    @staticmethod
    def _basis(record):
        for key in _BASIS_KEYS:
            value = record.get(key)
            if value not in (None, ""):
                return str(value)
        return None

    @staticmethod
    def _source_id(record):
        for key in _SOURCE_KEYS:
            value = record.get(key)
            if value not in (None, ""):
                return str(value)
        return SOURCE_ID

    @staticmethod
    def hash(record):
        """Content identity used as the revision primary key."""
        return hashlib.sha256(_canonical(canonical_content(record)).encode()).hexdigest()[:24]

    def revision_id(self, record):
        return self.hash(record)

    # ------------------------------------------------------------------ #
    # Writes.
    # ------------------------------------------------------------------ #
    @contextmanager
    def batch(self):
        """Group writes into one atomic transaction (all-or-nothing)."""
        self._batch_depth += 1
        try:
            yield self
        except Exception:
            self._batch_depth -= 1
            if self._batch_depth == 0:
                self.con.rollback()
            raise
        else:
            self._batch_depth -= 1
            if self._batch_depth == 0:
                self.con.commit()

    def _commit(self):
        if self._batch_depth == 0:
            self.con.commit()

    def _find_equivalent(self, record):
        """Find a row with identical source content but another identity.

        Pre-upgrade hashes included rule-derived fields (for example the old
        ``logical_id``), so identical source content can be stored under a
        different primary key.  ``canonical_hash`` covers every migrated row;
        this fallback only inspects rows that have not been migrated yet, so
        repeated ingestion stays cheap.
        """
        wanted = _canonical(canonical_content(record))
        rows = self.con.execute(
            "SELECT content_hash, payload FROM records"
            " WHERE canonical_hash IS NULL").fetchall()
        for content_hash, payload in rows:
            try:
                stored = json.loads(payload)
            except (TypeError, ValueError):
                continue
            if _canonical(canonical_content(stored)) == wanted:
                return content_hash
        return None

    def is_known(self, record):
        content_hash = self.hash(record)
        if self._hash_row(content_hash):
            return True
        return self._find_equivalent(record) is not None

    def _hash_row(self, content_hash):
        return self.con.execute(
            "SELECT 1 FROM records WHERE content_hash=? OR canonical_hash=?",
            (content_hash, content_hash)).fetchone()

    def upsert(self, record, *, source_id=None, event_time=None,
               revised_time=None, receive_time=None, revision_no=None,
               basis=None, rule_version=None, commit=True, **_ignored):
        """Append a revision unless its source content is already stored.

        Returns ``True`` only when a new revision row was written.  Re-delivery
        of identical content is a no-op, which keeps duplicate pages, full
        re-runs and continuation runs idempotent.
        """
        content_hash = self.hash(record)
        if self._hash_row(content_hash):
            return False

        logical_id = self._logical_id(record)
        effective_event = normalize_time(
            event_time if event_time is not None else self._event_time(record))
        if self._find_equivalent(record):
            return False

        effective_revised = normalize_time(
            revised_time if revised_time is not None else self._revised_time(record))
        effective_rev = (revision_no if revision_no is not None
                         else self._revision_no(record))
        if receive_time is None:
            receive_time = record.get("receive_time")
        if receive_time is None:
            receive_time = self._next_receive_time_on(self.con)
        effective_receive = normalize_time(receive_time)

        # ``basis`` is source-provided revision evidence (why the report was
        # revised).  It is stored verbatim.  Arrival-order labels are never
        # persisted; the initial/revision distinction is derived from the
        # source-ordered chain in the read views, so it cannot depend on when
        # or in which order a revision happened to be delivered.
        effective_basis = basis or self._basis(record)

        # The payload stores the received source content only.  Rule-derived
        # fields live in their own columns, so a later rule change never
        # rewrites stored content and the identity stays stable.
        payload = json.dumps(canonical_content(record), ensure_ascii=False)
        self.con.execute(
            "INSERT INTO records(content_hash,id,ts,payload,logical_id,source_id,"
            "event_time,revised_time,receive_time,revision_no,basis,rule_version,"
            "canonical_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (content_hash, str(record.get("id", "")), str(record.get("ts", "")),
             payload, logical_id,
             source_id or self._source_id(record), effective_event,
             effective_revised, effective_receive, _as_int(effective_rev),
             effective_basis, rule_version or RULE_VERSION, content_hash))
        if commit:
            self._commit()
        return True

    # ------------------------------------------------------------------ #
    # Reads.
    # ------------------------------------------------------------------ #
    def count(self):
        return self.con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def event_count(self):
        """Number of distinct logical events, ignoring duplicate content rows."""
        return len(self._groups(self._rows()))

    def all_records(self):
        return [json.loads(p) for (_h, p) in
                self.con.execute("SELECT content_hash,payload FROM records")]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM records WHERE content_hash=?",
            (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    def _rows(self, where="", params=()):
        query = "SELECT " + ",".join(_COLUMNS) + " FROM records"
        if where:
            query += " WHERE " + where
        return [dict(zip(_COLUMNS, row))
                for row in self.con.execute(query, params).fetchall()]

    @staticmethod
    def _rank(row):
        # Source facts only; arrival order intentionally excluded.
        return (int(row.get("revision_no") or 0),
                str(row.get("revised_time") or ""),
                str(row.get("event_time") or ""),
                str(row.get("content_hash") or ""))

    @classmethod
    def _groups(cls, rows):
        """Group rows into per-event revision chains.

        Rows that describe identical source content (possible after upgrading
        a database whose old hashes included derived fields) are collapsed into
        one revision.  The earliest-received representative is kept and the
        others stay in the table as evidence; they must not look like
        simultaneous conflicting revisions.
        """
        unique = {}
        for row in rows:
            key = (row["logical_id"], row.get("canonical_hash") or row["content_hash"])
            current = unique.get(key)
            if current is None:
                unique[key] = row
                continue
            if (str(row.get("receive_time") or ""),
                    row["content_hash"]) < (str(current.get("receive_time") or ""),
                                           current["content_hash"]):
                unique[key] = row
        grouped = {}
        for row in unique.values():
            grouped.setdefault(row["logical_id"], []).append(row)
        for chain in grouped.values():
            chain.sort(key=cls._rank)
        return grouped

    @classmethod
    def _conflict_key(cls, row):
        # Everything except content identity: rows sharing it are simultaneous
        # candidates and cannot be ordered by source facts alone.
        return cls._rank(row)[:3]

    @classmethod
    def _chain_index(cls, grouped):
        """Map content hash -> authoritative chain metadata."""
        index = {}
        for chain in grouped.values():
            by_moment = {}
            for row in chain:
                by_moment.setdefault(cls._conflict_key(row), []).append(
                    row["content_hash"])
            previous = None
            for position, row in enumerate(chain):
                peers = [h for h in by_moment[cls._conflict_key(row)]
                         if h != row["content_hash"]]
                index[row["content_hash"]] = {
                    "version_no": position + 1,
                    "prev_version_id": previous,
                    "conflict": int(bool(peers)),
                    "conflict_with": sorted(peers),
                }
                previous = row["content_hash"]
        return index

    @staticmethod
    def _flatten(grouped):
        return [row for chain in grouped.values() for row in chain]

    @classmethod
    def _newest(cls, rows):
        newest = {}
        for row in rows:
            current = newest.get(row["logical_id"])
            if current is None or cls._rank(row) > cls._rank(current):
                newest[row["logical_id"]] = row
        return {logical_id: row["content_hash"]
                for logical_id, row in newest.items()}

    @classmethod
    def _view(cls, rows, index=None, scope=None):
        """Render stored rows as a view.

        ``version_no``/``prev_version_id`` are global chain evidence: the same
        ``revision_id`` reports the same position in every view.  ``is_latest``
        is relative to what the view could see: for ``latest()``/``origin()``/
        ``history()`` the scope is the full history (so it marks the chain
        head), while ``as_of(t)`` and ``initial()`` scope it to the revisions
        known at that moment.  That is what lets ``as_of(t)`` reproduce exactly
        what ``latest()`` returned at receive time ``t``.
        """
        newest = cls._newest(rows if scope is None else scope)
        out = []
        for row in rows:
            payload = json.loads(row["payload"])
            payload.update({
                "revision_id": row["content_hash"],
                "logical_id": row["logical_id"],
                "source_id": row["source_id"],
                "event_time": row["event_time"],
                "revised_time": row["revised_time"],
                "receive_time": row["receive_time"],
                "revision_no": _as_int(row["revision_no"], 0),
                "basis": row["basis"],
                "rule_version": row["rule_version"],
            })
            if index and row["content_hash"] in index:
                payload.update(index[row["content_hash"]])
            payload["is_latest"] = int(
                newest.get(row["logical_id"]) == row["content_hash"])
            if not payload.get("basis"):
                payload["basis"] = ("initial"
                                    if payload.get("version_no") == 1
                                    else "revision")
            out.append(payload)
        out.sort(key=lambda item: item["logical_id"])
        return out

    def latest(self):
        """Current view: newest valid revision for every logical event."""
        grouped = self._groups(self._rows())
        index = self._chain_index(grouped)
        chosen = [chain[-1] for chain in grouped.values()]
        return self._view(chosen, index, scope=self._flatten(grouped))

    current = latest
    current_view = latest

    def initial(self):
        """Initial-report view: what was held when each event was first seen.

        Per event this equals ``as_of`` at that event's earliest reception
        time.  Revisions that arrive later (including a late copy of an older
        revision) cannot rewrite what the initial report said, so this is an
        accurate reconstruction of the information available at first report.
        Version-chain evidence is indexed over the full history, so a
        ``revision_id`` looks identical in ``latest()``, ``initial()`` and
        ``history()``.
        """
        grouped = self._groups(self._rows())
        index = self._chain_index(grouped)
        chosen = []
        for chain in grouped.values():
            snapshot = self._first_received(chain)
            if snapshot is not None:
                chosen.append(snapshot)
        # Scope is each event's first-reception snapshot, so ``is_latest``
        # means exactly what it did at that moment.
        return self._view(chosen, index, scope=chosen)

    initial_reports = initial
    initial_view = initial

    def origin(self):
        """Source-time view: the earliest revision published for every event.

        This is the bulletin's own initial parameters, even if the local
        process first observed the event after it had already been revised.
        """
        grouped = self._groups(self._rows())
        index = self._chain_index(grouped)
        return self._view([chain[0] for chain in grouped.values()], index,
                          scope=self._flatten(grouped))

    first_reports = origin

    def as_of(self, receive_time):
        """Transaction-time view: current state as known at ``receive_time``.

        Only revisions received at or before the moment are considered, so the
        returned set is exactly what was known then.  Version-chain metadata is
        still indexed over the complete history, which keeps the evidence for a
        ``revision_id`` identical in every view.
        """
        moment = normalize_time(receive_time)
        full = self._groups(self._rows())
        visible = {}
        for logical_id, chain in full.items():
            known = [r for r in chain if str(r.get("receive_time") or "") <= moment]
            if known:
                visible[logical_id] = known
        chosen = [chain[-1] for chain in visible.values()]
        # Scope to the visible rows: ``is_latest`` marks the newest revision
        # known at that moment, exactly as ``latest()`` reported it.
        return self._view(chosen, self._chain_index(full), scope=chosen)

    snapshot = as_of

    def history(self, logical_id):
        """Ordered revision chain for one event, oldest source revision first.

        ``version_no``/``prev_version_id`` describe the position in the chain
        and ``is_latest`` marks its head, so the evidence lines up with
        ``latest()`` and ``as_of()``.
        """
        rows = self._groups(self._rows("logical_id=?", (logical_id,))).get(
            str(logical_id), [])
        return self._view(rows, self._chain_index({str(logical_id): rows}))

    revisions = history

    def explain(self, logical_id):
        """Provenance and revision evidence for one logical event."""
        chain = self.history(logical_id)
        receive_times = [r["receive_time"] for r in chain if r.get("receive_time")]
        return {
            "logical_id": str(logical_id),
            "sources": sorted({r["source_id"] for r in chain if r.get("source_id")}),
            "revision_count": len(chain),
            "revision_ids": [r["revision_id"] for r in chain],
            "first_received": min(receive_times, default=None),
            "latest_received": max(receive_times, default=None),
            "origin_revision_id": chain[0]["revision_id"] if chain else None,
            "latest_revision_id": chain[-1]["revision_id"] if chain else None,
            "initial_revision_id": (self._first_received(chain) or {}).get(
                "revision_id"),
            "conflicts": sorted({
                tuple(sorted([r["revision_id"]] + r["conflict_with"]))
                for r in chain if r.get("conflict")}),
            "history": chain,
        }

    @staticmethod
    def _first_received(chain):
        if not chain:
            return None
        arrivals = [r["receive_time"] for r in chain if r.get("receive_time")]
        first = min(arrivals, default=None)
        snapshot = ([r for r in chain if r["receive_time"] == first]
                    if first is not None else chain)
        return snapshot[-1]

    @staticmethod
    def digest(view):
        """Stable digest of a rendered view, for consistency checks."""
        material = [
            (item.get("logical_id"), item.get("revision_id"), item.get("version_no"),
             item.get("is_latest"), item.get("source_id"), item.get("event_time"),
             item.get("receive_time"), item.get("basis"))
            for item in view
        ]
        return hashlib.sha256(_canonical(material).encode()).hexdigest()[:24]

    # ------------------------------------------------------------------ #
    # Watermark / rule version / compatibility helpers.
    # ------------------------------------------------------------------ #
    def watermark(self):
        row = self.con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone()
        return row[0] if row else None

    def set_watermark(self, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES('watermark',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(value),))
        self._commit()

    def rule_version(self):
        return self._meta_get("rule_version") or RULE_VERSION

    def set_rule_version(self, value):
        self._meta_set("rule_version", value)
        self._commit()

    def reset(self):
        """Remove all data and processing metadata.

        The schema/rule version of the (still current) database is re-recorded
        so a reset database is immediately consistent, while the logical clock
        and any migration/rule history start fresh.
        """
        self.con.execute("DELETE FROM records")
        self.con.execute("DELETE FROM meta")
        self._meta_set("schema_version", SCHEMA_VERSION)
        self._meta_set("rule_version", RULE_VERSION)
        self.con.commit()
