"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import hashlib
import importlib
import json

from lxml import html as _lh

from .config import SPEC
from .store import Store, canonical_content


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
# Non-column fields the source may attach to a revision for provenance.
_EXTRA_FIELDS = ("revision_no", "revision", "rev", "version", "revision_reason",
                 "basis", "reason", "revised_at", "revision_time", "updated_at",
                 "update_time", "source_id", "source", "feed",
                 "event_time", "occurred_at")


def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        rows = []
        for it in items:
            row = {c: _coerce(c, it.get(c)) for c in cols}
            # Optional revision evidence travels alongside the source columns.
            for key in _EXTRA_FIELDS:
                if key in it and key not in row:
                    row[key] = it[key]
            rows.append(row)
        return rows
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


class FetchError(RuntimeError):
    """Raised when a page cannot be retrieved; nothing has been written."""


def fetch_all(http: Http):
    """Collect records from the local source.

    Pages are read until an under-full page is seen.  Any non-200 page (after
    bounded retries) aborts the whole collection with :class:`FetchError`, so a
    transient cross-page failure never yields a truncated snapshot.  Identical
    pages are still returned as-is; content identity in the store makes them
    harmless and keeps duplicate accounts visible to callers.
    """
    records, attempts, page = [], 0, 1
    while True:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        attempts += att + 1
        if status != 200:
            raise FetchError("fetch failed status=%s page=%s" % (status, page))
        page_recs = parse(body, SPEC.fmt)
        records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            break
        page += 1
        if page > 200:
            break
    return records, attempts


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(canonical_content(d), sort_keys=True, ensure_ascii=False)


def content_hash(d):
    """Content identity, ignoring processing-derived fields.

    Matches :meth:`Store.hash` so deduplication during fetch and revision
    identity in storage always agree.
    """
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def _ingest(store, records, *, mode="full", source_id=None):
    """Normalize, dedupe and append one fetched batch atomically.

    A single reception timestamp is reserved for the whole batch, so the
    transaction-time view can reconstruct exactly what one delivery contained.
    The event-time watermark is only ever advanced; it is never used to discard
    unseen content, because a late revision legitimately carries an older
    event time and must still be recorded.
    """
    norm = [normalize(dict(r)) for r in records]
    norm, dup_in_batch = dedup(norm)
    norm = _apply_kernel(norm)

    # Only consume a reception timestamp when the delivery actually adds
    # revisions.  A no-op re-run then leaves the logical clock untouched, so
    # later continuations receive the same timestamps as if the redundant run
    # had never happened.
    pending = [r for r in norm if not store.is_known(r)]
    ingested = 0
    if pending:
        received = store.allocate_receive_time()
        with store.batch():
            for r in pending:
                if store.upsert(r, source_id=source_id, receive_time=received):
                    ingested += 1
    else:
        received = store.latest_receive_time()

    # The event-time watermark is a progress marker and only ever moves
    # forward, so a late revision carrying an older event time cannot make it
    # regress.  It is never used to filter records.
    observed = [str(r.get("ts", "")) for r in norm if str(r.get("ts", ""))]
    current = store.watermark()
    candidates = observed + ([current] if current else [])
    if candidates:
        advanced = max(candidates)
        if advanced != current:
            store.set_watermark(advanced)
    return norm, dup_in_batch, ingested, received


def run(mode="full", con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    try:
        # fetch_all only returns once every page succeeded.  A failure on any
        # page aborts the whole run before anything is written, so a partial
        # delivery can never be mistaken for a complete snapshot.
        #
        # ``incremental`` is kept as a compatibility alias.  It performs the
        # same fetch and relies on content identity for idempotency instead of
        # filtering on the event-time watermark: a late revision may carry an
        # older event time and must still be recorded.
        records, attempts = fetch_all(http)
        norm, dup_in_batch, ingested, received = _ingest(
            store, records, mode=mode)

        total = store.count()
        return {"engine": "crawl", "mode": mode, "fetched": len(records),
                "duplicates": dup_in_batch, "ingested": ingested, "count": total,
                "http_attempts": attempts, "slept": round(sleeper.slept, 4),
                "watermark": store.watermark(), "receive_time": received,
                "events": store.event_count(), "revisions": total,
                "schema_version": store.schema_version(),
                "rule_version": store.rule_version()}
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        status, _h, body = self.t.request("POST", "/captcha",
                                          json_body={"code": self.t.cookies.get("challenge", "")},
                                          headers=self._csrf())
        ok = status == 200 and json.loads(body).get("ok") is True
        self.log.append("captcha:%s" % ("solved" if ok else "failed"))
        return ok

    def submit(self, record):
        # The token identifies the revision content, not the processing state,
        # so re-submitting an unchanged revision stays a server-side dedup.
        token = content_hash(record)
        status, _h, body = self.t.request("POST", "/submit",
                                          json_body={"token": token, "record": record},
                                          headers=self._csrf())
        data = json.loads(body)
        self.log.append("submit:%s" % ("created" if data.get("created") else "dedup"))
        return bool(data.get("created"))


def run_rpa(con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    r = Rpa()
    try:
        logged = r.login()
        items = r.collect()
        norm, dup, ingested, received = _ingest(store, items)
        total = store.count()
        created = dedup_submit = 0
        for x in norm[:2]:
            if r.submit(x):
                created += 1
            else:
                dedup_submit += 1
        for x in norm[:1]:
            if not r.submit(x):
                dedup_submit += 1
        return {"engine": "rpa", "logged_in": logged, "captcha_solved": r.captcha_solved,
                "collected": len(items), "fetched": len(items), "duplicates": dup,
                "ingested": ingested, "count": total, "events": store.event_count(),
                "revisions": total, "receive_time": received,
                "submit_created": created, "submit_dedup": dedup_submit, "log": r.log}
    finally:
        if own:
            con.close()
