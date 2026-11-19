"""Rule layer: derive stable logical identity for source records.

The kernel is intentionally free of batch state.  It used to number versions
by position inside the current fetch, which made the same record appear as a
different version depending on delivery order.  Version chains are now owned
by the store and derived from source facts, so this module only annotates the
information that is intrinsic to a single record.

``process`` is the public entry point and stays backward compatible: it
returns a dict that is merged into the record.
"""
from __future__ import annotations


def logical_id(record):
    """Stable identity of the event a revision describes."""
    for key in ("logical_id", "event_id", "earthquake_id", "id"):
        value = record.get(key)
        if value not in (None, ""):
            return str(value)
    return str(record.get("name", "")).lower()


def process(record):
    """Attach rule-derived fields to one record (no cross-record state)."""
    return {"logical_id": logical_id(record)}


def reduce(records):
    """Kept for API compatibility; deliberately a pure pass-through.

    Version numbering and latest selection happen in :class:`Store` where the
    full revision history is available.  Numbering a single batch here would
    reintroduce fetch-order dependent versions.
    """
    return records
