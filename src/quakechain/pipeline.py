"""Public orchestration entry point."""
from . import framework


def run(mode="full", con=None):
    from .config import SPEC
    if SPEC.engine == "rpa":
        return framework.run_rpa(con=con)
    return framework.run(mode=mode, con=con)


def _store(con=None):
    from .store import Store
    own = con is None
    con = con or Store().connect()
    return Store(con), con, own


def latest(con=None):
    """Current view: newest valid revision for every logical event."""
    store, con, own = _store(con)
    try:
        return store.latest()
    finally:
        if own:
            con.close()


def initial(con=None):
    """Initial-report view: what was first received for every event."""
    store, con, own = _store(con)
    try:
        return store.initial()
    finally:
        if own:
            con.close()


def origin(con=None):
    """Source-time view: the earliest published revision per event."""
    store, con, own = _store(con)
    try:
        return store.origin()
    finally:
        if own:
            con.close()


def as_of(receive_time, con=None):
    """Reconstruct the current view as it stood at ``receive_time``."""
    store, con, own = _store(con)
    try:
        return store.as_of(receive_time)
    finally:
        if own:
            con.close()


def digest(view):
    """Stable digest for comparing views across runs."""
    from .store import Store
    return Store.digest(view)


def explain(logical_id, con=None):
    """Provenance-annotated revision evidence for one event."""
    store, con, own = _store(con)
    try:
        return store.explain(logical_id)
    finally:
        if own:
            con.close()


def history(logical_id, con=None):
    store, con, own = _store(con)
    try:
        return store.history(logical_id)
    finally:
        if own:
            con.close()
