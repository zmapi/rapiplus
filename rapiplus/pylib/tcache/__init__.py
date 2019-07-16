from .core import Cache

def open(fn, mode="c", max_timedelta=None):
    return Cache(fn, mode, max_timedelta)

def ensure_exists(fn):
    try:
        Cache(fn, "r").close()
    except FileNotFoundError:
        Cache(fn, "c").close()
