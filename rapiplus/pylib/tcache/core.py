import shelve
import filock
import trace
import collections
import os
from datetime import datetime, timedelta

class ExpirationException(Exception):
    pass

class Cache(collections.MutableMapping):

    def __init__(self, path, mode="c", max_timedelta=None, delete_expired=True):
        if max_timedelta is None:
            max_timedelta = timedelta.max
        self.max_timedelta = max_timedelta
        self.delete_expired = delete_expired
        lock_fn = path + "~" 
        if mode == "r":
            self.lock = filock.open(lock_fn, "r")
            if not os.path.isfile(path):
                raise FileNotFoundError(path)
        else:
            self.lock = filock.open(lock_fn, "w")
        self.shelve = shelve.open(path, mode)

    def close(self):
        if hasattr(self, "shelve"):
            self.shelve.close()
        if hasattr(self, "lock"):
            self.lock.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __contains__(self, key):
        return key in self.shelve

    def __getitem__(self, key):
        value = self.get(key)
        if value is None:
            raise KeyError(key)
        return value

    def __delitem__(self, key):
        del self.shelve[key]

    def __len__(self):
        return len(self.shelve)

    def __iter__(self):
        return self.shelve.__iter__()

    def __setitem__(self, key, value):
        holder = {
            "data": value,
            "timestamp": datetime.utcnow(),
            "frozen": False,
        }
        self.shelve[key] = holder

    def get(self, key, default=None, max_timedelta=None):
        holder = self.shelve.get(key)
        if not holder:
            return default
        if holder["frozen"]:
            return holder["data"]
        if max_timedelta is None:
            max_timedelta = self.max_timedelta
        now = datetime.utcnow()
        if now - holder["timestamp"] > max_timedelta:
            # if self.delete_expired:
            #     try:
            #         del self[key]
            #     except KeyError:
            #         # read-only access
            #         pass
            return default
        return holder["data"]

    def update(self, d):
        for k, v in d.items():
            self[k] = v

    def freeze(self, key):
        holder = self.shelve[key]
        holder["frozen"] = True
        self.shelve[key] = holder 

    def unfreeze(self, key):
        holder = self.shelve[key]
        holder["frozen"] = False
        self.shelve[key] = holder 

    def clear_expired(self, max_timedelta=None):
        res = []
        keys = self.keys()
        if max_timedelta is None:
            max_timedelta = self.max_timedelta
        now = datetime.utcnow()
        for k in keys:
            holder = self.shelve.get(k)
            if holder["frozen"]:
                continue
            if now - holder["timestamp"] > max_timedelta:
                del self[k]
                res.append(k)
        # reorganize underlying data structure to reclaim space
        self.shelve.dict.reorganize()
        return res
