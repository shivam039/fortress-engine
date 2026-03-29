# engine/utils/caching.py
import time
from functools import wraps
import collections

class TTLCache:
    def __init__(self, ttl: int = 600):
        self.ttl = ttl
        self.cache = {}
        self.expiry = {}

    def get(self, key):
        if key in self.cache:
            if time.time() < self.expiry[key]:
                return self.cache[key]
            else:
                del self.cache[key]
                del self.expiry[key]
        return None

    def set(self, key, value):
        self.cache[key] = value
        self.expiry[key] = time.time() + self.ttl

def ttl_cache(ttl: int = 600):
    cache = TTLCache(ttl=ttl)
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            cached_val = cache.get(key)
            if cached_val is not None:
                return cached_val
            result = func(*args, **kwargs)
            cache.set(key, result)
            return result
        return wrapper
    return decorator
