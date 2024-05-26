from typing import Optional
import hashlib
import json
import pickle
from redis import Redis
from fsspec.implementations.reference import ReferenceFileSystem


class RedisCachingReferenceFileSystem(ReferenceFileSystem):
    """
    A reference filesystem that caches files in a redis instance. This is 
    useful for running serverless web apps where the same "chunks" of data
    are being requested multiple times across multiple difference worker
    threads that do not share memory. This is a drop in replacement for 
    fsspec.implementations.reference.ReferenceFileSystem to be used with 
    xarray.

    The async methods do not seem to be called by xarray, so this class does
    not implement cacheing on the async methods. In the future this may have 
    to change, TBD
    """

    protocol = "rediscachedreference"

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis: Optional[Redis] = None,
        expiry_time: int = 604800,
        cache_key_prefix: str = "fsspec-redis-cache",
        **kwargs,
    ) -> None:
        """
        Parameters
        ----------
        redis_host : str
            The hostname of the redis instance.
        redis_port : int
            The port of the redis instance.
        redis: Redis
            A redis client to use as a backend. If not provided, one will be
            created using the host and port. This must me an async redis client
            from redis-py redis.async module
        expiry_time: int
            The time in seconds after which a redis copy is considered useless.
            Set to falsy to prevent expiry. The default is equivalent to one
            week.
        cache_key_prefix : str
            The prefix to use for the keys in the redis cache. This is useful
            when using the same redis instance for multiple caches. The default
            key prefix is `fsspec-redis-cache`.
        kwargs : dict
            keyword arguments to pass to the target reference filesystem (The same arguments as ReferenceFileSystem)
        """
        fo = kwargs.get("fo", 'unknown')
        if isinstance(fo, str):
            self.source = fo
        elif isinstance(fo, dict):
            self.source = hashlib.md5(json.dumps(fo).encode()).hexdigest()
        else:
            self.source = "unknown"

        if redis is None:
            self.redis = Redis(host=redis_host, port=redis_port, db=0)
        else:
            self.redis = redis

        self.expiry = expiry_time
        self.cache_key_prefix = cache_key_prefix

        try:
            super().__init__(**kwargs)
        except Exception as e:
            # if init encountered an error, invalidate cache and try again
            # this can happen if an s3 file has changed since we cached it,
            # leading to a mismatched etag and s3fs.utils.FileExpired exception
            self.invalidate_cache()
            super().__init__(**kwargs)


    def cat(self, path, recursive=False, on_error="raise", **kwargs):
        cached = self._get_cached(path)
        if cached is not None:
            return cached

        chunk = super().cat(path, recursive=recursive, on_error=on_error, **kwargs)
        self._put_cache(chunk, path)
        return chunk

    def _cache_key(self, path, start=None, end=None):
        """
        Returns the cache key for the given path.
        """
        key = f"{self.cache_key_prefix}-{self.source}-{path}"
        if start is not None:
            key += f"-{start}"
        if end is not None:
            key += f"-{end}"
        return key

    def _get_cached(self, path, start=None, end=None) -> bytes | None:
        """
        Attempts to fetch the file from the cache, returning None if it
        is not available.
        """
        key = self._cache_key(path, start, end)
        data = self.redis.get(key)
        if data is None:
            return None
        return pickle.loads(data)

    def _put_cache(self, data, path, start=None, end=None):
        """
        Caches the file data for the given path.
        """
        key = self._cache_key(path, start, end)
        self.redis.set(key, pickle.dumps(data), ex=self.expiry)

    def _cached_keys(self):
        """ 
        Returns the keys of all the cached files
        """
        keys = self.redis.keys(f"{self.cache_key_prefix}-*")
        return keys

    def invalidate_cache(self):
        """
        Invalidates the cache for the current filesystem. All cached data
        with the same cache_key_prefix will be deleted.
        """
        keys = self.redis.keys(f"{self.cache_key_prefix}-*")
        if keys:
            self.redis.delete(*keys)
