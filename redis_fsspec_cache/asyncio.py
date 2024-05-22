import json
import re
from typing import Callable, ClassVar, Optional
from fsspec import filesystem
from fsspec.asyn import AsyncFileSystem

from redis.asyncio import Redis


class RedisAsyncCachingFilesystem(AsyncFileSystem):
    """An async fsspec filesystem that caches reads in a Redis instance.

    Instead of using an implementation of the fsspec caching interface, this
    class uses a Redis instance to cache reads from the target filesystem. This
    is useful when the target filesystem is not seekable, and so cannot be
    cached using the standard fsspec caching interface.

    This class requires that the target filesystem is an async filesystem, as
    and it only implements the async interface. All file operations are passed
    through to the target filesystem, and the result is cached in Redis when
    applicable.
    """

    protocol: ClassVar[str | tuple[str, ...]] = "redisasynccached"

    def __init__(
        self,
        redis_host: str = "localhost",
        redis_port: int = 6379,
        redis: Optional[Redis] = None,
        expiry_time: int = 604800,
        cache_key_prefix: str = "fsspec-redis-cache",
        target_protocol: Optional[str] = None,
        target_options: Optional[dict] = None,
        fs: Optional[AsyncFileSystem] = None,
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
        target_protocol : str
            The protocol to use for the target filesystem.
        target_options : dict
            The options to use for the target filesystem.
        fs : fsspec.asyn.AsyncFileSystem
            Directly provide a filesystem to use as the target.
        kwargs : dict
            Additional keyword arguments to pass to the target filesystem.
        """
        super().__init__(**kwargs)

        if fs is None and target_protocol is None:
            raise ValueError(
                "Please provide filesystem instance(fs) or target_protocol"
            )
        if not (fs is None) ^ (target_protocol is None):
            raise ValueError(
                "Both filesystems (fs) and target_protocol may not be both given."
            )

        if redis is None:
            self.redis = Redis(host=redis_host, port=redis_port, db=0)
        else:
            self.redis = redis

        self.kwargs = target_options or {}
        self.expiry = expiry_time
        self.cache_key_prefix = cache_key_prefix

        self.target_protocol = (
            target_protocol
            if isinstance(target_protocol, str)
            else (fs.protocol if isinstance(fs.protocol, str) else fs.protocol[0])
        )
        self.fs = fs if fs is not None else filesystem(target_protocol, **self.kwargs)

        def _strip_protocol(path):
            # acts as a method, since each instance has a difference target
            return self.fs._strip_protocol(type(self)._strip_protocol(path))

        self._strip_protocol: Callable = _strip_protocol

    async def _cat_file(self, path, start=None, end=None, **kwargs):
        cached = await self._get_cached(path, start, end)
        if cached is not None:
            return json.loads(cached)
        chunk = await self.fs._cat_file(path, start=start, end=end, **kwargs)
        chunk = re.sub(r'^\s*\{\s*"version"\s*:', '{"source":"' + path + '","version":', chunk.decode())
        await self._put_cache(chunk.encode('utf-8'), path, start, end)

        clean_chunk = json.loads(chunk)
        return clean_chunk

    async def _cp_file(self, path1, path2, **kwargs):
        return await self.fs._cp_file(path1, path2, **kwargs)

    async def _get_file(self, rpath, lpath, **kwargs):
        return await self.fs._get_file(rpath, lpath, **kwargs)

    async def _info(self, path, **kwargs):
        return await self.fs._info(path, **kwargs)

    async def _ls(self, path, detail=False, **kwargs):
        return await self.fs._ls(path, detail=detail, **kwargs)

    async def _put_file(self, lpath, rpath, **kwargs):
        # Something cache here
        return await self.fs._put_file(lpath, rpath, **kwargs)

    async def _mkdir(self, path, create_parents=True, **kwargs):
        return await self.fs._mkdir(path, create_parents, **kwargs)

    async def _makedirs(self, path, exist_ok=False):
        return await self.fs._makedirs(path, exist_ok)

    async def _pipe_file(self, path, value, **kwargs):
        return await self.fs._pipe_file(path, value, **kwargs)

    async def _rm_file(self, path, **kwargs):
        return await self.fs._rm_file(path, **kwargs)

    async def open_async(self, path, mode="rb", **kwargs):
        return await self.fs.open_async(path, mode, **kwargs)

    def _cache_key(self, path, start=None, end=None):
        """
        Returns the cache key for the given path.
        """
        key = f"{self.cache_key_prefix}-{path}"
        if start is not None:
            key += f"-{start}"
        if end is not None:
            key += f"-{end}"
        return key

    async def _get_cached(self, path, start=None, end=None) -> bytes | None:
        """
        Attempts to fetch the file from the cache, returning None if it
        is not available.
        """
        key = self._cache_key(path, start, end)
        return await self.redis.get(key)

    async def _put_cache(self, data, path, start=None, end=None):
        """
        Caches the file data for the given path.
        """
        key = self._cache_key(path, start, end)
        await self.redis.set(key, data, ex=self.expiry)

    async def _cached_keys(self):
        """ 
        Returns the keys of all the cached files
        """
        keys = await self.redis.keys(f"{self.cache_key_prefix}-*")
        return keys

    async def invalidate_cache(self):
        """
        Invalidates the cache for the current filesystem. All cached data
        with the same cache_key_prefix will be deleted.
        """
        keys = await self.redis.keys(f"{self.cache_key_prefix}-*")
        if keys:
            await self.redis.delete(*keys)

    async def close_redis(self):
        """
        Closes the redis connection.
        """
        await self.redis.close()
