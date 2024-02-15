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
        redis_host: Optional[str] = "localhost",
        redis_port: Optional[int] = 6379,
        redis: Optional[Redis] = None,
        expiry_time: Optional[int] = 604800,
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
        # TODO: Cache here
        return await self.fs._cat_file(path, start=start, end=end, **kwargs)

    async def _cp_file(self, path1, path2, **kwargs):
        return await self.fs._cp_file(path1, path2, **kwargs)

    async def _get_file(self, rpath, lpath, **kwargs):
        # TODO: Maybe cache here?
        return await self.fs._get_file(rpath, lpath, **kwargs)
    
    async def _info(self, path, **kwargs):
        return await self.fs.info(path, **kwargs)
    
    async def _ls(self, path, detail=False, **kwargs):
        return await self.fs.ls(path, detail=detail, **kwargs)
    
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
    
    def _open(self, path, mode="rb", block_size=None, autocommit=True, cache_options=None, **kwargs):
        return self._open(path, mode=mode, block_size=block_size, autocommit=autocommit, cache_options=cache_options, **kwargs)
