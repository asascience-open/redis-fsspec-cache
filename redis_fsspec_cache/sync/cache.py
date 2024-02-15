from typing import Callable
from fsspec.caching import BaseCache
from redis import Redis

# IDK not exported from fsspec
# https://github.com/fsspec/filesystem_spec/blob/5df4b0b30dc011f9d6eceded5a078e92b2b5c11d/fsspec/caching.py#L36
Fetcher = Callable[[int, int], bytes]  # Maps (start, end) to bytes


class RedisBlockCache(BaseCache):
    """A block cache that uses Redis as a backend.

    Adapted from fsspec.caching.BlockCache which uses an inmemory LRUCache as a backend

    Parameters
    ----------
    blocksize : int
        The number of bytes to store in each block.
        Requests are only ever made for ``blocksize``, so this
        should balance the overhead of making a request against
        the granularity of the blocks.
    fetcher : Callable
    size : int
        The total size of the file being cached.
    maxblocks : int
        The maximum number of blocks to cache for. The maximum memory
        use for this cache is then ``blocksize * maxblocks``.
    filename : str
        The name of the file to use as a key prefix in redis.
    redis : Redis
        A redis client to use as a backend.
    expiry : int
        The time in seconds after which a redis copy is considered useless.
        Set to false to prevent expiry. The default is equivalent to one
        week.
    """

    name = "redisblockcache"

    def __init__(
        self,
        blocksize: int,
        fetcher: Fetcher,
        size: int,
        filename: str = None,
        redis: Redis = None,
        expiry: int = 604800,
    ) -> None:
        super().__init__(blocksize, fetcher, size)
        self.redis = redis
        self.filename = filename
        self.expiry = expiry

    def __repr__(self) -> str:
        return (
            f"<RedisBlockCache blocksize={self.blocksize}, "
            f"size={self.size}, nblocks={self.nblocks}>"
        )

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        if start is None:
            start = 0
        if stop is None:
            stop = self.size
        if start >= self.size or start >= stop:
            return b""

        # byte position -> block numbers
        start_block_number = start // self.blocksize
        start_block_offset = start % self.blocksize
        end_block_number = stop // self.blocksize

        blocks = bytearray()
        for i_block in range(start_block_number, end_block_number + 1):
            # TODO: ASYNC???
            block = self._fetch_cache_block(i_block)
            blocks.extend(block)

        all_bytes = bytes(blocks)

        return all_bytes[start_block_offset:start_block_offset + (stop - start)]

    def _fetch_cache_block(self, i_block: int) -> bytes:
        block = self.redis.get(f"{self.filename}-{i_block}")
        if block is None:
            block = self.fetcher(
                i_block * self.blocksize, (i_block + 1) * self.blocksize
            )
            self.redis.set(f"{self.filename}-{i_block}", block, ex=self.expiry)
        return block


class RedisChunkCache(BaseCache):
    """A cache that uses Redis as a backend and caches exact chunks.

    Chunks are cached as ranges are requested exactly. This is specifically useful 
    when using reference filesystems that are not seekable and can only be read in chunks.

    Parameters
    ----------
    blocksize : int
        The number of bytes to store in each block.
        Requests are only ever made for ``blocksize``, so this
        should balance the overhead of making a request against
        the granularity of the blocks.
    fetcher : Callable
    size : int
        The total size of the file being cached.
    filename : str
        The name of the file to use as a key prefix in redis.
    redis : Redis
        A redis client to use as a backend.
    expiry : int
        The time in seconds after which a redis copy is considered useless.
        Set to false to prevent expiry. The default is equivalent to one
        week.
    """

    name = "redischunkcache"

    def __repr__(self) -> str:
        return (
            "<RedisChunkCache>"
        )
    
    def __init__(
        self,
        blocksize: int,
        fetcher: Fetcher,
        size: int,
        filename: str = None,
        redis: Redis = None,
        expiry: int = 604800,
    ) -> None:
        super().__init__(blocksize, fetcher, size)
        self.redis = redis
        self.filename = filename
        self.expiry = expiry

    def _fetch(self, start: int | None, stop: int | None) -> bytes:
        if start is None:
            start = 0
        if stop is None:
            stop = self.size
        if start >= self.size or start >= stop:
            return b""
        
        chunk = self.redis.get(f"{self.filename}-{start}-{stop}")
        if chunk is None:
            chunk = self.fetcher(start, stop)
            self.redis.set(f"{self.filename}-{start}-{stop}", chunk, ex=self.expiry)
        return chunk