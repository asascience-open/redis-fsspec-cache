from typing import Callable
from fsspec.caching import BaseCache

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
    """

    name = "redisblockcache"

    def __init__(
        self, blocksize: int, fetcher: Fetcher, size: int, maxblocks: int = 32
    ) -> None:
        super().__init__(blocksize, fetcher, size, maxblocks)

    def __repr__(self) -> str:
        return (
            f"<RedisBlockCache blocksize={self.blocksize}, "
            f"size={self.size}, nblocks={self.nblocks}>"
        )
    
    def _fetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None:
            end = self.size
        if start >= self.size or start >= end:
            return b""
        
        # byte position -> block numbers
        start_block_number = start // self.blocksize
        end_block_number = end // self.blocksize