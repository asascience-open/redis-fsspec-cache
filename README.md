# redis_fsspec_cache

A simple redis based filesystem cache for fsspec

### Motivation

`fsspec` currently contains a filesystem cache that is based on a local filesystem, as well as in memory caches. As we start to deploy python services to serverless platforms, we need a way to share a cache between multiple instances of a service. This package provides a filesystem cache that uses redis as a backend, allowing multiple instances of a service to share a cache. 

Specifically, this package looks to improve api route response times when building services with [`xpublish`](https://github.com/xpublish-community/xpublish) deployed to serverless environments. 

## Installation

```bash
pip install git+https://github.com/mpiannucci/redis-fsspec-cache.git
```

## Usage

### Kerchunk (Reference filesystem)

When using this package with ReferenceFileSystem from `fsspec`, use the `RedisCachingReferenceFileSystem` class specifically made to cache 
the reference mapped file chunks: 

```python
from redis import Redis
from redis_fsspec_cache.reference import RedisCachingReferenceFileSystem

redis = Redis(host="localhost", port=6380)

new_cached_fs = RedisCachingReferenceFileSystem(
    redis=redis,
    expiry_time=60,
    fo='s3://nextgen-dmac-cloud-ingest/nos/ngofs2/nos.ngofs2.fields.best.nc.zarr', 
    remote_protocol='s3', 
    remote_options={'anon':True}, 
    target_protocol='s3', 
    target_options={'anon':True}, 
    asynchronous=True, 
)
```

See [xarray_usage.ipynb](./examples/xarray_kerchunk_usage.ipynb) for a more detailed example of usage with xarray and zarr.

### Synchronous (Traditional File Systems)

```python
from redis_fsspec_cache import RedisCachingFileSystem

fs = RedisCachingFileSystem(
    redis_host="localhost",
    redis_port=6380,
    expiry_time=60,
    method="chunk"
    target_protocol="s3",
    target_options={
        'anon': True,
    },
)
```

When a block or chunk is cached, it will be visible in redis using the `KEYS` command:

```bash
KEYS *
1) "noaa-hrrr-bdp-pds/hrrr.20230927/conus/hrrr.t00z.wrfsubhf00.grib2-0"
```

You can also use the protocol directly

```python
with fsspec.open(
    "rediscache::s3://nextgen-dmac-cloud-ingest/nos/ngofs2/nos.ngofs2.fields.best.nc.zarr",
    mode="r",
    s3={"anon": True},
    rediscache={"redis_port": 6380, "expiry": 60},
) as f:
    # Do stuff with f
```

#### Block vs Chunk Caching

The `method` parameter controls whether the cache will store blocks or chunks. When `method="block"`, the cache will store each file block as a separate key in redis. When `method="chunk"`, the cache will store each file chunk as a separate key in redis. This distinction is important when considering the size of target chunks, for example when accessing GRIB or NetCDF files from cloud storage, where data is accessed as specific chunks at predetermined byte ranges. In this scenario, blocks may not map directly to the target chunks, and so may result in more data being fetched and cached than is necessary.

See [sync_usage.ipynb](./examples/sync_usage.ipynb) for a simple example.

### Asynchronous (unstable, unfinished?)

Asynchronous mode is also supported via the `RedisAsyncCachingFileSystem` class. This is separate from the `RedisCachingFileSystem` class, as it requires a different redis client workflow and thus doesnt work with `fsspec`'s `AsyncFileSystem` class. 

See [async_usage.ipynb](./examples/async_usage.ipynb) for a simple example.
