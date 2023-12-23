# redis_fsspec_cache

A prototype redis based filesystem cache for fsspec

### Motivation

`fsspec` currently contains a filesystem cache that is based on a local filesystem, as well as in memory caches. As we start to deploy python services to serverless platforms, we need a way to share a cache between multiple instances of a service. This package provides a filesystem cache that uses redis as a backend, allowing multiple instances of a service to share a cache. 

Specifically, this package looks to improve api route response times when building services with [`xpublish`](https://github.com/xpublish-community/xpublish) deployed to serverless environments. 

## Installation

```bash
pip install git+https://github.com/mpiannucci/redis-fsspec-cache.git
```

## Usage

```python
from redis_fsspec_cache import RedisCachingFileSystem

fs = RedisCachingFileSystem(
    redis_host="localhost",
    redis_port=6380,
    expiry_time=60,
    target_protocol="s3",
    target_options={
        'anon': True,
    },
)
```

When a block is cached, it will be visible in redis using the `KEYS` command:

```bash
KEYS *
1) "noaa-hrrr-bdp-pds/hrrr.20230927/conus/hrrr.t00z.wrfsubhf00.grib2-0"
```