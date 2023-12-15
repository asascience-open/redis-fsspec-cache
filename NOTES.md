# Notes

We need two pieces to get this working:

1. A Cache implementation, derived from `BaseCache` to store and retreive chunks from redis
2. A Filesystem implementation, derived from `BaseFilesystem` that will wrap `_open` to cache file as it is read.

Questions: 

1. Do we need to make a block cache at all?
2. What about async? 