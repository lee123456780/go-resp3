# go-resp3

[![GoDoc](https://godoc.org/github.com/d024441/go-resp3/client?status.png)](https://godoc.org/github.com/d024441/go-resp3/client)
[![Go Report Card](https://goreportcard.com/badge/github.com/d024441/go-resp3)](https://goreportcard.com/report/github.com/d024441/go-resp3)
![](https://github.com/d024441/go-resp3/workflows/build/badge.svg)
![](https://github.com/d024441/go-resp3/workflows/test/badge.svg)

go-resp3 client is a Go implementation of the [Redis](https://redis.io/) [RESP3 protocol](https://github.com/antirez/RESP3).
It is intended as a simple Go wrapper for Redis commands and is not going to support
* former Redis protocols (RESP3 only).
* Redis cluster protocol (allthough cluster commands are implemented) - please use [Redis Cluster Proxy](https://github.com/artix75/redis-cluster-proxy) instead.   

## Installation

```
go get github.com/d024441/go-resp3/client
```

## Building

To build go-resp3 you need to have a working Go environment with [version 1.13.x or higher installed](https://golang.org/dl/).

## Documentation

API documentation and documented examples can be found at <https://godoc.org/github.com/d024441/go-resp3/client>.

## Tests

To run the driver tests and benchmarks a running Redis server (version >= 6.0) is required.
For the Redis connection localhost (127.0.0.1) and the default Redis port is used.
The following environment variables can be set to use alternative host and port addresses:
- REDIS_HOST
- REDIS_PORT 

## Features

* Full RESP3 implementation supporting receiving attributes, streamed strings and streamed aggregate types.
* Standardized generated command interface.
* Asynchronous client with concurrent read / write supporting commands and out of band data within same connection.
* Redis pipeline support.
* Redis server-assisted client side caching.
* Support Redis RESP3 out of bound data: Pubsub, Monitor and key slot invalidations (cache).
* Extendable via custom connection and pipeline (please see [example](https://godoc.org/github.com/d024441/go-resp3/client#ex-package--Redefine>)).
* Redis 6 TLS (SSL) support (please see [example](https://github.com/d024441/go-resp3/blob/master/client/example_tls_test.go)).

## Edge cases

To simplify the command API some of the Redis commands got replaced by a set of commands:

Redis command | Replaced by API commands
------------- | ------------------------
Acl | AclCat, AclDeluser, AclGenpass, AclGetuser, AclHelp, AclList, AclLoad, AclSave, AclSetuser, AclUsers, AclWhoami
Bitop | BitopAnd, BitopNot, BitopOr, BitopXor
Object | ObjectEncoding, ObjectFreq, ObjectHelp, ObjectIdletime, ObjectRefcount
Pubsub | PubsubChannels, PubsubNumpat, PubsubNumsub
Set | Set, SetEx, SetExNx, SetExXx, SetNx, SetPx, SetPxNx, SetPxXx, SetXx
Slowlog | SlowlogGet, SlowlogLen, SlowlogReset
Xgroup | XgroupCreate, XgroupSetid, XgroupDestroy, XgroupDelconsumer, XgroupHelp
Xinfo | XinfoConsumers, XinfoGroups, XinfoStream, XinfoHelp
Zadd | Zadd, ZaddCh, ZaddNx, ZaddXx, ZaddXxCh

Please see the following table for deprecated Redis commands and their replacements:

Redis command | Replaced by API command
------------- | -----------------------
Hmset | Hset
Setex | SetEx
Setnx | SetNx
Psetex | SetPx
Zadd [INCR] | Zincrby
Slaveof | Replicaof
Sync | Psync

## Dependencies

* n/a

## Todo

This is an early beta version. The client API may be subject to change until the first stable version is available.
