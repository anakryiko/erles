erles - Erlang client for Event Store
=====

**erles** is an Erlang client library for [Event Store](http://geteventstore.com/).

erles is licensed under [The MIT License](http://opensource.org/licenses/MIT).

Copyright © 2013 Andrii Nakryiko <andrii.nakryiko@gmail.com>


Features
========

erles aims to provide a convenient and reliable client for using Event Store.
Currently it is on par with official Event Store's client with regard to standard operations
and strives to be even more convenient to use thanks to dynamic nature of Erlang.

Main features:

  - Permanent connection to node/cluster with automatic reconnects.
  - User authentication support;
  - Automatic reconnection to current cluster master.
  - Batch writes to stream.
  - Explicit transaction writes.
  - Stream deletion (both soft- and hard-deletes).
  - Single event reads from streams.
  - Stream reads in forward/backward direction from usual stream or $all pseudo-stream.
  - Primitive subscriptions provided by Event Store directly.
  - Permanent catch-up subscriptions which catch-up, subscribe to live events, and reconnects on errors/connection drops.
  - Metadata set/get operations which allow to work with both structured stream metadata, as well as with raw binary metadata.
  - Ping operation (just for fun and testing that Event Store is responding :) ).


Getting started
===============

Installation
------------

Download sources from [Bitbucket](https://bitbucket.org/anakryiko/erles) or [GitHub](http://github.com/anakryiko/erles) repository.

To build library and run tests:

```bash
$ rebar compile
$ rebar skip_deps=true eunit
```
You can also add repository as a dependency to rebar.config:

```
{deps, [
    ....
    {erles, ".*", {hg, "https://bitbucket.org/anakryiko/erles", {branch, "master"}}}
]}.
```

or

```
{deps, [
    ....
    {erles, ".*", {git, "git://github.com/anakryiko/erles.git", {branch, "master"}}}
]}.
```

erles requires some of standard application to be started, so if you are working with it in Erlang REPL, please run `erles:start()` the first thing. Otherwise adding `erles` to `applications` property to your .app should be fine.

Creating connection
-------------------

All operations in erles are done through a connection, which once established stays open and keeps reconnecting in the event of TCP connection drop.

You can establish connection in three different ways:

  - Connection to single node. You should specify its IP address and TCP port:

```erlang
  {ok, C} = erles:connect(node, {{127,0,0,1}, 1113}).
```

  - Connection to cluster of nodes. You specify a list of tuple with IP address and *HTTP* port of each node. Erles will automatically determine through Event Store's gossip protocol the best node to connect to.

```erlang
  {ok, C} = erles:connect(cluster, [{{127,0,0,1}, 7777},
                                    {{127,0,0,1}, 8888},
                                    {{127,0,0,1}, 9999}]).
```

  - Connection to cluster through resolving specified DNS entry. You specify DNS name and HTTP port. Erles resolves DNS entry into a list of IP addresses and pairs IPs with provided HTTP port forming essentially same configuration that is used with `cluster` connection mode. This mode is intended to be used with Event Store's managers, though specifying HTTP port of cluster node is enough to function properly provided all machines have nodes with same HTTP port. Usage:

```erlang
  {ok, C} = erles:connect(dns, {<<"demo.cluster.foo.com">>, 30778}).
```

Writing events
--------------

Reading events
--------------

Subscriptions
-------------

Working with stream metadata
---------------------
