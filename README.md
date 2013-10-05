**erles** - Erlang client for Event Store
=====

**erles** is an Erlang client library for
[Event Store](http://geteventstore.com/).

erles is licensed under [The MIT License](http://opensource.org/licenses/MIT).
See `LICENSE` file for details.

Copyright © 2013 Andrii Nakryiko <andrii.nakryiko@gmail.com>

*I'm very new to Erlang ecosystem and this is my first Erlang project, so if I'm
doing something in a non-optimal, non-idiomatic or, even worse, plain wrong way,
please, let me know and I'll try to fix it.*

Supported versions of Event Store
=================================

*As erles uses and supports some of nice latest changes to Event Store (next
expected versions on writes and soft deletes), which haven't been released yet,
erles can be used only with latest dev-builds currently. As soon as Event Store
releases its new version, erles will stick to supporting stable versions.*

*Erles itself is not yet production ready, so there could be some slight breaking
changes if that will lead to better client library.*

Features
========

erles aims to provide a convenient and reliable client for working with Event
Store. It strives to be on par or better (and more convenient) with official
Event Store client functionality-wise.

Main features:

  - Permanent connection to node/cluster with automatic reconnects.
  - User authentication support;
  - Automatic reconnection to master node for maximum performance and least
    network overhead.
  - Batch writes to stream.
  - Explicit transaction writes.
  - Stream deletion (both soft- and hard-deletes).
  - Single event reads from streams.
  - Stream reads in forward/backward direction from usual stream or $all
    pseudo-stream.
  - Permanent catch-up subscriptions which catch-up, subscribe to live events,
    and reconnects on errors/connection drops.
  - Primitive subscriptions provided by Event Store directly for those who need
    full control.
  - Metadata set/get operations which allow to work with both raw and structured
    stream metadata.
  - Ping operation (just for fun and testing that Event Store is responding :).


Using erles
===========

Installation
------------

Download sources from [Bitbucket](https://bitbucket.org/anakryiko/erles) repository.

To build library and run tests:

```bash
$ rebar get-deps
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

erles requires some of standard application to be started, so if you are working
with it in Erlang shell, please first run `erles:start()`. There is
also a convenience `shell.cmd`/`shell.sh` scripts that start Erlang console with
correct `ebin` paths set.

If you are doing proper Erlang release, adding `erles` to `applications`
property of your .app file should just work.

General conventions
-------------------

### Strings ###

Whenever string value is expected, erles accepts both `<<"binary strings">>` and
`"list strings"`. If not - that's a bug, please report it.

### Operation results ###

All operations in case of success return either `ok` atom or a tuple in which
first element is atom `ok`. Also most operations in case of failure return a
2-tuple `{error, Reason}`, where `Reason` is either predefined atom or any
Erlang term (only in some rare cases).

Common operation failure reasons:

  - `retry_limit` - operation reached allowed number of retries;
  - `server_timeout` - no response from server was returned within allotted
    time;
  - `{aborted, AbortReason}` - operation was aborted due to closed erles
    connection (because of `AbortReason`).
  - `{not_authenticated, EsMsg}` - provided login/password was not authenticated
    on server. `EsMsg` can provide a bit more information;
  - `{bad_request, EsMsg}` - Event Store received malformed package for some
    reason (e.g., because of protocol version mismatch). `EsMsg` can provide
    more information.

For more detail information on structure and type of arguments and results,
please refer to `erles.hrl` header file (which you probably will want to
include into your modules for more convenient usage of erles records).

### Operation options ###

Most erles functions exist in at least two forms:

  - short one with sensible defaults;

  - the long one, which takes a list of additional options that modify or
    enhance operation behavior.

**Connection operation** accepts the following options:

  - `{default_auth, noauth | defauth | {Login, Pass}}` (default: `noauth`) -
    default authentication used for each operation if no explicit override is
    provided for particular operation:

    * `noauth` - no authentication should be used;

    * `defauth` - same as `noauth` (when set as connection option, see
      below for meaning when used for individual operation);

    * `{Login, Pass}` - login/password string pair to use for authentication.

  - `{max_server_ops, pos_integer()}` (default: `2000`) - maximum amount of
    active operations on Event Store node. Used to prevent node overload. All
    other operations are enqueued internally in erles connection.

  - `{operation_timeout, pos_integer()}` (default: `7000`) - timeout after which
    `{error, server_timeout}` will be returned for operation, if there was no
    response from Event Store.

  - `{operation_retries, non_neg_integer()}` (default: `10`) - how many times
    operation will be retried (due to reconnection, prepare/commit/forwarding
    timeout, etc) before giving up and returning `{error, retry_limit}`.

  - `{retry_delay, non_neg_integer()}` (default: `500`) - the delay before
    operation retry.

  - `{connection_timeout, pos_integer()}` (default: `2000`) - TCP connection
    timeout.

  - `{reconnection_delay, non_neg_integer()}` (default: `500`) - the delay
    before TCP reconnection in case of dropped connection.

  - `{max_conn_retries, non_neg_integer()}` (default: `10`) - how many times
    erles will try to reconnect to a given destination (node or cluster).

  - `{heartbeat_period, pos_integer()}` (default: `500`) - heartbeat period.

  - `{heartbeat_timeout, pos_integer()}` (default: `1000`) - heartbeat timeout
    after which connection is declared closed, if no heartbeat response is get
    from server.

  - `{dns_timeout, pos_integer()}` (default: `2000`) - timeout of resolving DNS
    entry.

  - `{gossip_timeout, pos_integer()}.` (default: `1000`) - timeout of getting
    gossip from node.

All timeouts are in milliseconds. `infinity` is not accepted.

**Write operations** (append, transactions, stream deletion, setting stream
metadata) accept the following options:

  - `{auth, noauth | defauth | {Login, Pass}}` (default: `defauth`) -
    authentication used for operation.

      * `noauth` - no authentication should be used;

      * `defauth` - use default authentication of connection (set with
        `default_auth` option provided in `connect/4`). If no default
        authentication is set on connection level, `noauth` is used;

      * `{Login, Pass}` - login/password string pair to use for authentication.


  - `{master_only, boolean()}` - the flag that specifies whether operation
    should be performed exclusively on master node. When `master_only` flag is
    set, erles connection will automatically reconnect to new master in case
    connected node is not master. This option ensures no additional
    communication overhead and lower latency for writes (otherwise write
    operations are forwarded within Event Store nodes internally) and most up
    to date reads from master for reads (otherwise reads are served locally
    from Event Store node, no forwarding occurs). Default value is `true`.

**Read operations** (single event read, stream range reads, getting stream
metadata) accept the following options:

  - `auth` - same as for write operations;

  - `master_only` - same as for write operations, see option's description
    for effect on reads;

  - `{resolve, boolean()}` - the flag that indicates whether Event Store
    should resolve links emitted from projections into actual events. If the
    flag is set - any link is automatically replaced with original linked
    event. Defaults to `true`.

**Subscription operation** accepts the following options:

  - `auth` - same as for write and read operations;

  - `resolve` - same as for read operations;

  - `{subscriber, pid()}` - PID of process that will receive all subscription
    messages. If not provided - the caller process ID is used.

  - `{read_batch, pos_integer()}` - the number of events that will be read
    in a single stream read operation during catch-up phase of subscription.
    Default value - `100`.

Connection
----------

All operations in erles are performed through a connection, which, once
established, stays open and keeps reconnecting in case of TCP connection drop.

erles connection is established through calling `connect/2` (or `connect/3`)
function. When successful, function returns process ID, which is used with all
operations (except subscription `unsubscribe/1`).

```erlang
-spec connect('node', {Ip :: inet:ip_address(), TcpPort :: 0..65535})              -> {'ok', pid()} | {'error', term()};
             ('dns', {ClusterDns :: string() | binary(), ManagerPort :: 0..65535}) -> {'ok', pid()} | {'error', term()};
             ('cluster', [{Ip :: inet:ip_address(), HttpPort :: 0..65535}])        -> {'ok', pid()} | {'error', term()}.
```

Three connection modes are currently supported:

  - Connection to single node. IP address and TCP port of Event Store node
    should be specified:

```erlang
> {ok, C} = erles:connect(node, {{127,0,0,1}, 1113}).
{ok,<0.33.0>}
```

  - Connection to cluster of nodes. A list of 2-tuples with IP address and
    *HTTP* port of each node is specified. erles will automatically determine
    through gossip protocol the best node to connect to.

```erlang
> {ok, C} = erles:connect(cluster, [{{127,0,0,1}, 7777},
                                    {{127,0,0,1}, 8888},
                                    {{127,0,0,1}, 9999}]).
{ok,<0.40.0>}
```

  - Connection to cluster of nodes through specifying DNS name and common HTTP
    port for all endpoints. erles will resolve DNS name into a list of IP
    addresses and pair them with provided HTTP port forming same list of
    endpoints used with `cluster` connection mode. This mode is intended to be
    used with Event Store's managers, though specifying HTTP port of cluster
    node is enough to function properly provided all machines have nodes with
    same HTTP port. Usage:

```erlang
> {ok, C} = erles:connect(dns, {<<"demo.cluster.foo.com">>, 30778}).
{ok,<0.45.0>}
```

To close existing connection just call `close/1`:

```erlang
> erles:close(C).
ok
```

Writing events
-------

Event Store provides two ways to store events, both of which are supported by
erles:

  - batch writes when all sent events are committed immediately;

  - transactional writes. You have to start transaction specifying expected
    version of stream you write to, then you can make multiple transactional
    writes of events which Event Store won't commit until you explicitly
    commit that transaction.

### Batch writes ###

Batch writes are done with `append/4` (`append/5` for variant with options)
function:

```erlang
-spec append(pid(), stream_id(), exp_ver(), [event_data()]) ->
        {'ok', stream_ver()} | {'error', write_error()}.
```

Where parameters (in order) are:

  1. erles connection PID (returned from `connect/2` function).
  2. Non-empty stream ID (either binary or list string).
  3. Expected version of stream we are about to write to. Expected version is
     either atom `any` (to specify that you don't care about version of
     stream, so no optimistic concurrency control should be used and write
     should always succeed) or integer, with `-2` meaning same as `any`, `-1`
     meaning empty stream, any other non-negative integer specifying the
     number of last event in the stream.
  4. List of events to append to stream, each event defined as:

```erlang
-record(event_data,
        {
            event_id = erles_utils:gen_uuid()                 :: uuid(),
            event_type = erlang:error({required, event_type}) :: binary() | string(),
            data_type = raw                                   :: 'raw' | 'json',
            data = erlang:error({required, data})             :: binary(),
            metadata = <<>>                                   :: binary()
        }).
```

The two required fields are `event_type` and `data`. `event_id`, if not
specified, will be set to newly generated UUID, `metadata` will be empty
and data type will be assumed to be `raw` (not structured). If you are
writing valid JSON as data, you should set `data_type` to `json`, so
Event Store's projections will know how to work with event data.

In case of success, the new expected version of stream is returned (that should
be used with consequent writes to same stream).

The write-specific failure reasons include:

  - `wrong_exp_ver` - provided stream expected version is wrong;

  - `access_denied` - you don't have enough access rights for operation, provide
    login/password credentials with enough access rights;

  - `stream_deleted` - you are trying to write events into the stream that was
    permanently deleted;

  - `invalid_transaction` - this can mean few things, but most probably wrong
    transaction ID (for transaction operations).

Example:

```erlang
> {ok, NextExpVer} = erles:append(C, <<"stream">>, any,
                                  [#event_data{event_type="type1", data="data1"},
                                   #event_data{event_type="type2", data="data2"}]).
{ok,1}
```

### Transactional writes ###

To start transaction you use `txn_start/3` (`txn_start/4`) function:

```erlang
-spec txn_start(pid(), stream_id(), exp_ver()) ->
        {'ok', trans_id()} | {'error', write_error()}.
```

All parameters have the same meaning as with `append/4`. In case of success
transaction ID is returned which should be used with all operations withing that
transaction. Failure reasons are same as for `append/4`.

Example:

```erlang
> {ok, Tid} = erles:txn_start(C, <<"stream">>, 1).
{ok,9679198}
```

To write some events within transaction you use `txn_append/3` function:

```erlang
-spec txn_append(pid(), trans_id(), [event_data()]) ->
        'ok' | {'error', write_error()}.
```

It accepts connection PID, transaction ID returned from `txn_start/3` and a list
of event of the same structure as in `append/4`. When operation succeeds, events
are not committed yet.

Example:

```erlang
> ok = erles:txn_append(C, Tid, [#event_data{event_type = <<"et1">>,
                                             data = <<"{\"num\":123}">>,
                                             data_type = json}]).
ok
> ok = erles:txn_append(C, Tid, [#event_data{event_type = <<"et2">>,
                                             data = <<1,2,3>>}]).
ok
```

To force events to be stored in the stream, call `txn_commit/2`:

```erlang
-spec txn_commit(pid(), trans_id()) ->
        {'ok', stream_ver()} | {'error', write_error()}.
```

The result is the same as with `append/4` - expected version of stream for
following writes. Failures are same as well.

Example:

```erlang
> {ok, NextExpVer} = erles:txn_commit(C, Tid).
{ok,3}
```


Reading events
-------

To be written... For now, please take a look at `erles_tests.erl` for some
samples.

Subscriptions
-------

To be written... For now, please take a look at `erles_tests.erl` for some
samples.

Working with stream metadata
--------------

To be written... For now, please take a look at `erles_tests.erl` for some
samples.
