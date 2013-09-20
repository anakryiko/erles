-module(erlesque).

-export([connect/1, connect/2, close/1, start_link/1, start_link/2]).
-export([ping/1]).

-export([append/4, append/5]).
-export([transaction_start/3, transaction_start/4]).
-export([transaction_write/3, transaction_write/4]).
-export([transaction_commit/2, transaction_commit/3]).
-export([delete/3, delete/4]).

-export([read_event/3, read_event/4]).
-export([read_stream_forward/4, read_stream_forward/5]).
-export([read_stream_backward/4, read_stream_backward/5]).
-export([read_all_forward/3, read_all_forward/4]).
-export([read_all_backward/3, read_all_backward/4]).

-export([subscribe/2, subscribe/3]).

-define(EXPECTED_VERSION_ANY, -2).
-define(REQUIRE_MASTER, true).
-define(LAST_EVENT_NUMBER, -1).
-define(LAST_LOG_POSITION, -1).

connect(Destination) -> start_link(Destination).

connect(Destination, Options) -> start_link(Destination, Options).

close(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, close).

start_link(Destination = {node, _Ip, _Port}) ->
    erlesque_fsm:start_link(Destination).

start_link(Destination = {node, _Ip, _Port}, Options) ->
    erlesque_fsm:start_link(Destination, Options).

%%% PING
ping(Pid) ->
    gen_fsm:sync_send_event(Pid, {op, {ping, temp, defauth}, []}).


%%% APPEND EVENTS TO STREAM
append(Pid, StreamId, ExpectedVersion, Events) ->
    append(Pid, StreamId, ExpectedVersion, Events, []).

append(Pid, StreamId, ExpectedVersion, Events, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPECTED_VERSION_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    append(Pid, StreamId, ExpVer, Events, Auth, RequireMaster).

append(Pid, StreamId, ExpectedVersion, Events, Auth, RequireMaster)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPECTED_VERSION_ANY,
             is_list(Events),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid,
                            {op, {write_events, temp, Auth},
                                 {StreamId, ExpectedVersion, Events, RequireMaster}}, infinity).

%%% START EXPLICIT TRANSACTION
transaction_start(Pid, StreamId, ExpectedVersion) ->
    transaction_start(Pid, StreamId, ExpectedVersion, []).

transaction_start(Pid, StreamId, ExpectedVersion, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPECTED_VERSION_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    transaction_start(Pid, StreamId, ExpVer, Auth, RequireMaster).

transaction_start(Pid, StreamId, ExpectedVersion, Auth, RequireMaster)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPECTED_VERSION_ANY,
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_start, temp, Auth},
                                      {StreamId, ExpectedVersion, RequireMaster}}, infinity).


%%% WRITE EVENTS IN EXPLICIT TRANSACTION
transaction_write(Pid, TransactionId, Events) ->
    transaction_write(Pid, TransactionId, Events, []).

transaction_write(Pid, TransactionId, Events, Options) ->
    Auth = proplists:get_value(auth, Options, defauth),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    transaction_write(Pid, TransactionId, Events, Auth, RequireMaster).

transaction_write(Pid, TransactionId, Events, Auth, RequireMaster)
        when is_integer(TransactionId), TransactionId >= 0,
             is_list(Events),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_write, temp, Auth},
                                      {TransactionId, Events, RequireMaster}}, infinity).


%%% COMMIT EXPLICIT TRANSACTION
transaction_commit(Pid, TransactionId) ->
    transaction_commit(Pid, TransactionId, []).

transaction_commit(Pid, TransactionId, Options) ->
    Auth = proplists:get_value(auth, Options, defauth),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    transaction_commit(Pid, TransactionId, Auth, RequireMaster).

transaction_commit(Pid, TransactionId, Auth, RequireMaster)
        when is_integer(TransactionId), TransactionId >= 0,
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_commit, temp, Auth},
                                      {TransactionId, RequireMaster}}, infinity).


%%% DELETE STREAM
delete(Pid, StreamId, ExpectedVersion) ->
    delete(Pid, StreamId, ExpectedVersion, []).

delete(Pid, StreamId, ExpectedVersion, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPECTED_VERSION_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    delete(Pid, StreamId, ExpVer, Auth, RequireMaster).

delete(Pid, StreamId, ExpectedVersion, Auth, RequireMaster)
    when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPECTED_VERSION_ANY,
         is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {delete_stream, temp, Auth},
                                      {StreamId, ExpectedVersion, RequireMaster}}, infinity).


%%% READ SINGLE EVENT
read_event(Pid, StreamId, EventNumber) ->
    read_event(Pid, StreamId, EventNumber, []).

read_event(Pid, StreamId, EventNumber, Options) ->
    Pos = case EventNumber of
        last -> ?LAST_EVENT_NUMBER;
        _ -> EventNumber
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    ResolveLinks = proplists:get_value(resolve, Options, false),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    read_event(Pid, StreamId, Pos, Auth, ResolveLinks, RequireMaster).

read_event(Pid, StreamId, EventNumber, Auth, ResolveLinks, RequireMaster)
        when is_integer(EventNumber), EventNumber >= ?LAST_EVENT_NUMBER,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_event, temp, Auth},
                                      {StreamId, EventNumber, ResolveLinks, RequireMaster}}, infinity).


%%% READ STREAM EVENTS IN FORWARD DIRECTION
read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount) ->
    read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount, []).

read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount, Options) ->
    Pos = case FromEventNumber of
        first -> 0;
        _ -> FromEventNumber
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    ResolveLinks = proplists:get_value(resolve, Options, false),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    read_stream_forward(Pid, StreamId, Pos, MaxCount, Auth, ResolveLinks, RequireMaster).

read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount, Auth, ResolveLinks, RequireMaster)
        when is_integer(FromEventNumber), FromEventNumber >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_stream_events_forward, temp, Auth},
                                      {StreamId, FromEventNumber, MaxCount, ResolveLinks, RequireMaster}}, infinity).


%%% READ STREAM EVENTS IN BACKWARD DIRECTION
read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount) ->
    read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount, []).

read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount, Options) ->
    Pos = case FromEventNumber of
        last -> ?LAST_EVENT_NUMBER;
        _ -> FromEventNumber
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    ResolveLinks = proplists:get_value(resolve, Options, false),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    read_stream_backward(Pid, StreamId, Pos, MaxCount, Auth, ResolveLinks, RequireMaster).

 read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount, Auth, ResolveLinks, RequireMaster)
        when is_integer(FromEventNumber), FromEventNumber >= ?LAST_EVENT_NUMBER,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_stream_events_backward, temp, Auth},
                                      {StreamId, FromEventNumber, MaxCount, ResolveLinks, RequireMaster}}, infinity).


%%% READ ALL EVENTS IN FORWARD DIRECTION
read_all_forward(Pid, FromPos, MaxCount) ->
    read_all_forward(Pid, FromPos, MaxCount, []).

read_all_forward(Pid, FromPos, MaxCount, Options) ->
    Pos = case FromPos of
        first -> {tfpos, 0, 0};
        {tfpos, _, _} -> FromPos
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    ResolveLinks = proplists:get_value(resolve, Options, false),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    read_all_forward(Pid, Pos, MaxCount, Auth, ResolveLinks, RequireMaster).

read_all_forward(Pid, FromPos={tfpos, CommitPos, PreparePos}, MaxCount, Auth, ResolveLinks, RequireMaster)
        when is_integer(CommitPos), CommitPos >= ?LAST_LOG_POSITION,
             is_integer(PreparePos), PreparePos >= ?LAST_LOG_POSITION,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_all_events_forward, temp, Auth},
                                      {FromPos, MaxCount, ResolveLinks, RequireMaster}}, infinity).


%%% READ ALL EVENTS IN BACKWARD DIRECTION
read_all_backward(Pid, FromPos, MaxCount) ->
    read_all_backward(Pid, FromPos, MaxCount, []).

read_all_backward(Pid, FromPos, MaxCount, Options) ->
    Pos = case FromPos of
        last -> {tfpos, ?LAST_LOG_POSITION, ?LAST_LOG_POSITION};
        {tfpos, _, _} -> FromPos
    end,
    Auth = proplists:get_value(auth, Options, defauth),
    ResolveLinks = proplists:get_value(resolve, Options, false),
    RequireMaster = proplists:get_value(require_master, Options, ?REQUIRE_MASTER),
    read_all_backward(Pid, Pos, MaxCount, Auth, ResolveLinks, RequireMaster).

read_all_backward(Pid, FromPos={tfpos, CommitPos, PreparePos}, MaxCount, Auth, ResolveLinks, RequireMaster)
        when is_integer(CommitPos), CommitPos >= ?LAST_LOG_POSITION,
             is_integer(PreparePos), PreparePos >= ?LAST_LOG_POSITION,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_all_events_backward, temp, Auth},
                                      {FromPos, MaxCount, ResolveLinks, RequireMaster}}, infinity).


%%% PRIMITIVE SUBSCRIPTION TO STREAM
subscribe(Pid, StreamId) ->
    subscribe(Pid, StreamId, []).

subscribe(Pid, StreamId, Options) ->
    Auth = proplists:get_value(auth, Options, defauth),
    ResolveLinks = proplists:get_value(resolve, Options, false),
    SubscriberPid = proplists:get_value(subscriber, Options, self()),
    subscribe(Pid, StreamId, Auth, ResolveLinks, SubscriberPid).

subscribe(Pid, StreamId, Auth, ResolveLinks, SubscriberPid) ->
    Stream = case StreamId of
        all -> <<"">>;
        _ -> StreamId
    end,
    gen_fsm:sync_send_event(Pid, {op, {subscribe_to_stream, perm, Auth},
                                      {Stream, ResolveLinks, SubscriberPid}}, infinity).

