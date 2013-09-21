-module(erlesque).
-export([test/1, test_par/3, test_seq/3]).
-export([connect/2, connect/3, close/1]).
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

%%% CONNECT
connect(node, {Ip={_I1, _I2, _I3, _I4}, Port})
        when is_integer(Port), Port > 0, Port < 65536 ->
    erlesque_fsm:start_link({node, Ip, Port});

connect(dns, {ClusterDns, ManagerPort})
        when is_list(ClusterDns),
             is_integer(ManagerPort), ManagerPort > 0, ManagerPort < 65536 ->
    erlesque_fsm:start_link({dns, ClusterDns, ManagerPort});

connect(cluster, SeedNodes)
        when is_list(SeedNodes) ->
    erlesque_fsm:start_link({cluster, SeedNodes}).


connect(node, {Ip={_I1, _I2, _I3, _I4}, Port}, Options)
        when is_integer(Port), Port > 0, Port < 65536,
             is_list(Options) ->
    erlesque_fsm:start_link({node, Ip, Port}, Options);

connect(dns, {ClusterDns, ManagerPort}, Options)
        when is_list(ClusterDns),
             is_integer(ManagerPort), ManagerPort > 0, ManagerPort < 65536,
             is_list(Options) ->
    erlesque_fsm:start_link({dns, ClusterDns, ManagerPort}, Options);

connect(cluster, SeedNodes, Options)
        when is_list(SeedNodes),
             is_list(Options) ->
    erlesque_fsm:start_link({cluster, SeedNodes}, Options).

%%% CLOSE
close(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, close).


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


test(Cnt) ->
    {ok, C} = erlesque:connect({node, {127,0,0,1}, 1113}),
    {TimePar, _Value1} = timer:tc(fun() -> test_par(Cnt, Cnt, C) end),
    {TimeSeq, _Value2} = timer:tc(fun() -> test_seq(Cnt, Cnt, C) end),
    {TimePar, TimeSeq}.

test_par(0, 0, _C) -> ok;
test_par(0, Cnt2, C) -> receive done -> test_par(0, Cnt2-1, C) end;
test_par(Cnt1, Cnt2, C) ->
    Self = self(),
    spawn_link(fun() ->
        {ok, _} = erlesque:append(C, <<"test-event">>, any, [erlesque_req_tests:create_event()]),
        Self ! done
    end),
    test_par(Cnt1-1, Cnt2, C).

test_seq(0, 0, _C) -> ok;
test_seq(0, Cnt2, C) -> receive done -> test_seq(0, Cnt2-1, C) end;
test_seq(Cnt1, Cnt2, C) ->
    {ok, _} = erlesque:append(C, <<"test-event">>, any, [erlesque_req_tests:create_event()]),
    self() ! done,
    test_seq(Cnt1-1, Cnt2, C).


