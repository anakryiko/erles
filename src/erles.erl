-module(erles).

-export([start/0]).
-export([connect/2, connect/3, close/1]).
-export([ping/1]).
-export([append/4, append/5]).
-export([txn_start/3, txn_start/4]).
-export([txn_append/3, txn_append/4]).
-export([txn_commit/2, txn_commit/3]).
-export([delete/3, delete/4, delete/5]).
-export([read_event/3, read_event/4]).
-export([read_stream/4, read_stream/5, read_stream/6]).
-export([subscribe/2, subscribe/3, unsubscribe/1]).
-export([subscribe_perm/2, subscribe_perm/3, subscribe_perm/4, unsubscribe_perm/1]).
-export([set_metadata/4, set_metadata/5]).
-export([get_metadata/2, get_metadata/3, get_metadata/4]).

-include("erles.hrl").
-include("erles_internal.hrl").

%% Use this function when working with Erles from REPL to start all dependencies
-spec start() -> 'ok' | {'error', term()}.
start() ->
    application:load(erles),
    {ok, Apps} = application:get_key(erles, applications),
    lists:foreach(fun(App) -> application:start(App) end, Apps),
    application:start(erles).

%% CONNECT
-spec connect('node', {Ip :: inet:ip_address(), TcpPort :: 0..65535})              -> {'ok', pid()} | {'error', term()};
             ('dns', {ClusterDns :: string() | binary(), ManagerPort :: 0..65535}) -> {'ok', pid()} | {'error', term()};
             ('cluster', [{Ip :: inet:ip_address(), HttpPort :: 0..65535}])        -> {'ok', pid()} | {'error', term()}.
connect(node, {Ip, Port})
        when is_integer(Port), Port >= 0, Port < 65536 ->
    erles_fsm:start_link({node, Ip, Port});

connect(dns, {ClusterDns, ManagerPort})
        when is_list(ClusterDns) orelse is_binary(ClusterDns),
             is_integer(ManagerPort), ManagerPort >= 0, ManagerPort < 65536 ->
    erles_fsm:start_link({dns, ClusterDns, ManagerPort});

connect(cluster, SeedNodes)
        when is_list(SeedNodes) ->
    erles_fsm:start_link({cluster, SeedNodes}).

-spec connect('node', {Ip :: inet:ip_address(), TcpPort :: 0..65535}, [connect_option()])              -> {'ok', pid()} | {'error', term()};
             ('dns', {ClusterDns :: string() | binary(), ManagerPort :: 0..65535}, [connect_option()]) -> {'ok', pid()} | {'error', term()};
             ('cluster', [{Ip :: inet:ip_address(), HttpPort :: 0..65535}], [connect_option()])        -> {'ok', pid()} | {'error', term()}.
connect(node, {Ip, Port}, Options)
        when is_integer(Port), Port >= 0, Port < 65536,
             is_list(Options) ->
    erles_fsm:start_link({node, Ip, Port}, Options);

connect(dns, {ClusterDns, ManagerPort}, Options)
        when is_list(ClusterDns) orelse is_binary(ClusterDns),
             is_integer(ManagerPort), ManagerPort >= 0, ManagerPort < 65536,
             is_list(Options) ->
    erles_fsm:start_link({dns, ClusterDns, ManagerPort}, Options);

connect(cluster, SeedNodes, Options)
        when is_list(SeedNodes),
             is_list(Options) ->
    erles_fsm:start_link({cluster, SeedNodes}, Options).

%% CLOSE
-spec close(pid()) -> 'ok'.
close(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, close).


%% PING
-spec ping(pid()) -> 'ok' | {'error', operation_error()}.
ping(Pid) ->
    gen_fsm:sync_send_event(Pid, {op, {ping, temp, noauth}, {}}, infinity).


%% APPEND EVENTS TO STREAM
-spec append(pid(), stream_id(), exp_ver(), [event_data()]) ->
        {'ok', stream_ver()} | {'error', write_error()}.
append(Pid, StreamId, ExpectedVersion, Events) ->
    append(Pid, StreamId, ExpectedVersion, Events, []).

-spec append(pid(), stream_id(), exp_ver(), [event_data()], [write_option()]) ->
        {'ok', stream_ver()} | {'error', write_error()}.
append(Pid, StreamId, ExpectedVersion, Events, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPVER_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    append(Pid, StreamId, ExpVer, Events, Auth, MasterOnly).

-spec append(pid(), stream_id(), exp_ver(), [event_data()], auth(), boolean()) ->
        {'ok', stream_ver()} | {'error', write_error()}.
append(Pid, StreamId, ExpectedVersion, Events, Auth, MasterOnly)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPVER_ANY,
             is_list(Events),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid,
                            {op, {write_events, temp, Auth},
                                 {StreamId, ExpectedVersion, Events, MasterOnly}}, infinity).

%% START EXPLICIT TRANSACTION
-spec txn_start(pid(), stream_id(), exp_ver()) ->
        {'ok', trans_id()} | {'error', write_error()}.
txn_start(Pid, StreamId, ExpectedVersion) ->
    txn_start(Pid, StreamId, ExpectedVersion, []).

-spec txn_start(pid(), stream_id(), exp_ver(), [write_option()]) ->
        {'ok', trans_id()} | {'error', write_error()}.
txn_start(Pid, StreamId, ExpectedVersion, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPVER_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    txn_start(Pid, StreamId, ExpVer, Auth, MasterOnly).

-spec txn_start(pid(), stream_id(), exp_ver(), auth(), boolean()) ->
        {'ok', trans_id()} | {'error', write_error()}.
txn_start(Pid, StreamId, ExpectedVersion, Auth, MasterOnly)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPVER_ANY,
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_start, temp, Auth},
                                      {StreamId, ExpectedVersion, MasterOnly}}, infinity).


%% WRITE EVENTS IN EXPLICIT TRANSACTION
-spec txn_append(pid(), trans_id(), [event_data()]) ->
        'ok' | {'error', write_error()}.
txn_append(Pid, TransactionId, Events) ->
    txn_append(Pid, TransactionId, Events, []).

-spec txn_append(pid(), trans_id(), [event_data()], [write_option()]) ->
        'ok' | {'error', write_error()}.
txn_append(Pid, TransactionId, Events, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    txn_append(Pid, TransactionId, Events, Auth, MasterOnly).

-spec txn_append(pid(), trans_id(), [event_data()], auth(), boolean()) ->
        'ok' | {'error', write_error()}.
txn_append(Pid, TransactionId, Events, Auth, MasterOnly)
        when is_integer(TransactionId), TransactionId >= 0,
             is_list(Events),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_write, temp, Auth},
                                      {TransactionId, Events, MasterOnly}}, infinity).


%% COMMIT EXPLICIT TRANSACTION
-spec txn_commit(pid(), trans_id()) ->
        {'ok', stream_ver()} | {'error', write_error()}.
txn_commit(Pid, TransactionId) ->
    txn_commit(Pid, TransactionId, []).

-spec txn_commit(pid(), trans_id(), [write_option()]) ->
        {'ok', stream_ver()} | {'error', write_error()}.
txn_commit(Pid, TransactionId, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    txn_commit(Pid, TransactionId, Auth, MasterOnly).

-spec txn_commit(pid(), trans_id(), auth(), boolean()) ->
        {'ok', stream_ver()} | {'error', write_error()}.
txn_commit(Pid, TransactionId, Auth, MasterOnly)
        when is_integer(TransactionId), TransactionId >= 0,
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_commit, temp, Auth},
                                      {TransactionId, MasterOnly}}, infinity).


%% DELETE STREAM
-spec delete(pid(), stream_id(), exp_ver()) ->
        'ok' | {'error', write_error()}.
delete(Pid, StreamId, ExpectedVersion) ->
    delete(Pid, StreamId, ExpectedVersion, soft, []).

-spec delete(pid(), stream_id(), exp_ver(), 'soft' | 'perm') ->
        'ok' | {'error', write_error()}.
delete(Pid, StreamId, ExpectedVersion, DeleteType) ->
    delete(Pid, StreamId, ExpectedVersion, DeleteType, []).

-spec delete(pid(), stream_id(), exp_ver(), 'soft' | 'perm', [write_option()]) ->
        'ok' | {'error', write_error()}.
delete(Pid, StreamId, ExpectedVersion, DeleteType, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPVER_ANY;
        _   -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    delete(Pid, StreamId, ExpVer, DeleteType, Auth, MasterOnly).

-spec delete(pid(), stream_id(), exp_ver(), 'soft' | 'perm', auth(), boolean()) ->
        'ok' | {'error', write_error()}.
delete(Pid, StreamId, ExpectedVersion, DeleteType, Auth, MasterOnly)
    when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPVER_ANY,
         DeleteType =:= soft orelse DeleteType =:= perm,
         is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {delete_stream, temp, Auth},
                                      {StreamId, ExpectedVersion, DeleteType, MasterOnly}}, infinity).


%% READ SINGLE EVENT
-spec read_event(pid(), stream_id(), event_num() | 'first' | 'last') ->
        {'ok', event_res()} | {'error', read_event_error()}.
read_event(Pid, StreamId, EventNumber) ->
    read_event(Pid, StreamId, EventNumber, []).

-spec read_event(pid(), stream_id(), event_num() | 'first' | 'last', [read_option()]) ->
        {'ok', event_res()} | {'error', read_event_error()}.
read_event(Pid, StreamId, EventNumber, Options) ->
    Pos = case EventNumber of
        first -> 0;
        last  -> ?LAST_EVENT_NUM;
        _     -> EventNumber
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    read_event(Pid, StreamId, Pos, Auth, ResolveLinks, MasterOnly).

-spec read_event(pid(), stream_id(), event_num(), auth(), boolean(), boolean()) ->
        {'ok', event_res()} | {'error', read_event_error()}.
read_event(Pid, StreamId, EventNumber, Auth, ResolveLinks, MasterOnly)
        when is_integer(EventNumber), EventNumber >= ?LAST_EVENT_NUM,
             is_boolean(ResolveLinks),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {read_event, temp, Auth},
                                      {StreamId, EventNumber, ResolveLinks, MasterOnly}}, infinity).


%% READ STREAM OR ALL EVENTS
-spec read_stream(pid(), all_stream_id(), stream_pos() | 'first' | 'last', pos_integer()) ->
        {'ok', Events :: [event_res()], NextFrom :: stream_pos(), EndOfStream :: boolean()}
      | {'error', read_stream_error()}.
read_stream(Pid, StreamId, From, MaxCount) ->
    read_stream(Pid, StreamId, From, MaxCount, forward, []).

-spec read_stream(pid(), all_stream_id(), stream_pos() | 'first' | 'last', pos_integer(), read_dir()) ->
        {'ok', Events :: [event_res()], NextFrom :: stream_pos(), EndOfStream :: boolean()}
      | {'error', read_stream_error()}.
read_stream(Pid, StreamId, From, MaxCount, Direction) ->
    read_stream(Pid, StreamId, From, MaxCount, Direction, []).

-spec read_stream(pid(), all_stream_id(), stream_pos() | 'first' | 'last', pos_integer(), read_dir(), [read_option()]) ->
        {'ok', Events :: [event_res()], NextFrom :: stream_pos(), EndOfStream :: boolean()}
      | {'error', read_stream_error()}.
read_stream(Pid, StreamId, From, MaxCount, Direction, Options) ->
    FromPos = stream_pos(StreamId, From, Direction),
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    MasterOnly = proplists:get_value(master_only, Options, ?DEF_MASTER_ONLY),
    read_stream(Pid, StreamId, FromPos, MaxCount, Direction, Auth, ResolveLinks, MasterOnly).

-spec read_stream(pid(), all_stream_id(), stream_pos() | {'tfpos', -1, -1} | -1,
                  pos_integer(), read_dir(), auth(), boolean(), boolean()) ->
        {'ok', Events :: [event_res()], NextFrom :: stream_pos(), EndOfStream :: boolean()}
      | {'error', read_stream_error()}.
read_stream(Pid, all, From={tfpos, CommitPos, PreparePos}, MaxCount, forward, Auth, ResolveLinks, MasterOnly)
        when is_integer(CommitPos), CommitPos >= 0,
             is_integer(PreparePos), PreparePos >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {read_all_events_forward, temp, Auth},
                                      {From, MaxCount, ResolveLinks, MasterOnly}}, infinity);

read_stream(Pid, all, From={tfpos, CommitPos, PreparePos}, MaxCount, backward, Auth, ResolveLinks, MasterOnly)
        when is_integer(CommitPos), CommitPos >= ?LAST_LOG_POS,
             is_integer(PreparePos), PreparePos >= ?LAST_LOG_POS,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {read_all_events_backward, temp, Auth},
                                      {From, MaxCount, ResolveLinks, MasterOnly}}, infinity);

read_stream(Pid, StreamId, From, MaxCount, forward, Auth, ResolveLinks, MasterOnly)
        when is_integer(From), From >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {read_stream_events_forward, temp, Auth},
                                      {StreamId, From, MaxCount, ResolveLinks, MasterOnly}}, infinity);

read_stream(Pid, StreamId, From, MaxCount, backward, Auth, ResolveLinks, MasterOnly)
        when is_integer(From), From >= ?LAST_EVENT_NUM,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(MasterOnly) ->
    gen_fsm:sync_send_event(Pid, {op, {read_stream_events_backward, temp, Auth},
                                      {StreamId, From, MaxCount, ResolveLinks, MasterOnly}}, infinity).

-spec stream_pos(all_stream_id(), stream_pos() | 'first' | 'last' | {'tfpos', -1, -1} | -1, read_dir()) ->
        stream_pos() | 'first' | 'last' | {'tfpos', -1, -1} | -1.
stream_pos(all, first, forward)             -> {tfpos, 0, 0};
stream_pos(all, last, backward)             -> {tfpos, ?LAST_LOG_POS, ?LAST_LOG_POS};
stream_pos(all, {tfpos, C, P}, _Dir)        -> {tfpos, C, P};
stream_pos(_StreamId, first, forward)       -> 0;
stream_pos(_StreamId, last, backward)       -> ?LAST_EVENT_NUM;
stream_pos(_StreamId, EventNumber, _Dir)    -> EventNumber.


%% PRIMITIVE SUBSCRIPTION
-spec subscribe(pid(), all_stream_id())
        -> {'ok', SubscrPid :: pid(), stream_pos()}.
subscribe(Pid, StreamId) ->
    subscribe(Pid, StreamId, []).

-spec subscribe(pid(), all_stream_id(), [subscr_option()])
        -> {'ok', SubscrPid :: pid(), stream_pos()}.
subscribe(Pid, StreamId, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    SubscriberPid = proplists:get_value(subscriber, Options, self()),
    subscribe(Pid, StreamId, Auth, ResolveLinks, SubscriberPid).

-spec subscribe(pid(), all_stream_id(), auth(), boolean(), pid())
        -> {'ok', SubscrPid :: pid(), stream_pos()}.
subscribe(Pid, StreamId, Auth, ResolveLinks, SubscriberPid) ->
    gen_fsm:sync_send_event(Pid, {op, {subscribe_to_stream, perm, Auth},
                                      {StreamId, ResolveLinks, SubscriberPid}}, infinity).

-spec unsubscribe(SubscrPid :: pid()) -> 'ok'.
unsubscribe(SubscrPid) ->
    erles_subscr:stop(SubscrPid).


%% PERMANENT CATCH-UP SUBSCRIPTION
-spec subscribe_perm(pid(), all_stream_id()) ->
        {'ok', SubscrPid :: pid()} | {'error', term()}.
subscribe_perm(Pid, StreamId) ->
    subscribe_perm(Pid, StreamId, live, []).

-spec subscribe_perm(pid(), all_stream_id(), subscr_pos()) ->
        {'ok', SubscrPid :: pid()} | {'error', term()}.
subscribe_perm(Pid, StreamId, From) ->
    subscribe_perm(Pid, StreamId, From, []).

-spec subscribe_perm(pid(), all_stream_id(), subscr_pos(), [subscr_perm_option()]) ->
        {'ok', SubscrPid :: pid()} | {'error', term()}.
subscribe_perm(Pid, StreamId, From, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    SubscriberPid = proplists:get_value(subscriber, Options, self()),
    MaxCount = proplists:get_value(max_count, Options, 100),
    erles_subscr_perm:start_link(Pid, StreamId, From, SubscriberPid, Auth, ResolveLinks, MaxCount).

-spec unsubscribe_perm(SubscrPid :: pid()) -> 'ok'.
unsubscribe_perm(SubscrPid) ->
    erles_subscr_perm:stop(SubscrPid).


%% SET STREAM METADATA
-spec set_metadata(pid(), all_stream_id(), exp_ver(), stream_meta() | binary()) ->
        {'ok', stream_ver()} | {'error', write_error()}.
set_metadata(Pid, StreamId, ExpectedVersion, Meta) ->
    set_metadata(Pid, StreamId, ExpectedVersion, Meta, []).

-spec set_metadata(pid(), all_stream_id(), exp_ver(), stream_meta() | binary(), [write_option()]) ->
        {'ok', stream_ver()} | {'error', write_error()}.
set_metadata(Pid, all, ExpectedVersion, RawMeta, Options) ->
    set_metadata(Pid, <<"$$all">>, ExpectedVersion, RawMeta, Options);

set_metadata(Pid, StreamId, ExpectedVersion, RawMeta, Options) when is_binary(RawMeta) ->
    MetaEvent = #event_data{data=RawMeta, data_type=raw, event_type=?ET_STREAMMETADATA},
    append(Pid, <<"$$", StreamId/binary>>, ExpectedVersion, [MetaEvent], Options);

set_metadata(Pid, StreamId, ExpectedVersion, Meta = #stream_meta{}, Options) ->
    Json = erles_utils:meta_to_metajson(Meta),
    JsonBin = jsx:encode(Json),
    MetaEvent = #event_data{data=JsonBin, data_type=json, event_type=?ET_STREAMMETADATA},
    append(Pid, <<"$$", StreamId/binary>>, ExpectedVersion, [MetaEvent], Options).


%% GET STREAM METADATA
-spec get_metadata(pid(), all_stream_id()) ->
    {ok, struct_meta_res() | raw_meta_res()} | {'error', 'bad_json' | read_event_error()}.
get_metadata(Pid, StreamId) ->
    get_metadata(Pid, StreamId, struct).

-spec get_metadata(pid(), all_stream_id(), 'struct' | 'raw') ->
    {ok, struct_meta_res() | raw_meta_res()} | {'error', 'bad_json' | read_event_error()}.
get_metadata(Pid, StreamId, MetaType) ->
    get_metadata(Pid, StreamId, MetaType, []).

-spec get_metadata(pid(), all_stream_id(), 'struct', [read_option()]) ->
                        {ok, struct_meta_res()} | {'error', 'bad_json' | read_event_error()};
                  (pid(), all_stream_id(), 'raw', [read_option()]) ->
                        {ok, raw_meta_res()} | {'error', read_event_error()}.
get_metadata(Pid, StreamId, struct, Options) ->
    case get_metadata(Pid, StreamId, raw, Options) of
        {ok, {meta, <<>>, EventNumber}} ->
            {ok, {meta, #stream_meta{}, EventNumber}};
        {ok, {meta, RawMeta, EventNumber}} ->
            F = fun(_, _, _) -> {error, bad_json} end,
            Opts = [{error_handler, F}, {incomplete_handler, F}],
            case jsx:decode(RawMeta, Opts) of
                {error, bad_json} -> {error, bad_json};
                MetaJson -> {ok, {meta, erles_utils:metajson_to_meta(MetaJson), EventNumber}}
            end;
        {error, Reason} ->
            {error, Reason}
    end;

get_metadata(Pid, all, raw, Options) ->
    get_metadata(Pid, <<"$$all">>, raw, Options);

get_metadata(Pid, StreamId, raw, Options) when is_list(StreamId) ->
    get_metadata(Pid, list_to_binary(StreamId), raw, Options);

get_metadata(Pid, StreamId, raw, Options) ->
    case read_event(Pid, <<"$$", StreamId/binary>>, last, Options) of
        {ok, {event, Event=#event{}, _EventPos}} ->
            {ok, {meta, Event#event.data, Event#event.event_number}};
        {error, no_stream} ->
            {ok, {meta, <<>>, -1}};
        {error, Reason} ->
            {error, Reason}
    end.

