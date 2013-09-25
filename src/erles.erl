-module(erles).

-export([start/0]).
-export([connect/2, connect/3, close/1]).
-export([ping/1]).

-export([append/4, append/5]).
-export([transaction_start/3, transaction_start/4]).
-export([transaction_write/3, transaction_write/4]).
-export([transaction_commit/2, transaction_commit/3]).
-export([delete/3, delete/4]).

-export([read_event/3, read_event/4]).
-export([read_stream/4, read_stream/5, read_stream/6]).

-export([subscribe/2, subscribe/3, unsubscribe/1]).
-export([subscribe_perm/2, subscribe_perm/3, subscribe_perm/4, unsubscribe_perm/1]).

-export([set_metadata/4, set_metadata/5]).
-export([get_metadata/2, get_metadata/3, get_metadata/4]).

-include("erles.hrl").
-include("erles_internal.hrl").

%% Use this function when working with Erles from REPL to start all dependencies
start() ->
    application:load(erles),
    {ok, Apps} = application:get_key(erles, applications),
    lists:foreach(fun(App) -> application:start(App) end, Apps),
    application:start(erles).

%% CONNECT
connect(node, {Ip, Port})
        when is_integer(Port), Port > 0, Port < 65536 ->
    erles_fsm:start_link({node, Ip, Port});

connect(dns, {ClusterDns, ManagerPort})
        when is_list(ClusterDns) orelse is_binary(ClusterDns),
             is_integer(ManagerPort), ManagerPort > 0, ManagerPort < 65536 ->
    erles_fsm:start_link({dns, ClusterDns, ManagerPort});

connect(cluster, SeedNodes)
        when is_list(SeedNodes) ->
    erles_fsm:start_link({cluster, SeedNodes}).


connect(node, {Ip, Port}, Options)
        when is_integer(Port), Port > 0, Port < 65536,
             is_list(Options) ->
    erles_fsm:start_link({node, Ip, Port}, Options);

connect(dns, {ClusterDns, ManagerPort}, Options)
        when is_list(ClusterDns) orelse is_binary(ClusterDns),
             is_integer(ManagerPort), ManagerPort > 0, ManagerPort < 65536,
             is_list(Options) ->
    erles_fsm:start_link({dns, ClusterDns, ManagerPort}, Options);

connect(cluster, SeedNodes, Options)
        when is_list(SeedNodes),
             is_list(Options) ->
    erles_fsm:start_link({cluster, SeedNodes}, Options).

%% CLOSE
close(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, close).


%% PING
ping(Pid) ->
    gen_fsm:sync_send_event(Pid, {op, {ping, temp, noauth}, {}}, infinity).


%% APPEND EVENTS TO STREAM
append(Pid, StreamId, ExpectedVersion, Events) ->
    append(Pid, StreamId, ExpectedVersion, Events, []).

append(Pid, StreamId, ExpectedVersion, Events, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPVER_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    append(Pid, StreamId, ExpVer, Events, Auth, RequireMaster).

append(Pid, StreamId, ExpectedVersion, Events, Auth, RequireMaster)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPVER_ANY,
             is_list(Events),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid,
                            {op, {write_events, temp, Auth},
                                 {StreamId, ExpectedVersion, Events, RequireMaster}}, infinity).

%% START EXPLICIT TRANSACTION
transaction_start(Pid, StreamId, ExpectedVersion) ->
    transaction_start(Pid, StreamId, ExpectedVersion, []).

transaction_start(Pid, StreamId, ExpectedVersion, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPVER_ANY;
        _ -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    transaction_start(Pid, StreamId, ExpVer, Auth, RequireMaster).

transaction_start(Pid, StreamId, ExpectedVersion, Auth, RequireMaster)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPVER_ANY,
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_start, temp, Auth},
                                      {StreamId, ExpectedVersion, RequireMaster}}, infinity).


%% WRITE EVENTS IN EXPLICIT TRANSACTION
transaction_write(Pid, TransactionId, Events) ->
    transaction_write(Pid, TransactionId, Events, []).

transaction_write(Pid, TransactionId, Events, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    transaction_write(Pid, TransactionId, Events, Auth, RequireMaster).

transaction_write(Pid, TransactionId, Events, Auth, RequireMaster)
        when is_integer(TransactionId), TransactionId >= 0,
             is_list(Events),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_write, temp, Auth},
                                      {TransactionId, Events, RequireMaster}}, infinity).


%% COMMIT EXPLICIT TRANSACTION
transaction_commit(Pid, TransactionId) ->
    transaction_commit(Pid, TransactionId, []).

transaction_commit(Pid, TransactionId, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    transaction_commit(Pid, TransactionId, Auth, RequireMaster).

transaction_commit(Pid, TransactionId, Auth, RequireMaster)
        when is_integer(TransactionId), TransactionId >= 0,
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {transaction_commit, temp, Auth},
                                      {TransactionId, RequireMaster}}, infinity).


%% DELETE STREAM
delete(Pid, StreamId, ExpectedVersion) ->
    delete(Pid, StreamId, ExpectedVersion, []).

delete(Pid, StreamId, ExpectedVersion, Options) ->
    ExpVer = case ExpectedVersion of
        any -> ?EXPVER_ANY;
        _   -> ExpectedVersion
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    delete(Pid, StreamId, ExpVer, Auth, RequireMaster).

delete(Pid, StreamId, ExpectedVersion, Auth, RequireMaster)
    when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPVER_ANY,
         is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {delete_stream, temp, Auth},
                                      {StreamId, ExpectedVersion, RequireMaster}}, infinity).


%% READ SINGLE EVENT
read_event(Pid, StreamId, EventNumber) ->
    read_event(Pid, StreamId, EventNumber, []).

read_event(Pid, StreamId, EventNumber, Options) ->
    Pos = case EventNumber of
        first -> 0;
        last  -> ?LAST_EVENT_NUM;
        _     -> EventNumber
    end,
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    read_event(Pid, StreamId, Pos, Auth, ResolveLinks, RequireMaster).

read_event(Pid, StreamId, EventNumber, Auth, ResolveLinks, RequireMaster)
        when is_integer(EventNumber), EventNumber >= ?LAST_EVENT_NUM,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_event, temp, Auth},
                                      {StreamId, EventNumber, ResolveLinks, RequireMaster}}, infinity).


%% READ STREAM OR ALL EVENTS
read_stream(Pid, StreamId, From, MaxCount) ->
    read_stream(Pid, StreamId, From, MaxCount, forward, []).

read_stream(Pid, StreamId, From, MaxCount, Direction) ->
    read_stream(Pid, StreamId, From, MaxCount, Direction, []).

read_stream(Pid, StreamId, From, MaxCount, Direction, Options) ->
    FromPos = stream_pos(StreamId, From, Direction),
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    RequireMaster = proplists:get_value(require_master, Options, ?DEF_REQ_MASTER),
    read_stream(Pid, StreamId, FromPos, MaxCount, Direction, Auth, ResolveLinks, RequireMaster).

read_stream(Pid, all, From={tfpos, CommitPos, PreparePos}, MaxCount, forward, Auth, ResolveLinks, RequireMaster)
        when is_integer(CommitPos), CommitPos >= 0,
             is_integer(PreparePos), PreparePos >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_all_events_forward, temp, Auth},
                                      {From, MaxCount, ResolveLinks, RequireMaster}}, infinity);

read_stream(Pid, all, From={tfpos, CommitPos, PreparePos}, MaxCount, backward, Auth, ResolveLinks, RequireMaster)
        when is_integer(CommitPos), CommitPos >= ?LAST_LOG_POS,
             is_integer(PreparePos), PreparePos >= ?LAST_LOG_POS,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_all_events_backward, temp, Auth},
                                      {From, MaxCount, ResolveLinks, RequireMaster}}, infinity);

read_stream(Pid, StreamId, From, MaxCount, forward, Auth, ResolveLinks, RequireMaster)
        when is_integer(From), From >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_stream_events_forward, temp, Auth},
                                      {StreamId, From, MaxCount, ResolveLinks, RequireMaster}}, infinity);

read_stream(Pid, StreamId, From, MaxCount, backward, Auth, ResolveLinks, RequireMaster)
        when is_integer(From), From >= ?LAST_EVENT_NUM,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks),
             is_boolean(RequireMaster) ->
    gen_fsm:sync_send_event(Pid, {op, {read_stream_events_backward, temp, Auth},
                                      {StreamId, From, MaxCount, ResolveLinks, RequireMaster}}, infinity).

stream_pos(all, first, forward)             -> {tfpos, 0, 0};
stream_pos(all, last, backward)             -> {tfpos, ?LAST_LOG_POS, ?LAST_LOG_POS};
stream_pos(all, {tfpos, C, P}, _Dir)        -> {tfpos, C, P};
stream_pos(_StreamId, first, forward)       -> 0;
stream_pos(_StreamId, last, backward)       -> ?LAST_EVENT_NUM;
stream_pos(_StreamId, EventNumber, _Dir)    -> EventNumber.


%% PRIMITIVE SUBSCRIPTION
subscribe(Pid, StreamId) ->
    subscribe(Pid, StreamId, []).

subscribe(Pid, StreamId, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    SubscriberPid = proplists:get_value(subscriber, Options, self()),
    subscribe(Pid, StreamId, Auth, ResolveLinks, SubscriberPid).

subscribe(Pid, StreamId, Auth, ResolveLinks, SubscriberPid) ->
    gen_fsm:sync_send_event(Pid, {op, {subscribe_to_stream, perm, Auth},
                                      {StreamId, ResolveLinks, SubscriberPid}}, infinity).

unsubscribe(SubscriptionPid) ->
    gen_fsm:send_event(SubscriptionPid, unsubscribe).


%% PERMANENT CATCH-UP SUBSCRIPTION
subscribe_perm(Pid, StreamId) ->
    subscribe_perm(Pid, StreamId, live, []).

subscribe_perm(Pid, StreamId, From) ->
    subscribe_perm(Pid, StreamId, From, []).

subscribe_perm(Pid, StreamId, From, Options) ->
    Auth = proplists:get_value(auth, Options, ?DEF_AUTH),
    ResolveLinks = proplists:get_value(resolve, Options, ?DEF_RESOLVE),
    SubscriberPid = proplists:get_value(subscriber, Options, self()),
    MaxCount = proplists:get_value(max_count, Options, 100),
    erles_perm_sub:start_link(Pid, StreamId, From, SubscriberPid, Auth, ResolveLinks, MaxCount).

unsubscribe_perm(SubscrPid) ->
    erles_perm_sub:stop(SubscrPid).


%% SET STREAM METADATA
set_metadata(Pid, StreamId, ExpectedVersion, Meta) ->
    set_metadata(Pid, StreamId, ExpectedVersion, Meta, []).

set_metadata(Pid, all, ExpectedVersion, RawMeta, Options) ->
    set_metadata(Pid, <<"$$all">>, ExpectedVersion, RawMeta, Options);

set_metadata(Pid, StreamId, ExpectedVersion, RawMeta, Options) when is_binary(RawMeta) ->
    MetaEvent = #event_data{data=RawMeta, data_type=raw, event_type=?ET_STREAMMETADATA},
    append(Pid, <<"$$", StreamId/binary>>, ExpectedVersion, [MetaEvent], Options);

set_metadata(Pid, StreamId, ExpectedVersion, Meta = #stream_meta{}, Options) ->
    Json = meta_to_metajson(Meta),
    JsonBin = jsx:encode(Json),
    MetaEvent = #event_data{data=JsonBin, data_type=json, event_type=?ET_STREAMMETADATA},
    append(Pid, <<"$$", StreamId/binary>>, ExpectedVersion, [MetaEvent], Options).


meta_to_metajson(Meta) ->
    J1 = meta_to_metajson(Meta#stream_meta.custom, custom, []),
    J2 = meta_to_metajson(Meta#stream_meta.acl, ?META_ACL, J1),
    J3 = meta_to_metajson(Meta#stream_meta.cache_control, ?META_CACHECONTROL, J2),
    J4 = meta_to_metajson(Meta#stream_meta.truncate_before, ?META_TRUNCATEBEFORE, J3),
    J5 = meta_to_metajson(Meta#stream_meta.max_age, ?META_MAXAGE, J4),
    J6 = meta_to_metajson(Meta#stream_meta.max_count, ?META_MAXCOUNT, J5),
    case J6 of
        [] -> [{}]; % JSX's notion of empty JSON object
        NonEmptyJson -> NonEmptyJson
    end.

meta_to_metajson(undefined, _JsonKey, MetaJson) ->
    MetaJson;

meta_to_metajson(Acl=#stream_acl{}, JsonKey=?META_ACL, MetaJson) ->
    J1 = meta_to_metajson(Acl#stream_acl.metawrite_roles, ?META_ACLMETAWRITE, []),
    J2 = meta_to_metajson(Acl#stream_acl.metaread_roles, ?META_ACLMETAREAD, J1),
    J3 = meta_to_metajson(Acl#stream_acl.delete_roles, ?META_ACLDELETE, J2),
    J4 = meta_to_metajson(Acl#stream_acl.write_roles, ?META_ACLWRITE, J3),
    J5 = meta_to_metajson(Acl#stream_acl.read_roles, ?META_ACLREAD, J4),
    AclJson = case J5 of
        [] -> [{}]; % JSX's notion of empty JSON object
        NonEmptyJson -> NonEmptyJson
    end,
    [{JsonKey, AclJson} | MetaJson];

meta_to_metajson(Values, custom, MetaJson) ->
    Values ++ MetaJson;

meta_to_metajson(Value, JsonKey, MetaJson) ->
    [{JsonKey, Value} | MetaJson].


%% GET STREAM METADATA
get_metadata(Pid, StreamId) ->
    get_metadata(Pid, StreamId, struct).

get_metadata(Pid, StreamId, struct) ->
    get_metadata(Pid, StreamId, struct, []);

get_metadata(Pid, StreamId, raw) when is_list(StreamId) ->
    get_metadata(Pid, list_to_binary(StreamId), raw);

get_metadata(Pid, StreamId, raw) ->
    get_metadata(Pid, StreamId, raw, []).

get_metadata(Pid, StreamId, struct, Options) ->
    case get_metadata(Pid, StreamId, raw, Options) of
        {ok, {meta, <<>>, EventNumber}} ->
            {ok, {meta, #stream_meta{}, EventNumber}};
        {ok, {meta, RawMeta, EventNumber}} ->
            F = fun(_, _, _) -> {error, bad_json} end,
            Opts = [{error_handler, F}, {incomplete_handler, F}],
            case jsx:decode(RawMeta, Opts) of
                {error, bad_json} -> {error, bad_json};
                MetaJson -> {ok, {meta, metajson_to_meta(MetaJson), EventNumber}}
            end;
        {error, Reason} ->
            {error, Reason}
    end;

get_metadata(Pid, all, raw, Options) ->
    get_metadata(Pid, <<"$$all">>, raw, Options);

get_metadata(Pid, StreamId, raw, Options) ->
    case read_event(Pid, <<"$$", StreamId/binary>>, last, Options) of
        {ok, {event, Event=#event{}, _EventPos}} ->
            {ok, {meta, Event#event.data, Event#event.event_number}};
        {error, no_stream} ->
            {ok, {meta, <<>>, -1}};
        {error, Reason} ->
            {error, Reason}
    end.


metajson_to_meta(MetaJson) ->
    Meta = lists:foldl(fun metajson_keyval_to_meta/2, #stream_meta{}, MetaJson),
    case Meta#stream_meta.custom of
        undefined -> Meta;
        List when is_list(List) -> Meta#stream_meta{custom=lists:reverse(List)}
    end.

metajson_keyval_to_meta({}, Meta)                            -> Meta;
metajson_keyval_to_meta({?META_MAXCOUNT, Value}, Meta)       -> Meta#stream_meta{max_count=Value};
metajson_keyval_to_meta({?META_MAXAGE, Value}, Meta)         -> Meta#stream_meta{max_age=Value};
metajson_keyval_to_meta({?META_TRUNCATEBEFORE, Value}, Meta) -> Meta#stream_meta{truncate_before=Value};
metajson_keyval_to_meta({?META_CACHECONTROL, Value}, Meta)   -> Meta#stream_meta{cache_control=Value};
metajson_keyval_to_meta({?META_ACL, Value}, Meta)            -> Meta#stream_meta{acl=acljson_to_acl(Value)};
metajson_keyval_to_meta({Custom, Value}, Meta=#stream_meta{custom=undefined}) -> Meta#stream_meta{custom=[{Custom, Value}]};
metajson_keyval_to_meta({Custom, Value}, Meta=#stream_meta{custom=List})      -> Meta#stream_meta{custom=[{Custom, Value} | List]}.

acljson_to_acl(AclJson) -> lists:foldl(fun acljson_keyval_to_acl/2, #stream_acl{}, AclJson).

acljson_keyval_to_acl({}, Acl)                          -> Acl;
acljson_keyval_to_acl({?META_ACLREAD, Value}, Acl)      -> Acl#stream_acl{read_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLWRITE, Value}, Acl)     -> Acl#stream_acl{write_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLDELETE, Value}, Acl)    -> Acl#stream_acl{delete_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLMETAREAD, Value}, Acl)  -> Acl#stream_acl{metaread_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLMETAWRITE, Value}, Acl) -> Acl#stream_acl{metawrite_roles=canon_role(Value)}.

canon_role(Role) when is_binary(Role) -> [Role]; % turn single role string into single element array
canon_role(Roles) when is_list(Roles) -> Roles.
