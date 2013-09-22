-module(erlesque).
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

-export([set_metadata/4, set_metadata/5]).
-export([get_metadata/2, get_metadata/3, get_metadata/4]).

-include("erlesque.hrl").
-include("erlesque_internal.hrl").


-define(EXPECTED_VERSION_ANY, -2).
-define(REQUIRE_MASTER, true).
-define(LAST_EVENT_NUMBER, -1).
-define(LAST_LOG_POSITION, -1).

%%% METADATA AND SYS SETTINGS
-define(ET_STREAMMETADATA, <<"$metadata">>).
-define(ET_SETTINGS, <<"$settings">>).

-define(META_MAXAGE, <<"$maxAge">>).
-define(META_MAXCOUNT, <<"$maxCount">>).
-define(META_TRUNCATEBEFORE, <<"$tb">>).
-define(META_CACHECONTROL, <<"$cacheControl">>).
-define(META_ACL, <<"$acl">>).
-define(META_ACLREAD, <<"$r">>).
-define(META_ACLWRITE, <<"$w">>).
-define(META_ACLDELETE, <<"$d">>).
-define(META_ACLMETAREAD, <<"$mr">>).
-define(META_ACLMETAWRITE, <<"$mw">>).

-define(ROLE_ADMINS, <<"$admins">>).
-define(ROLE_ALL, <<"$all">>).

-define(SYS_USERSTREAMACL, <<"$userStreamAcl">>).
-define(SYS_SYSTEMSTREAMACL, <<"$systemStreamAcl">>).

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
        all -> <<>>;
        _ -> StreamId
    end,
    gen_fsm:sync_send_event(Pid, {op, {subscribe_to_stream, perm, Auth},
                                      {Stream, ResolveLinks, SubscriberPid}}, infinity).


%%% SET STREAM METADATA
set_metadata(Pid, StreamId, ExpectedVersion, RawMeta) when is_binary(RawMeta) ->
    set_metadata(Pid, StreamId, ExpectedVersion, RawMeta, []);

set_metadata(Pid, StreamId, ExpectedVersion, Meta = #stream_meta{}) ->
    set_metadata(Pid, StreamId, ExpectedVersion, Meta, []).

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


%%% GET STREAM METADATA
get_metadata(Pid, StreamId) ->
    get_metadata(Pid, StreamId, structured).

get_metadata(Pid, StreamId, structured) ->
    get_metadata(Pid, StreamId, structured, []);

get_metadata(Pid, StreamId, raw) when is_list(StreamId) ->
    get_metadata(Pid, list_to_binary(StreamId), raw);

get_metadata(Pid, StreamId, raw) ->
    get_metadata(Pid, StreamId, raw, []).

get_metadata(Pid, StreamId, structured, Options) ->
    case get_metadata(Pid, StreamId, raw, Options) of
        {ok, <<>>, EventNumber} ->
            {ok, #stream_meta{}, EventNumber};
        {ok, RawMeta, EventNumber} ->
            F = fun(_, _, _) -> {error, bad_json} end,
            Opts = [{error_handler, F}, {incomplete_handler, F}],
            case jsx:decode(RawMeta, Opts) of
                {error, bad_json} -> {error, bad_json};
                MetaJson -> {ok, metajson_to_meta(MetaJson), EventNumber}
            end;
        {error, Reason} ->
            {error, Reason}
    end;

get_metadata(Pid, StreamId, raw, Options) ->
    case read_event(Pid, <<"$$", StreamId/binary>>, last, Options) of
        {ok, Event=#event{}} ->
            {ok, Event#event.data, Event#event.event_number};
        {error, no_stream} ->
            {ok, <<>>, -1};
        {error, Reason} ->
            {error, Reason}
    end.


metajson_to_meta(MetaJson) ->
    Meta = metajson_to_meta(MetaJson, #stream_meta{}),
    case Meta#stream_meta.custom of
        undefined -> Meta;
        List when is_list(List) -> Meta#stream_meta{custom=lists:reverse(List)}
    end.

metajson_to_meta([], Meta)                    -> Meta;
metajson_to_meta([{}], Meta)                  -> Meta;
metajson_to_meta([{Key, Value} | Tail], Meta) -> metajson_to_meta(Tail, metajson_keyval_to_meta(Key, Value, Meta)).

metajson_keyval_to_meta(?META_MAXCOUNT, Value, Meta)       -> Meta#stream_meta{max_count=Value};
metajson_keyval_to_meta(?META_MAXAGE, Value, Meta)         -> Meta#stream_meta{max_age=Value};
metajson_keyval_to_meta(?META_TRUNCATEBEFORE, Value, Meta) -> Meta#stream_meta{truncate_before=Value};
metajson_keyval_to_meta(?META_CACHECONTROL, Value, Meta)   -> Meta#stream_meta{cache_control=Value};
metajson_keyval_to_meta(?META_ACL, Value, Meta)            -> Meta#stream_meta{acl=acljson_to_acl(Value, #stream_acl{})};
metajson_keyval_to_meta(Custom, Value, Meta=#stream_meta{custom=undefined}) -> Meta#stream_meta{custom=[{Custom, Value}]};
metajson_keyval_to_meta(Custom, Value, Meta=#stream_meta{custom=List})      -> Meta#stream_meta{custom=[{Custom, Value} | List]}.

acljson_to_acl([], Acl)                    -> Acl;
acljson_to_acl([{}], Acl)                  -> Acl;
acljson_to_acl([{Key, Value} | Tail], Acl) -> acljson_to_acl(Tail, acljson_keyval_to_acl(Key, Value, Acl)).

acljson_keyval_to_acl(?META_ACLREAD, Value, Acl)      -> Acl#stream_acl{read_roles=canon_role(Value)};
acljson_keyval_to_acl(?META_ACLWRITE, Value, Acl)     -> Acl#stream_acl{write_roles=canon_role(Value)};
acljson_keyval_to_acl(?META_ACLDELETE, Value, Acl)    -> Acl#stream_acl{delete_roles=canon_role(Value)};
acljson_keyval_to_acl(?META_ACLMETAREAD, Value, Acl)  -> Acl#stream_acl{metaread_roles=canon_role(Value)};
acljson_keyval_to_acl(?META_ACLMETAWRITE, Value, Acl) -> Acl#stream_acl{metawrite_roles=canon_role(Value)}.

canon_role(Role) when is_binary(Role) -> [Role]; % turn single role string into single element array
canon_role(Roles) when is_list(Roles) -> Roles.
