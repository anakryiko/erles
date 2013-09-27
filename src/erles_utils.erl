-module(erles_utils).
-export([gen_uuid/0, uuid_to_string/1, uuid_to_string/2]).
-export([parse_ip/1]).
-export([shuffle/1]).
-export([resolved_event/2]).
-export([meta_to_metajson/1, metajson_to_meta/1]).

-include("erles_clientapi_pb.hrl").
-include("erles.hrl").
-include("erles_internal.hrl").

%%% UUID routines were taken from
%%% https://github.com/okeuday/uuid/blob/master/src/uuid.erl

%%% Copyright (c) 2011-2013, Michael Truog <mjtruog at gmail dot com>
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%
%%%     * Redistributions of source code must retain the above copyright
%%%       notice, this list of conditions and the following disclaimer.
%%%     * Redistributions in binary form must reproduce the above copyright
%%%       notice, this list of conditions and the following disclaimer in
%%%       the documentation and/or other materials provided with the
%%%       distribution.
%%%     * All advertising materials mentioning features or use of this
%%%       software must display the following acknowledgment:
%%%         This product includes software developed by Michael Truog
%%%     * The name of the author may not be used to endorse or promote
%%%       products derived from this software without specific prior
%%%       written permission
%%%
%%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
%%% CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
%%% INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
%%% OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
%%% DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
%%% CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%%% SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
%%% BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
%%% SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
%%% INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
%%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
%%% NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
%%% DAMAGE.

-spec gen_uuid() -> uuid().
gen_uuid() ->
    <<Rand1:48, _:4, Rand2:12, _:2, Rand3:62>> = crypto:rand_bytes(16),
    <<Rand1:48,
      0:1, 1:1, 0:1, 0:1,  % version 4 bits
      Rand2:12,
      1:1, 0:1,            % RFC 4122 variant bits
      Rand3:62>>.


-spec uuid_to_list(Value :: uuid()) -> iolist().
uuid_to_list(Value)
    when is_binary(Value), byte_size(Value) == 16 ->
    <<B1:32/unsigned-integer,
      B2:16/unsigned-integer,
      B3:16/unsigned-integer,
      B4:16/unsigned-integer,
      B5:48/unsigned-integer>> = Value,
    [B1, B2, B3, B4, B5].


-spec uuid_to_string(Value :: uuid()) -> string().
uuid_to_string(Value) ->
    uuid_to_string(Value, standard).


-spec uuid_to_string(Value :: uuid(), 'standard' | 'nodash') -> string().
uuid_to_string(Value, standard) ->
    [B1, B2, B3, B4, B5] = uuid_to_list(Value),
    lists:flatten(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~4.16.0b-~12.16.0b",
                                [B1, B2, B3, B4, B5]));

uuid_to_string(Value, nodash) ->
    [B1, B2, B3, B4, B5] = uuid_to_list(Value),
    lists:flatten(io_lib:format("~8.16.0b~4.16.0b~4.16.0b~4.16.0b~12.16.0b",
                                [B1, B2, B3, B4, B5])).


-spec parse_ip(Value :: binary() | string()) -> inet:ip_address().
parse_ip(Bin) when is_binary(Bin) -> parse_ip(binary_to_list(Bin));
parse_ip(String)                  -> {ok, Addr} = inet:parse_address(String), Addr.


-spec shuffle(List :: [T]) -> [T].
shuffle(L) when is_list(L) ->
    [X || {_, X} <- lists:sort([{random:uniform(), Y} || Y <- L])].


-spec resolved_event('all', #resolvedevent{})                              -> all_event_res();
                    ('stream', #resolvedevent{} | #resolvedindexedevent{}) -> stream_event_res().

resolved_event(all, E = #resolvedevent{}) ->
    ResolvedEvent = event_rec(E#resolvedevent.event),
    OrigPos = {tfpos, E#resolvedevent.commit_position, E#resolvedevent.prepare_position},
    {event, ResolvedEvent, OrigPos};

resolved_event(stream, E = #resolvedevent{}) ->
    OrigEvent = case E#resolvedevent.link of
        undefined -> E#resolvedevent.event;
        Link -> Link
    end,
    ResolvedEvent = event_rec(E#resolvedevent.event),
    OrigEventNumber = OrigEvent#eventrecord.event_number,
    {event, ResolvedEvent, OrigEventNumber};

resolved_event(stream, E = #resolvedindexedevent{}) ->
    OrigEvent = case E#resolvedindexedevent.link of
        undefined -> E#resolvedindexedevent.event;
        Link -> Link
    end,
    ResolvedEvent = event_rec(E#resolvedindexedevent.event),
    OrigEventNumber  = OrigEvent#eventrecord.event_number,
    {event, ResolvedEvent, OrigEventNumber}.

-spec event_rec(EventRecord :: #eventrecord{}) -> #event{}.
event_rec(E = #eventrecord{}) ->
    #event{stream_id    = list_to_binary(E#eventrecord.event_stream_id),
           event_number = E#eventrecord.event_number,
           event_id     = E#eventrecord.event_id,
           event_type   = list_to_binary(E#eventrecord.event_type),
           data         = E#eventrecord.data,
           metadata     = E#eventrecord.metadata}.

-spec meta_to_metajson(stream_meta()) -> jsx:json_term().
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

-spec meta_to_metajson(term(), 'custom' | binary(), jsx:json_term()) -> jsx:json_term().
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


-spec metajson_to_meta(jsx:json_term()) -> stream_meta().
metajson_to_meta(MetaJson) ->
    Meta = lists:foldl(fun metajson_keyval_to_meta/2, #stream_meta{}, MetaJson),
    case Meta#stream_meta.custom of
        undefined -> Meta;
        List when is_list(List) -> Meta#stream_meta{custom=lists:reverse(List)}
    end.

-spec metajson_keyval_to_meta({} | {binary(), jsx:json_term()}, stream_meta()) -> stream_meta().
metajson_keyval_to_meta({}, Meta)                            -> Meta;
metajson_keyval_to_meta({?META_MAXCOUNT, Value}, Meta)       -> Meta#stream_meta{max_count=Value};
metajson_keyval_to_meta({?META_MAXAGE, Value}, Meta)         -> Meta#stream_meta{max_age=Value};
metajson_keyval_to_meta({?META_TRUNCATEBEFORE, Value}, Meta) -> Meta#stream_meta{truncate_before=Value};
metajson_keyval_to_meta({?META_CACHECONTROL, Value}, Meta)   -> Meta#stream_meta{cache_control=Value};
metajson_keyval_to_meta({?META_ACL, Value}, Meta)            -> Meta#stream_meta{acl=acljson_to_acl(Value)};
metajson_keyval_to_meta({Custom, Value}, Meta=#stream_meta{custom=undefined}) -> Meta#stream_meta{custom=[{Custom, Value}]};
metajson_keyval_to_meta({Custom, Value}, Meta=#stream_meta{custom=List})      -> Meta#stream_meta{custom=[{Custom, Value} | List]}.


-spec acljson_to_acl(jsx:json_term()) -> stream_acl().
acljson_to_acl(AclJson) -> lists:foldl(fun acljson_keyval_to_acl/2, #stream_acl{}, AclJson).

-spec acljson_keyval_to_acl({} | {binary(), jsx:json_term()}, stream_acl()) -> stream_acl().
acljson_keyval_to_acl({}, Acl)                          -> Acl;
acljson_keyval_to_acl({?META_ACLREAD, Value}, Acl)      -> Acl#stream_acl{read_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLWRITE, Value}, Acl)     -> Acl#stream_acl{write_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLDELETE, Value}, Acl)    -> Acl#stream_acl{delete_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLMETAREAD, Value}, Acl)  -> Acl#stream_acl{metaread_roles=canon_role(Value)};
acljson_keyval_to_acl({?META_ACLMETAWRITE, Value}, Acl) -> Acl#stream_acl{metawrite_roles=canon_role(Value)}.

-spec canon_role(binary()) -> [binary()];
                ([T]) -> [T].
canon_role(Role) when is_binary(Role) -> [Role]; % turn single role string into single element array
canon_role(Roles) when is_list(Roles) -> Roles.
