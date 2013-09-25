-module(erles_utils).
-export([gen_uuid/0, uuid_to_string/1, uuid_to_string/2]).
-export([parse_ip/1]).
-export([shuffle/1]).
-export([resolved_event/2]).

-include("erles_clientapi_pb.hrl").
-include("erles.hrl").

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


-spec uuid_to_string(Value :: uuid) -> string().
uuid_to_string(Value) ->
    uuid_to_string(Value, standard).


-spec uuid_to_string(Value :: uuid, 'standard' | 'nodash') -> string().
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


-spec resolved_event('all', #resolvedevent{})                             -> {'event', #event{}, tfpos()};
                    ('stream', #resolvedevent{} | #resolvedindexedevent{}) -> {'event', #event{}, event_num()}.

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
