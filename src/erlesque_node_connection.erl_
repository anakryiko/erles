-module(erlesque_node_connection).
-behavior(gen_server).
-export([start/2, start_link/2, connect/1, close/1]).
-export([append/4, append/5,
         delete/3, delete/4,
         read_event/4, read_event/5,
         read_forward/5, read_forward/6,
         read_backward/5, read_backward/6]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-define(ExpVersionAny, -2).
-define(EventNumLast, -1).

-record(state, {settings,
                name,
                endpoint,
                operations = gb_dict:empty()}).

start(Settings, Name, {IpAddr, Port}) -> 
    {ok, Pid} = gen_server:start(?MODULE,
                                 #state{settings=Settings, name=Name, endpoint={IpAddr, Port}},
                                 []),
    {node, Pid}.

start_link(Settings, Name, {IpAddr, Port}) -> 
    {ok, Pid} = gen_server:start_link(?MODULE,
                                      #state{settings=Settings, name=Name, endpoint={IpAddr, Port}},
                                      []),
    {node, Pid}.

%%% CONNECTION LIFE-CYCLE
connect({node, Pid}) ->
    gen_server:call(Pid, connect).

close({node, Pid}) ->
    gen_server:call(Pid, close).

%%% APPEND EVENTS TO STREAM
%append({node, Pid}, Stream, ExpVersion, Events) where ExpVersion >= ?ExpVersionAny ->
%    gen_server:call(Pid, {append, Stream, ExpVersion, Events, no_credentials});
%
%append({node, Pid}, Stream, any, Events) ->
%    gen_server:call(Pid, {append, Stream, ?ExpVersionAny, Events, no_credentials}).
%
%append({node, Pid}, Stream, ExpVersion, Events, {Login, Pass}) where ExpVersion >= ?ExpVersionAny ->
%    gen_server:call(Pid, {append, Stream, ExpVersion, Events, {Login, Pass}});
%
%append({node, Pid}, Stream, any, Events, {Login, Pass}) ->
%    gen_server:call(Pid, {append, Stream, ?ExpVersionAny, Events, {Login, Pass}}).

%%% DELETE STREAM
%delete({node, Pid}, Stream, ExpVersion) where ExpVersion >= ?ExpVersionAny ->
%    gen_server:call(Pid, {delete, Stream, ExpVersion, no_credentials});
%
%delete({node, Pid}, Stream, any) ->
%    gen_server:call(Pid, {delete, Stream, ?ExpVersionAny, no_credentials}).
%
%delete({node, Pid}, Stream, ExpVersion, {Login, Pass}) where ExpVersion >= ?ExpVersionAny ->
%    gen_server:call(Pid, {delete, Stream, ExpVersion, {Login, Pass}});
%
%delete({node, Pid}, Stream, any, {Login, Pass}) ->
%    gen_server:call(Pid, {delete, Stream, ?ExpVersionAny, {Login, Pass}}).

%%% READ SINGLE EVENT
%read_event({node, Pid}, Stream, EventNumber, ResolveLinks) when EventNumber >= ?EventNumLast ->
%    gen_server:call(Pid, {read_event, Stream, EventNumber, ResolveLinks, no_credentials});
%
%read_event({node, Pid}, Stream, last, ResolveLinks) ->
%    gen_server:call(Pid, {read_event, Stream, ?EventNumLast, ResolveLinks, no_credentials}).
%
%read_event({node, Pid}, Stream, EventNumber, ResolveLinks, {Login, Pass}) when EventNumber >= ?EventNumLast ->
%    gen_server:call(Pid, {read_event, Stream, EventNumber, ResolveLinks, {Login, Pass}});
%
%read_event({node, Pid}, Stream, last, ResolveLinks, {Login, Pass}) ->
%    gen_server:call(Pid, {read_event, Stream, ?EventNumLast, ResolveLinks, {Login, Pass}}).

%%% READ STREAM/$ALL FORWARD
%read_forward({node, Pid}, Stream, From, MaxCount, ResolveLinks) when From >= 0, MaxCount >= 0 ->
%    gen_server:call(Pid, {read_stream_forward, Stream, From, MaxCount, ResolveLinks, no_credentials});
%
%read_forward({node, Pid}, all, From, MaxCount, ResolveLinks) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_all_forward, From, MaxCount, ResolveLinks, no_credentials}).
%
%read_forward({node, Pid}, Stream, From, MaxCount, ResolveLinks, {Login, Pass}) when From >= 0, MaxCount >= 0 ->
%    gen_server:call(Pid, {read_stream_forward, Stream, From, MaxCount, ResolveLinks, {Login, Pass}});
%
%read_forward({node, Pid}, all, From, MaxCount, ResolveLinks, {Login, Pass}) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_all_forward, From, MaxCount, ResolveLinks, {Login, Pass}});

%%% READ STREAM BACKWARD
%read_backward({node, Pid}, Stream, From, MaxCount, ResolveLinks) when From >= ?EventNumLast, MaxCount >= 0 ->
%    gen_server:call(Pid, {read_stream_backward, Stream, From, MaxCount, ResolveLinks, no_credentials});
%
%read_backward({node, Pid}, Stream, last, MaxCount, ResolveLinks) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_stream_backward, Stream, ?EventNumLast, MaxCount, ResolveLinks, no_credentials});
%
%read_backward({node, Pid}, all, From, MaxCount, ResolveLinks) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_all_backward, From, MaxCount, ResolveLinks, no_credentials});
%
%read_backward({node, Pid}, all, last, MaxCount, ResolveLinks) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_all_backward, last, MaxCount, ResolveLinks, no_credentials}).

%read_backward({node, Pid}, Stream, From, MaxCount, ResolveLinks, {Login, Pass}) when From >= ?EventNumLast, MaxCount >= 0 ->
%    gen_server:call(Pid, {read_stream_backward, Stream, From, MaxCount, ResolveLinks, {Login, Pass}});
%
%read_backward({node, Pid}, Stream, last, MaxCount, ResolveLinks, {Login, Pass}) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_stream_backward, Stream, ?EventNumLast, MaxCount, ResolveLinks, {Login, Pass}});
%
%read_backward({node, Pid}, all, From, MaxCount, ResolveLinks, {Login, Pass}) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_all_backward, From, MaxCount, ResolveLinks, {Login, Pass}});
%
%read_backward({node, Pid}, all, last, MaxCount, ResolveLinks, {Login, Pass}) when MaxCount >= 0 ->
%    gen_server:call(Pid, {read_all_backward, last, MaxCount, ResolveLinks, {Login, Pass}}).

%%% OTP GEN_SERVER INTERNALS

init({Settings, Name}) -> {ok, {Settings, Name}}.

handle_call(connect, From, State#state{discoverer={M,F,A}}) ->
    case erlang:apply(M, F, A) of
        {ok, IpAddr, Port} ->
        {}
    end,
    throw(not_implemented).

handle_call(close, From, State) ->
    throw(not_implemented).

handle_call({append, Stream, ExpVersion, Events, Credentials}, From, State) ->
    throw(not_implemented).

handle_call({delete, Stream, ExpVersion, Credentials}, From, State) ->
    throw(not_implemented).

handle_call({read_event, Stream, EventNumber, ResolveLinks, Credentials}, From, State) ->
    throw(not_implemented).

handle_call({read_stream_forward, Stream, From, MaxCount, ResolveLinks, Credentials}, From, State) ->
    throw(not_implemented).

handle_call({read_stream_backward, Stream, From, MaxCount, ResolveLinks, Credentials}, From, State) ->
    throw(not_implemented).

handle_call({read_all_forward, From, MaxCount, ResolveLinks, Credentials}, From, State) ->
    throw(not_implemented).

handle_call({read_all_backward, From, MaxCount, ResolveLinks, Credentials}, From, State) ->
    throw(not_implemented).

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.

handle_cast(Msg, From, State) ->
    io:format("Unexpected CAST message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.

handle_info{Msg, State = #connection_settings{}} ->
    io:format("Unexpected INFO message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.

terminate(normal, State = {Settings, Name}) ->
    io:format("Terminating EventStore connection ~p~n", [Name]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.