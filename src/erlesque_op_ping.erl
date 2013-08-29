-module(erlesque_op_ping).
-behavior(gen_server).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

start_link(CorrId, ConnPid, Params) ->
    gen_server:start_link(?MODULE, {CorrId, ConnPid, Params}, []).

init({CorrId, ConnPid, _Params}) ->
    erlesque_conn:send(ConnPid, erlesque_pkg:create(ping, CorrId, <<1,2,3>>)),
    {ok, State}.


handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.

handle_cast(restart, Msg, State) ->
    {noreply, State}.

handle_cast(pause, Msg, State) ->
    {noreply, State}.

handle_cast(abort, Msg, State) ->
    {noreply, State}.

handle_cast({pkg, pong, _CorrId, _Auth, _Data}, Msg, {_, _}) ->

    {noreply, State}.

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
