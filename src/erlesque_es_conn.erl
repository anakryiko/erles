-module(erlesque_es_conn).
-behavior(gen_server).

-export([start_link/2, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {stub_pid, ip, port}).

start_link(Ip, Port) ->
    gen_server:start_link(?MODULE, {Ip, Port}, []).

stop(Pid) ->
    gen_server:call(Pid, stop).


init({Ip, Port}) ->
    {ok, StubPid} = erlesque_stubborn_conn:start_link(self(), Ip, Port),
    {ok, #state{stub_pid=StubPid, ip=Ip, port=Port}}.


handle_call(stop, _From, State = #state{stub_pid=StubPid}) ->
    ok = erlesque_stubborn_conn:stop(StubPid),
    {stop, normal, ok, State};

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({package, Data}, State) ->
    Pkg = erlesque_pkg:from_binary(Data),
    NewState = handle_pkg(State, Pkg),
    {noreply, NewState};

handle_info({connected, _Ip, _Port}, State = #state{ip=_Ip, port=_Port}) ->
    {noreply, State};

handle_info({disconnected, _Reason}, State = #state{ip=_Ip, port=_Port}) ->
    {noreply, State};

handle_info(reconnecting, State) ->
    {noreply, State};

handle_info({closed, Reason}, State) ->
    io:format("Connection stopped. Reason: ~p.~n", [Reason]),
    {noreply, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_pkg(State = #state{stub_pid=StubPid}, {pkg, heartbeat_req, CorrId, Auth, Data}) ->
    PkgBin = erlesque_pkg:to_binary({pkg, heartbeat_resp, CorrId, Auth, Data}),
    erlesque_stubborn_conn:send(StubPid, PkgBin),
    State;

handle_pkg(State, _Pkg) ->
    State.

