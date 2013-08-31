-module(erlesque_fsm).
-behavior(gen_fsm).

-export([start_link/1, operation_completed/3, operation_restarted/3, reconnect/3]).

-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, code_change/4, terminate/3]).
-export([connecting/2, connecting/3, connected/2, connected/3]).

-record(state, {conn_pid,
                sup_pid,
                waiting_ops = queue:new(),
                active_ops = dict:new()}).

start_link(ConnSettings = {node, _Ip, _Port}) ->
    gen_fsm:start_link(?MODULE, ConnSettings, []);

start_link(ConnSettings = {cluster, _DnsEntry, _ManagerPort}) ->
    gen_fsm:start_link(?MODULE, ConnSettings, []).


operation_completed(Pid, CorrId, Result) ->
    gen_fsm:send_all_state_event(Pid, {op_completed, CorrId, Result}).

operation_restarted(Pid, OldCorrId, NewCorrId) ->
    gen_fsm:send_all_state_event(Pid, {op_restarted, OldCorrId, NewCorrId}).

reconnect(_Pid, _Ip, _Port) ->
    throw(not_implemented).


init(ConnSettings) ->
    {ok, SupPid} = erlesque_ops_sup:start_link(),
    {ok, ConnPid} = erlesque_conn:start_link(self(), ConnSettings),
    {ok, connecting, #state{sup_pid=SupPid, conn_pid=ConnPid}}.


connecting({op, Operation, Params}, From, State=#state{waiting_ops=WaitingOps}) ->
    NewWaitingOps = queue:in({op, From, Operation, Params}, WaitingOps),
    {next_state, connecting, State#state{waiting_ops=NewWaitingOps}};

connecting(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, connecting, State]),
    {next_state, connecting, State}.

connecting(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, connecting, State]),
    {next_state, connecting, State}.


connected({op, Operation, Params}, From, State=#state{sup_pid=SupPid, conn_pid=ConnPid, active_ops=ActiveOps}) ->
    CorrId = erlesque_utils:create_uuid_v4(),
    {ok, Pid} = erlesque_ops_sup:start_operation(SupPid, Operation, {CorrId, self(), ConnPid, 7000, 10, 500}, Params),
    erlesque_ops:connected(Pid),
    NewActiveOps = dict:store(CorrId, {op, From, Pid}, ActiveOps),
    io:format("New op started corrid ~p, op ~p, params ~p~n", [CorrId, Operation, Params]),
    {next_state, connected, State#state{active_ops=NewActiveOps}};

connected(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, connected, State]),
    {next_state, connected, State}.

connected(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, connected, State]),
    {next_state, connected, State}.


handle_sync_event(close, _From, _StateName, State=#state{conn_pid=ConnPid, active_ops=ActiveOps, waiting_ops=WaitingOps}) ->
    ok = erlesque_conn:stop(ConnPid),
    io:format("Connection stopped. Reason: ~p.~n", [closed_by_client]),
    {NewActOps, NewWaitOps} = abort_operations(ActiveOps, WaitingOps, closed_by_client),
    {stop, normal, ok, State#state{active_ops=NewActOps, waiting_ops=NewWaitOps}};

handle_sync_event(Event, From, StateName, State) ->
    io:format("Unexpected GLOBAL SYNC EVENT ~p from ~p in state ~p, state data ~p~n", [Event, From, StateName, State]),
    {next_state, StateName, State}.


handle_event({op_completed, CorrId, Res}, StateName, StateData=#state{active_ops=Ops}) ->
    NewOps = complete_operation(Ops, CorrId, Res),
    io:format("Op_completed: corrid ~p, res ~p~n", [CorrId, Res]),
    {next_state, StateName, StateData#state{active_ops=NewOps}};

handle_event({op_restarted, OldCorrId, NewCorrId}, StateName, StateData=#state{active_ops=Ops}) ->
    NewOps = restart_operation(Ops, OldCorrId, NewCorrId),
    io:format("Op_restarted: oldcorrid ~p, newcorrid ~p~n", [OldCorrId, NewCorrId]),
    {next_state, StateName, StateData#state{active_ops=NewOps}};

handle_event(Event, StateName, State) ->
    io:format("Unexpected GLOBAL ASYNC EVENT  ~p in state ~p, state data ~p~n", [Event, StateName, State]),
    {next_state, StateName, State}.


handle_info({package, Data}, StateName, State) ->
    Pkg = erlesque_pkg:from_binary(Data),
    NewState = handle_pkg(State, Pkg),
    {next_state, StateName, NewState};

handle_info({connected, _Ip, _Port}, connecting, State = #state{active_ops=Ops}) ->
    NewOps = restart_operations(Ops),
    {next_state, connected, State#state{active_ops=NewOps}};

handle_info({disconnected, _Reason}, connected, State = #state{active_ops=Ops}) ->
    NewOps = pause_operations(Ops),
    {next_state, connecting, State#state{active_ops=NewOps}};

handle_info(reconnecting, connecting, State) ->
    {next_state, connecting, State};

handle_info({closed, Reason}, _StateName, State=#state{active_ops=ActiveOps, waiting_ops=WaitingOps}) ->
    io:format("Connection stopped. Reason: ~p.~n", [Reason]),
    {NewActiveOps, NewWaitingOps} = abort_operations(ActiveOps, WaitingOps, Reason),
    {stop, normal, ok, State#state{active_ops=NewActiveOps, waiting_ops=NewWaitingOps}};

handle_info(Msg, StateName, State) ->
    io:format("Unexpected INFO message ~p, state name ~p, state data ~p~n", [Msg, StateName, State]),
    {next_state, StateName, State}.


terminate(normal, _StateName, _State) ->
    io:format("Erlesque connection terminated!~n"),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


handle_pkg(State=#state{conn_pid=ConnPid}, {pkg, heartbeat_req, CorrId, _Auth, Data}) ->
    Pkg = erlesque_pkg:create(heartbeat_resp, CorrId, Data),
    erlesque_conn:send(ConnPid, Pkg),
    State;

handle_pkg(State=#state{active_ops=Ops}, Pkg={pkg, _Cmd, CorrId, _Auth, _Data}) ->
    io:format("Package arrived: ~p~n", [Pkg]),
    case dict:find(CorrId, Ops) of
        {ok, {op, _From, Pid}} ->
            erlesque_ops:handle_pkg(Pid, Pkg);
        error ->
            io:format("No operation with corrid ~p, pkg ~p, ops ~p~n", [CorrId, Pkg, Ops]),
            ok
    end,
    State.


restart_operations(Ops) ->
    Restart = fun(_, {op, _From, Pid}, _) -> erlesque_ops:connected(Pid) end,
    dict:fold(Restart, ok, Ops),
    Ops.

pause_operations(Ops) ->
    Pause = fun(_, {op, _From, Pid}, _) -> erlesque_ops:disconnected(Pid) end,
    dict:fold(Pause, ok, Ops),
    Ops.

abort_operations(ActiveOps, WaitingOps, Reason) ->
    dict:fold(fun(_, {op, From, Pid}, _) ->
        erlesque_ops:aborted(Pid),
        gen_fsm:reply(From, {error, Reason})
    end, ok, ActiveOps),
    list:fold(fun({op, From, _Operation, _Params}, _) ->
        gen_fsm:reply(From, {error, Reason})
    end, ok, dict:to_list(WaitingOps)),
    {ActiveOps, WaitingOps}.

complete_operation(Ops, CorrId, Res) ->
    case dict:find(CorrId, Ops) of
        {ok, {op, From, _Pid}} ->
            gen_fsm:reply(From, Res),
            dict:erase(CorrId, Ops);
        error ->
            io:format("No operation for completion with corrid ~p, res ~p, ops ~p~n", [CorrId, Res, Ops]),
            Ops
    end.

restart_operation(Ops, OldCorrId, NewCorrId) ->
    case dict:find(OldCorrId, Ops) of
        {ok, {op, From, Pid}} ->
            dict:store(NewCorrId, {op, From, Pid}, dict:erase(OldCorrId, Ops));
        error ->
            io:format("No operation for restart with oldcorrid ~p, newcorrid ~p, ops ~p~n", [OldCorrId, NewCorrId, Ops]),
            Ops
    end.
