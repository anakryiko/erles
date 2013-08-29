-module(erlesque_fsm).
-behavior(gen_fsm).

-export([start_link/1, stop/1]).
-export([ping/1]).

-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, code_change/4, terminate/3]).
-export([connecting/2, connecting/3, connected/2, connected/3]).

-record(state, {conn_pid,
                ops_sup_pid,
                waiting_ops = queue:new(),
                active_ops = dict:new()}).

start_link(ConnSettings = {node, _Ip, _Port}) ->
    gen_fsm:start_link(?MODULE, ConnSettings, []);

start_link(ConnSettings = {cluster, _DnsEntry, _ManagerPort}) ->
    gen_fsm:start_link(?MODULE, ConnSettings, []).

stop(Pid) ->
    gen_fsm:send_all_state_sync_event(Pid, close).

ping(Pid) ->
    gen_fsm:sync_send_event(Pid, {op, ping, []}, infinity).


init(ConnSettings) ->
    {ok, SupPid} = erlesque_ops_sup:start_link(),
    {ok, ConnPid} = erlesque_conn:start_link(self(), ConnSettings),
    {ok, connecting, #state{conn_pid=ConnPid, ops_sup_pid=SupPid}}.


connecting({op, Operation, Params}, From, StateData=#state{waiting_ops=WaitingOps}) ->
    NewWaitingOps = queue:in({op, From, Operation, Params}, WaitingOps),
    {next_state, connecting, StateData#state{waiting_ops=NewWaitingOps}};

connecting(Msg, From, StateData) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, connecting, StateData]),
    {next_state, connecting, StateData}.

connecting(Msg, StateData) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, connecting, StateData]),
    {next_state, connecting, StateData}.


connected({op, Operation, Params}, From, StateData=#state{ops_sup_pid=SupPid, conn_pid=ConnPid, active_ops=ActiveOps}) ->
    CorrId = erlesque_utils:create_uuid_v4(),
    {ok, Pid} = erlesque_ops_sup:start_operation(SupPid, Operation, CorrId, ConnPid, Params),
    NewActiveOps = dict:store(CorrId, {op, From, Pid}, ActiveOps),
    {next_state, connecting, StateData#state{active_ops=NewActiveOps}};

connected(Msg, From, StateData) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, connected, StateData]),
    {next_state, connected, StateData}.

connected(Msg, StateData) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, connected, StateData]),
    {next_state, connected, StateData}.


handle_sync_event(close, _From, _StateName, StateData=#state{conn_pid=ConnPid, active_ops=ActiveOps, waiting_ops=WaitingOps}) ->
    ok = erlesque_conn:stop(ConnPid),
    io:format("Connection stopped. Reason: ~p.~n", [closed_by_client]),
    {NewActOps, NewWaitOps} = abort_operations(ActiveOps, WaitingOps, closed_by_client),
    {stop, normal, ok, StateData#state{active_ops=NewActOps, waiting_ops=NewWaitOps}};

handle_sync_event(Event, From, StateName, StateData) ->
    io:format("Unexpected GLOBAL SYNC EVENT ~p from ~p in state ~p, state data ~p~n", [Event, From, StateName, StateData]),
    {next_state, StateName, StateData}.


handle_event(Event, StateName, StateData) ->
    io:format("Unexpected GLOBAL ASYNC EVENT  ~p in state ~p, state data ~p~n",
              [Event, StateName, StateData]),
    {next_state, StateName, StateData}.



handle_info({package, Data}, StateName, StateData) ->
    Pkg = erlesque_pkg:from_binary(Data),
    NewStateData = handle_pkg(StateData, Pkg),
    {next_state, StateName, NewStateData};

handle_info({connected, _Ip, _Port}, connecting, StateData = #state{active_ops=Ops}) ->
    NewOps = restart_operations(Ops),
    {next_state, connected, StateData#state{active_ops=NewOps}};

handle_info({disconnected, _Reason}, connected, StateData = #state{active_ops=Ops}) ->
    NewOps = pause_operations(Ops),
    {next_state, connecting, StateData#state{active_ops=NewOps}};

handle_info(reconnecting, connecting, StateData) ->
    {next_state, connecting, StateData};

handle_info({closed, Reason}, _StateName, StateData=#state{active_ops=ActiveOps, waiting_ops=WaitingOps}) ->
    io:format("Connection stopped. Reason: ~p.~n", [Reason]),
    {NewActiveOps, NewWaitingOps} = abort_operations(ActiveOps, WaitingOps, Reason),
    {stop, normal, ok, StateData#state{active_ops=NewActiveOps, waiting_ops=NewWaitingOps}};


handle_info({op_completed, CorrId, Res}, StateName, StateData=#state{active_ops=Ops}) ->
    complete_operation(Ops, CorrId, Res),
    {next_state, StateName, StateData};

handle_info({op_restarted, OldCorrId, NewCorrId}, StateName, StateData=#state{active_ops=Ops}) ->
    restart_operation(Ops, OldCorrId, NewCorrId),
    {next_state, StateName, StateData};


handle_info(Msg, StateName, StateData) ->
    io:format("Unexpected INFO message ~p, state name ~p, state data ~p~n", [Msg, StateName, StateData]),
    {next_state, StateName, StateData}.


terminate(normal, _StateName, _StateData) ->
    ok.

code_change(_OldVsn, StateName, StateData, _Extra) ->
    {ok, StateName, StateData}.


handle_pkg(State=#state{conn_pid=ConnPid}, {pkg, heartbeat_req, CorrId, _Auth, Data}) ->
    Pkg = erlesque_pkg:create(heartbeat_resp, CorrId, Data}),
    erlesque_conn:send(ConnPid, PkgBin),
    State;

handle_pkg(State=#state{active_ops=Ops}, Pkg={pkg, _Cmd, CorrId, _Auth, _Data}) ->
    case dict:find(CorrId, Ops) of
        {ok, {op, _From, Pid}} -> erlesque_ops:handle_pkg(Pid, Pkg);
        error -> ok
    end,
    State.


restart_operations(Ops) ->
    Restart = fun(_, {op, _From, Pid}, _) -> erlesque_ops:restart(Pid) end,
    dict:fold(Restart, ok, Ops),
    Ops.

pause_operations(Ops) ->
    Pause = fun(_, {op, _From, Pid}, _) -> erlesque_ops:pause(Pid) end,
    dict:fold(Pause, ok, Ops),
    Ops.

abort_operations(ActiveOps, WaitingOps, Reason) ->
    dict:fold(fun(_, {op, From, Pid}, _) ->
        erlesque_ops:abort(Pid),
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
            Ops
    end.

restart_operation(Ops, OldCorrId, NewCorrId) ->
    case dict:find(OldCorrId, Ops) of
        {ok, {op, From, Pid}} ->
            dict:store(NewCorrId, {op, From, Pid}, dict:erase(OldCorrId, Ops));
        error ->
            Ops
    end.
