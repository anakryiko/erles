-module(erles_fsm).
-behavior(gen_fsm).

-export([start_link/1, start_link/2]).
-export([operation_completed/2, operation_restarted/3]).

-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, code_change/4, terminate/3]).
-export([connecting/2, connecting/3, connected/2, connected/3]).

-include("erles_internal.hrl").

-record(state, {conn_pid,
                sup_pid,
                waiting_ops = queue:new(),
                active_ops = dict:new(),
                max_server_ops,
                op_timeout,
                op_retries,
                retry_delay,
                default_auth}).

-record(act_op, {type, pid}).
-record(wait_op, {type, from, op, auth, params}).


start_link(Destination) ->
    gen_fsm:start_link(?MODULE, {Destination, []}, []).

start_link(Destination, Options) ->
    gen_fsm:start_link(?MODULE, {Destination, Options}, []).

operation_completed(Pid, CorrId) ->
    gen_fsm:send_all_state_event(Pid, {op_completed, CorrId}).

operation_restarted(Pid, OldCorrId, NewCorrId) ->
    gen_fsm:send_all_state_event(Pid, {op_restarted, OldCorrId, NewCorrId}).


init({Destination, Options}) ->
    %% OPERATIONS SETTINGS
    MaxServerOps = get_opt(Options, max_server_ops,    ?DEF_MAX_SERVER_OPS, positive),
    OpTimeout    = get_opt(Options, operation_timeout, ?DEF_OPERATION_TIMEOUT, positive),
    OpRetries    = get_opt(Options, operation_retries, ?DEF_OPERATION_RETRIES, non_neg),
    RetryDelay   = get_opt(Options, retry_delay,       ?DEF_RETRY_DELAY, non_neg),
    DefaultAuth  = case proplists:lookup(default_auth, Options) of
        none -> ?DEF_DEFAULT_AUTH;
        {_, {Login, Pass}} -> {Login, Pass};
        {_, noauth} -> noauth
    end,
    {ok, SupPid} = erles_ops_sup:start_link(),
    ConnSettings = get_connection_settings(Options),
    {ok, ConnPid} = erles_conn:start_link(self(), Destination, ConnSettings),
    erles_conn:connect(ConnPid),
    {ok, connecting, #state{sup_pid=SupPid,
                            conn_pid=ConnPid,
                            max_server_ops=MaxServerOps,
                            op_timeout=OpTimeout,
                            op_retries=OpRetries,
                            retry_delay=RetryDelay,
                            default_auth=DefaultAuth}}.

connecting({op, {Operation, Type, Auth}, Params}, From, State=#state{waiting_ops=WaitingOps}) ->
    WaitOp = #wait_op{type=Type, from=From, op=Operation, auth=Auth, params=Params},
    NewWaitingOps = queue:in(WaitOp, WaitingOps),
    {next_state, connecting, State#state{waiting_ops=NewWaitingOps}};

connecting(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, connecting, State]),
    {next_state, connecting, State}.

connecting(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, connecting, State]),
    {next_state, connecting, State}.


connected({op, {Operation, Type, Auth}, Params}, From, State) ->
    WaitOp = #wait_op{type=Type, from=From, op=Operation, auth=Auth, params=Params},
    NewState = case State#state.max_server_ops =:= 0 of
        true ->
            State#state{waiting_ops=queue:in(WaitOp, State#state.waiting_ops)};
        false ->
            start_operation(WaitOp, State)
    end,
    {next_state, connected, NewState};

connected(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, connected, State]),
    {next_state, connected, State}.

connected(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p~nState data ~p~n", [Msg, connected, State]),
    {next_state, connected, State}.


handle_sync_event(close, _From, _StateName, State=#state{}) ->
    ok = erles_conn:stop(State#state.conn_pid),
    io:format("Connection stopped. Reason: ~p.~n", [closed_by_client]),
    {NewActOps, NewWaitOps} = abort_operations(State#state.active_ops,
                                               State#state.waiting_ops,
                                               closed_by_client),
    {stop, normal, ok, State#state{active_ops=NewActOps, waiting_ops=NewWaitOps}};

handle_sync_event(Event, From, StateName, State) ->
    io:format("Unexpected GLOBAL SYNC EVENT ~p from ~p in state ~p, state data ~p~n", [Event, From, StateName, State]),
    {next_state, StateName, State}.


handle_event({op_completed, CorrId}, StateName, State=#state{active_ops=Ops}) ->
    NewState = case dict:find(CorrId, Ops) of
        {ok, #act_op{type=temp}} ->
            State2 = State#state{active_ops=dict:erase(CorrId, Ops),
                                 max_server_ops=State#state.max_server_ops+1},
            start_waiting_operations(State2);
        {ok, #act_op{type=perm}} ->
            State#state{active_ops=dict:erase(CorrId, Ops)};
        error ->
            io:format("No operation for completion with corrid ~p, ops ~p~n", [CorrId, Ops]),
            State
    end,
    %io:format("Op_completed: corrid ~p~n", [CorrId]),
    {next_state, StateName, NewState};

handle_event({op_restarted, OldCorrId, NewCorrId}, StateName, State=#state{active_ops=Ops}) ->
    NewOps = case dict:find(OldCorrId, Ops) of
        {ok, ActOp=#act_op{}} ->
            dict:store(NewCorrId, ActOp, dict:erase(OldCorrId, Ops));
        error ->
            io:format("No operation for restart with oldcorrid ~p, newcorrid ~p, ops ~p~n", [OldCorrId, NewCorrId, Ops]),
            Ops
    end,
    io:format("Op_restarted: oldcorrid ~p, newcorrid ~p~n", [OldCorrId, NewCorrId]),
    {next_state, StateName, State#state{active_ops=NewOps}};

handle_event(Event, StateName, State) ->
    io:format("Unexpected GLOBAL ASYNC EVENT  ~p in state ~p, state data ~p~n", [Event, StateName, State]),
    {next_state, StateName, State}.


handle_info({package, Pkg}, StateName, State) ->
    NewState = handle_pkg(State, Pkg),
    {next_state, StateName, NewState};

handle_info({connected, {_Ip, _Port}}, connecting, State=#state{}) ->
    Restart = fun(_, #act_op{pid=Pid}, _) -> erles_ops:connected(Pid) end,
    dict:fold(Restart, ok, State#state.active_ops),
    NewState = start_waiting_operations(State),
    {next_state, connected, NewState};

handle_info({disconnected, {_Ip, _Port}, _Reason}, connected, State=#state{active_ops=Ops}) ->
    Pause = fun(_, #act_op{pid=Pid}, _) -> erles_ops:disconnected(Pid) end,
    dict:fold(Pause, ok, Ops),
    {next_state, connecting, State};

handle_info({closed, Reason}, _StateName, State=#state{active_ops=ActiveOps, waiting_ops=WaitingOps}) ->
    io:format("Connection stopped. Reason: ~p.~n", [Reason]),
    {NewActiveOps, NewWaitingOps} = abort_operations(ActiveOps, WaitingOps, Reason),
    {stop, normal, State#state{active_ops=NewActiveOps, waiting_ops=NewWaitingOps}};

handle_info(Msg, StateName, State) ->
    io:format("Unexpected INFO message ~p, state name ~p, state data ~p~n", [Msg, StateName, State]),
    {next_state, StateName, State}.


terminate(normal, _StateName, _State) ->
    io:format("erles connection terminated!~n"),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.


handle_pkg(State=#state{active_ops=Ops}, Pkg={pkg, _Cmd, CorrId, _Auth, _Data}) ->
    %io:format("Package arrived: ~p~n", [Pkg]),
    case dict:find(CorrId, Ops) of
        {ok, #act_op{pid=Pid}} ->
            erles_ops:handle_pkg(Pid, Pkg);
        error ->
            io:format("No operation with corrid ~p,~npkg ~p~nOps: ~p~n", [CorrId, Pkg, Ops]),
            ok
    end,
    State.

start_waiting_operations(State=#state{max_server_ops=0}) ->
    State;

start_waiting_operations(State=#state{})
        when State#state.max_server_ops >= 0 ->
    case queue:out(State#state.waiting_ops) of
        {{value, Op=#wait_op{}}, NewWaitingOps} ->
            NewState = start_operation(Op, State#state{waiting_ops=NewWaitingOps}),
            start_waiting_operations(NewState);
        {empty, _} ->
            State
    end.

start_operation(Op=#wait_op{}, State=#state{}) ->
    CorrId = erles_utils:gen_uuid(),
    Auth = case Op#wait_op.auth of
        defauth -> State#state.default_auth;
        Other -> Other
    end,
    SysParams = #sys_params{corr_id=CorrId,
                            esq_pid=self(),
                            conn_pid=State#state.conn_pid,
                            reply_pid=Op#wait_op.from,
                            op_timeout=State#state.op_timeout,
                            op_retries=State#state.op_retries,
                            retry_delay=State#state.retry_delay,
                            auth=Auth},
    {ok, Pid} = erles_ops_sup:start_operation(State#state.sup_pid,
                                                 Op#wait_op.op,
                                                 SysParams,
                                                 Op#wait_op.params),
    ActOp = #act_op{type=Op#wait_op.type, pid=Pid},
    erles_ops:connected(Pid),
    ActiveOps = dict:store(CorrId, ActOp, State#state.active_ops),
    MaxServerOps = case Op#wait_op.type of
        temp -> State#state.max_server_ops - 1;
        perm -> State#state.max_server_ops
    end,
    %io:format("New op started CorrId: ~p~nOp: ~p~nSysparams: ~p~n", [CorrId, Op, SysParams]),
    State#state{active_ops=ActiveOps, max_server_ops=MaxServerOps}.

abort_operations(ActiveOps, WaitingOps, Reason) ->
    Abort = fun(_, #act_op{pid=Pid}, _) -> erles_ops:aborted(Pid, Reason) end,
    ReplyAbort = fun(#wait_op{from=From}, _) -> gen_fsm:reply(From, {error, Reason}) end,
    dict:fold(Abort, ok, ActiveOps),
    lists:foldl(ReplyAbort, ok, queue:to_list(WaitingOps)),
    {ActiveOps, WaitingOps}.

get_connection_settings(Options) ->
    %% CONNECTION SETTINGS
    ConnTimeout        = get_opt(Options, connection_timeout,   ?DEF_CONNECTION_TIMEOUT, positive),
    ReconnDelay        = get_opt(Options, reconnection_delay,   ?DEF_RECONNECTION_DELAY, non_neg),
    MaxConnRetries     = get_opt(Options, max_conn_retries,     ?DEF_MAX_CONNECTION_RETRIES, non_neg),
    HeartbeatPeriod    = get_opt(Options, heartbeat_period,     ?DEF_HEARTBEAT_PERIOD, positive),
    HeartbeatTimeout   = get_opt(Options, heartbeat_timeout,    ?DEF_HEARTBEAT_TIMEOUT, positive),
    DnsTimeout         = get_opt(Options, dns_timeout,          ?DEF_DNS_TIMEOUT, positive),
    GossipTimeout      = get_opt(Options, gossip_timeout,       ?DEF_GOSSIP_TIMEOUT, positive),
    DiscoverDelay      = get_opt(Options, discover_delay,       ?DEF_DISCOVER_DELAY, non_neg),
    MaxDiscoverRetries = get_opt(Options, max_discover_retries, ?DEF_MAX_DISCOVER_RETRIES, non_neg),
    #conn_settings{conn_timeout=ConnTimeout,
                   reconn_delay=ReconnDelay,
                   max_conn_retries=MaxConnRetries,
                   heartbeat_period=HeartbeatPeriod,
                   heartbeat_timeout=HeartbeatTimeout,
                   dns_timeout=DnsTimeout,
                   gossip_timeout=GossipTimeout,
                   discover_delay=DiscoverDelay,
                   max_discover_retries=MaxDiscoverRetries}.

get_opt(Options, Key, Default, positive) ->
    case proplists:lookup(Key, Options) of
        none -> Default;
        {_, V} when is_integer(V), V > 0 -> V
    end;

get_opt(Options, Key, Default, non_neg) ->
    case proplists:lookup(Key, Options) of
        none -> Default;
        {_, V} when is_integer(V), V >= 0 -> V
    end.
