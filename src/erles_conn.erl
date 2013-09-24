-module(erles_conn).
-behavior(gen_server).

-export([start_link/3, connect/1, reconnect/3, stop/1, send/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erles_internal.hrl").

-record(state, {esq_pid,
                destination=unknown,
                socket=none,
                cur_endpoint=none,
                settings}).

-record(node, {state, tcp_ip, tcp_port, http_ip, http_port}).

start_link(EsqPid, Destination, Settings=#conn_settings{}) ->
    State = #state{esq_pid=EsqPid, destination=Destination, settings=Settings},
    gen_server:start_link(?MODULE, State, []).

connect(Pid) ->
    gen_server:cast(Pid, connect).

reconnect(Pid, Ip, Port) ->
    gen_server:call(Pid, {reconnect, Ip, Port}).

stop(Pid) ->
    gen_server:call(Pid, stop).

send(Pid, Pkg) ->
    gen_server:cast(Pid, {send, Pkg}).


init(State=#state{}) ->
    {ok, State}.

handle_call(stop, _From, State) ->
    case State#state.socket of
        none ->
            ok;
        _ ->
            ok = gen_tcp:close(State#state.socket),
            State#state.esq_pid ! {disconnected, State#state.cur_endpoint, conn_closed}
    end,
    State#state.esq_pid ! {closed, closed_by_client},
    {stop, normal, ok, State};


handle_call({reconnect, Ip, Port}, _From, State=#state{cur_endpoint={Ip, Port}}) ->
    {reply, already_connected, State};

handle_call({reconnect, Ip, Port}, _From, State) ->
    case State#state.socket of
        none ->
            ok;
        Socket ->
            io:format("Reconnecting from ~p to ~p.~n", [State#state.cur_endpoint, {Ip, Port}]),
            ok = gen_tcp:close(Socket),
            State#state.esq_pid ! {disconnected, State#state.cur_endpoint, conn_switch}
    end,
    case connect(switch, State, {node, Ip, Port}, none) of
        {ok, NewState} ->
            State#state.esq_pid ! {connected, NewState#state.cur_endpoint},
            {reply, ok, NewState};
        {error, Reason} ->
            State#state.esq_pid ! {closed, Reason},
            {stop, normal, ok, State}
    end;

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p~n State: ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast(connect, State) ->
    io:format("Connection to ~p requested.~n", [State#state.destination]),
    case connect(connect, State, State#state.destination, none) of
        {ok, NewState} ->
            State#state.esq_pid ! {connected, NewState#state.cur_endpoint},
            {noreply, NewState};
        {error, Reason} ->
            State#state.esq_pid ! {closed, Reason},
            {stop, normal, State}
    end;

handle_cast({send, Pkg}, State) ->
    case Pkg of
        {pkg, heartbeat_resp, _, _, _} -> ok;
        _ -> io:format("Package sent: ~p~n", [Pkg])
    end,
    send_pkg(State#state.socket, Pkg),
    {noreply, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p~n State: ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({tcp, Socket, Data}, State=#state{socket=Socket}) ->
    Pkg = erles_pkg:from_binary(Data),
    case Pkg of
        {pkg, heartbeat_req, CorrId, _Auth, PkgData} ->
            send_pkg(Socket, erles_pkg:create(heartbeat_resp, CorrId, PkgData));
        _Other ->
            io:format("Package received: ~p~n", [Pkg]),
            State#state.esq_pid ! {package, Pkg}
    end,
    {noreply, State};

handle_info({tcp, _Socket, _Data}, State) ->
    {noreply, State};

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    io:format("Connection to ~p closed.~n", [State#state.cur_endpoint]),
    State#state.esq_pid ! {disconnected, State#state.cur_endpoint, tcp_closed},
    case connect(reconnect, State, State#state.destination, State#state.cur_endpoint) of
        {ok, NewState} ->
            State#state.esq_pid ! {connected, NewState#state.cur_endpoint},
            {noreply, NewState};
        {error, Reason} ->
            State#state.esq_pid ! {closed, Reason},
            {stop, normal, State}
    end;

handle_info({tcp_closed, _Socket}, State) ->
    {noreply, State};

handle_info({tcp_error, Socket, Reason}, State = #state{socket=Socket}) ->
    State#state.esq_pid ! {closed, {tcp_error, Reason}},
    {stop, normal, State};

handle_info({tcp_error, _Socket, _Reason}, State) ->
    {noreply, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p~nState: ~p~n", [Msg, State]),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


connect(ConnType, State, {dns, ClusterDns, ManagerPort}, none) ->
    io:format("Resolving DNS entry '~p', manager port ~p.~n", [ClusterDns, ManagerPort]),
    DnsTimeout = State#state.settings#conn_settings.dns_timeout,
    case inet:getaddrs(ClusterDns, inet, DnsTimeout) of
        {ok, []} ->
            io:format("DNS lookup of '~p' returned empty list of addresses.~n", [ClusterDns]),
            {error, {dns_lookup_failed, empty_addr_list}};
        {ok, Addrs} ->
            io:format("DNS entry '~p' resolved into ~p.~n", [ClusterDns, Addrs]),
            SeedNodes = [{Ip, ManagerPort} || Ip <- Addrs],
            connect(ConnType, State, {cluster, SeedNodes}, none);
        {error, Reason} ->
            io:format("DNS lookup of '~p' failed, reason: ~p.~n", [ClusterDns, Reason]),
            {error, {dns_lookup_failed, Reason}}
    end;

connect(ConnType, State, {cluster, SeedNodes}, none) ->
    io:format("Connecting to cluster of nodes ~p.~n", [SeedNodes]),
    Nodes = [#node{state=manager,
                   tcp_ip=none,
                   tcp_port=none,
                   http_ip=Ip,
                   http_port=Port} || {Ip, Port} <- SeedNodes],
    connect(ConnType, State, {gossip, Nodes}, none);

connect(ConnType, State, {gossip, Nodes}, FailedEndpoint) ->
    io:format("Connecting to cluster of gossip nodes ~p.~n", [Nodes]),
    S = State#state.settings,
    Attempts = S#conn_settings.max_discover_retries + 1,
    case discover_best_endpoint(Attempts, S, Nodes, FailedEndpoint) of
        {ok, {Ip, Port}, Gossip} ->
            NewState = State#state{destination={gossip, Gossip}},
            connect(ConnType, NewState, {node, Ip, Port}, none);
        {error, Reason} ->
            {error, Reason}
    end;

connect(ConnType, State, {node, Ip, Port}, _FailedEndpoint) ->
    S = State#state.settings,
    Attempts = S#conn_settings.max_conn_retries + 1,
    case connect_direct(ConnType, Attempts, S, Ip, Port) of
        {ok, {Socket, _WorkerPid, Ip, Port}} ->
            {ok, State#state{socket=Socket, cur_endpoint={Ip, Port}}};
        {error, Reason} ->
            {error, Reason}
    end.


connect_direct(_ConnType, 0, _S, _Ip, _Port) ->
    {error, reconnection_limit};

connect_direct(ConnType, AttemptsLeft, S, Ip, Port)
        when AttemptsLeft > 0 ->
    case ConnType of
        connect ->
            io:format("Connecting to [~p:~p]...~n", [Ip, Port]);
        switch ->
            io:format("Switching to [~p:~p]...~n", [Ip, Port]);
        reconnect ->
            timer:sleep(S#conn_settings.reconn_delay),
            io:format("Reconnecting to [~p:~p]...~n", [Ip, Port])
    end,
    %% ES prefix length is little-endian, but unfortunately Erlang supports only
    %% big-endianness so we have to do framing explicitly
    case gen_tcp:connect(Ip, Port, [binary, {active, false}], S#conn_settings.conn_timeout) of
        {ok, Socket} ->
            io:format("Connection to [~p:~p] established.~n", [Ip, Port]),
            Pid = self(),
            WorkerPid = spawn_link(fun() -> receive_loop(Pid, Socket) end),
            {ok, {Socket, WorkerPid, Ip, Port}};
        {error, Reason} ->
            io:format("Connection to [~p:~p] failed. Reason: ~p.~n", [Ip, Port, Reason]),
            connect_direct(reconnect, AttemptsLeft-1, S, Ip, Port)
    end.

send_pkg(Socket, Pkg) ->
    Bin = erles_pkg:to_binary(Pkg),
    PkgLen = byte_size(Bin),
    gen_tcp:send(Socket, <<PkgLen:32/unsigned-little-integer>>),
    gen_tcp:send(Socket, Bin).

receive_loop(Pid, Socket) ->
    case gen_tcp:recv(Socket, 4) of
        {ok, L} ->
          <<Len:32/unsigned-little-integer>> = L,
          case gen_tcp:recv(Socket, Len) of
              {ok, Data} ->
                  Pid ! {tcp, Socket, Data},
                  receive_loop(Pid, Socket);
              {error, closed} ->
                  Pid ! {tcp_closed, Socket},
                  ok
          end;
        {error, closed} ->
            Pid ! {tcp_closed, Socket},
            ok
    end.


discover_best_endpoint(0, _S, _SeedNodes, _Failed) ->
    {error, endpoint_discovery_failed};

discover_best_endpoint(AttemptsLeft, S, SeedNodes, {Ip, Port}) when AttemptsLeft > 0 ->
    NotFailedPred = fun(N) -> N#node.tcp_ip =/= Ip orelse N#node.tcp_port =/= Port end,
    NotFailedNodes = lists:filter(NotFailedPred, SeedNodes),
    discover_best_endpoint(AttemptsLeft, S, NotFailedNodes, none);

discover_best_endpoint(AttemptsLeft, S, SeedNodes, none) when AttemptsLeft > 0 ->
    Seeds = arrange_gossip_candidates(SeedNodes),
    case get_gossip(Seeds, S#conn_settings.gossip_timeout) of
        {ok, Winner, Members} ->
            {ok, Winner, Members};
        {error, _Reason} ->
            case AttemptsLeft > 1 of
                true -> timer:sleep(S#conn_settings.discover_delay);
                false -> ok
            end,
            discover_best_endpoint(AttemptsLeft-1, S, Seeds, none)
    end.

arrange_gossip_candidates(Candidates) when is_list(Candidates) ->
    {Managers, Nodes} = lists:partition(fun(N) -> N#node.state =:= manager end, Candidates),
    All = erles_utils:shuffle(Nodes) ++ erles_utils:shuffle(Managers),
    [{N#node.http_ip, N#node.http_port} || N <- All].

get_gossip([], _Timeout) -> {error, no_more_seeds};

get_gossip([{Ip, Port} | SeedNodesTail], Timeout) ->
    case get_gossip(Ip, Port, Timeout) of
        {ok, []}         -> get_gossip(SeedNodesTail, Timeout);
        {ok, Members}    ->
            io:format("Members: ~p~n", [Members]),
            Ranked = lists:sort(fun(X, Y) -> rank_state(X#node.state) =< rank_state(Y#node.state) end,
                                [M || M <- Members, rank_state(M#node.state) > 0]),
            io:format("Ranked: ~p~n", [Ranked]),
            case Ranked of
                []           -> get_gossip(SeedNodesTail, Timeout);
                [N | _Other] -> {ok, {N#node.tcp_ip, N#node.tcp_port}, Members}
            end;
        {error, _Reason} -> get_gossip(SeedNodesTail, Timeout)
    end.


get_gossip(_Ip={I1, I2, I3, I4}, Port, Timeout) ->
    Url = lists:flatten(io_lib:format("http://~B.~B.~B.~B:~B/gossip?format=json", [I1, I2, I3, I4, Port])),
    case httpc:request(get, {Url, []}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Version, 200, _StatusDesc}, _Headers, Data}} ->
            Json = jsx:decode(Data),
            JsonMembers = proplists:get_value(<<"members">>, Json),
            AllMembers = [get_member(M) || M <- JsonMembers],
            {ok, [Node || {Alive, Node} <- AllMembers, Alive]};
        {ok, {{_Version, StatusCode, StatusDesc}, _Headers, _Data}} ->
            {error, {http_error, StatusCode, StatusDesc}};
        {error, Reason} ->
            {error, Reason}
    end.


get_member(Member) ->
    State = node_state(proplists:get_value(<<"state">>, Member)),
    IsAlive = proplists:get_value(<<"isAlive">>, Member),
    TcpIp = erles_utils:parse_ip(proplists:get_value(<<"externalTcpIp">>, Member)),
    TcpPort = proplists:get_value(<<"externalTcpPort">>, Member),
    HttpIp = erles_utils:parse_ip(proplists:get_value(<<"externalHttpIp">>, Member)),
    HttpPort = proplists:get_value(<<"externalHttpPort">>, Member),
    {IsAlive, #node{state=State,
                    tcp_ip=TcpIp,
                    tcp_port=TcpPort,
                    http_ip=HttpIp,
                    http_port=HttpPort}}.


node_state(<<"Initializing">>)   -> initializing;
node_state(<<"Unknown">>)        -> unknown;
node_state(<<"PreReplica">>)     -> prereplica;
node_state(<<"CatchingUp">>)     -> catchingup;
node_state(<<"Clone">>)          -> clone;
node_state(<<"Slave">>)          -> slave;
node_state(<<"PreMaster">>)      -> premaster;
node_state(<<"Master">>)         -> master;
node_state(<<"Manager">>)        -> manager;
node_state(<<"ShuttingDown">>)   -> shuttingdown;
node_state(<<"Shutdown">>)       -> shutdown;
node_state(_Unrecognized)         -> unrecognized.


rank_state(master)        -> 1;
rank_state(premaster)     -> 2;
rank_state(slave)         -> 3;
rank_state(clone)         -> 4;
rank_state(catchingup)    -> 5;
rank_state(prereplica)    -> 6;
rank_state(unknown)       -> 7;
rank_state(initializing)  -> 8;

rank_state(manager)       -> 0;
rank_state(shuttingdown)  -> 0;
rank_state(shutdown)      -> 0;
rank_state(unrecognized)  -> 0.

