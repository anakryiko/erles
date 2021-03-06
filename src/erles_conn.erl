-module(erles_conn).
-behavior(gen_server).

-export([start_link/3, connect/1, reconnect/3, stop/1, send/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erles_internal.hrl").

-record(state, {els_pid,
                destination=unknown,
                socket=none,
                cur_endpoint=none,
                settings,
                timer_ref=none,
                msg_num=0}).

-record(node, {state, tcp_ip, tcp_port, http_ip, http_port}).

start_link(ElsPid, Destination, Settings=#conn_settings{}) ->
    State = #state{els_pid=ElsPid, destination=Destination, settings=Settings},
    gen_server:start_link(?MODULE, State, []).

connect(Pid) ->
    gen_server:cast(Pid, connect).

reconnect(Pid, Ip, Port) ->
    gen_server:call(Pid, {reconnect, Ip, Port}).

stop(Pid) ->
    gen_server:call(Pid, stop).

send(Pid, Pkg) ->
    gen_server:cast(Pid, {send, Pkg}).

%% INTERNALS
init(State=#state{}) ->
    {ok, State}.

handle_call(stop, _From, State) ->
    close_tcp(State, conn_closed),
    {stop, normal, ok, State};


handle_call({reconnect, Ip, Port}, _From, State=#state{cur_endpoint={Ip, Port}}) ->
    {reply, already_connected, State};

handle_call({reconnect, Ip, Port}, From, State) ->
    close_tcp(State, connection_switch),
    gen_server:reply(From, ok),
    establish_connection(State, switch, {node, Ip, Port}, none);

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p~n State: ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast(connect, State) ->
    establish_connection(State, connect, State#state.destination, none);

handle_cast({send, Pkg}, State) ->
    case Pkg of
        {pkg, heartbeat_resp, _, _, _} -> ok;
        _ -> ok %io:format("Package sent: ~p~n", [Pkg])
    end,
    send_pkg(State#state.socket, Pkg),
    {noreply, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p~n State: ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({timeout, TimerRef, {heartbeat, MsgNum}}, State=#state{timer_ref=TimerRef}) ->
    NewState = case State#state.msg_num > MsgNum of
        true  -> start_heartbeat(State);
        false -> send_heartbeat(State)
    end,
    {noreply, NewState};

handle_info({timeout, TimerRef, {heartbeat_timeout, MsgNum}}, State=#state{timer_ref=TimerRef}) ->
    case State#state.msg_num > MsgNum of
        true ->
            NewState = start_heartbeat(State),
            {noreply, NewState};
        false ->
            close_tcp(State, heartbeat_timeout),
            establish_connection(State, reconnect, State#state.destination, State#state.cur_endpoint)
    end;

handle_info({timeout, _, _}, State) ->
    {noreply, State};

handle_info({tcp, Socket, Data}, State=#state{socket=Socket, msg_num=MsgNum}) ->
    Pkg = erles_pkg:from_binary(Data),
    case Pkg of
        {pkg, heartbeat_req, CorrId, _Auth, PkgData} ->
            send_pkg(Socket, erles_pkg:create(heartbeat_resp, CorrId, PkgData));
        {pkg, heartbeat_resp, _CorrId, _Auth, _PkgData} ->
            ok;
        _Other ->
            %io:format("Package received: ~p~n", [Pkg]),
            State#state.els_pid ! {package, Pkg}
    end,
    {noreply, State#state{msg_num=MsgNum+1}};

handle_info({tcp, _Socket, _Data}, State) ->
    {noreply, State};

handle_info({tcp_closed, Socket}, State=#state{socket=Socket}) ->
    close_tcp(State, tcp_closed),
    establish_connection(State, reconnect, State#state.destination, State#state.cur_endpoint);

handle_info({tcp_closed, _Socket}, State) ->
    {noreply, State};

handle_info({tcp_error, Socket, Reason}, State = #state{socket=Socket}) ->
    State#state.els_pid ! {closed, {tcp_error, Reason}},
    {stop, {tcp_error, Reason}, State};

handle_info({tcp_error, _Socket, _Reason}, State) ->
    {noreply, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p~nState: ~p~n", [Msg, State]),
    {noreply, State}.


terminate(_Reason, _State) ->
    %io:format("erles_conn terminated: ~p~n", [_Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


close_tcp(#state{socket=none}, _Reason) ->
    ok;

close_tcp(State, Reason) ->
    %io:format("Connection to ~p closed: ~p.~n", [State#state.cur_endpoint, Reason]),
    ok = gen_tcp:close(State#state.socket),
    State#state.els_pid ! {disconnected, State#state.cur_endpoint, Reason}.

start_heartbeat(State=#state{settings=S}) ->
    Timeout = S#conn_settings.heartbeat_period,
    TimerRef = erlang:start_timer(Timeout, self(), {heartbeat, State#state.msg_num}),
    State#state{timer_ref=TimerRef}.

send_heartbeat(State=#state{settings=S}) ->
    Pkg = erles_pkg:create(heartbeat_req, erles_utils:gen_uuid(), noauth, <<>>),
    send_pkg(State#state.socket, Pkg),
    Timeout = S#conn_settings.heartbeat_timeout,
    TimerRef = erlang:start_timer(Timeout, self(), {heartbeat_timeout, State#state.msg_num}),
    State#state{timer_ref=TimerRef}.

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


establish_connection(State, ConnType, Destination, FailedEndpoint) ->
    %io:format("Establishing connection to ~p.~n", [Destination]),
    S = State#state.settings,
    Attempts = S#conn_settings.max_conn_retries + 1,
    case connect(State, ConnType, Destination, FailedEndpoint, Attempts) of
        {ok, NewState} ->
            NewState#state.els_pid ! {connected, NewState#state.cur_endpoint},
            NewState2 = start_heartbeat(NewState),
            {noreply, NewState2};
        {error, Reason} ->
            State#state.els_pid ! {closed, {connect_failed, Reason}},
            {stop, {connect_failed, Reason}, State}
    end.


connect(_State, _ConnType, _Destination, _FailedEndpoint, 0) ->
    {error, reconnection_limit};

connect(State, ConnType, {dns, ClusterDns, ManagerPort}, none, Attempts)
        when is_binary(ClusterDns) ->
    connect(State, ConnType, {dns, binary_to_list(ClusterDns), ManagerPort}, none, Attempts);

connect(State, ConnType, {dns, ClusterDns, ManagerPort}, none, Attempts) ->
    %io:format("Connecting to ~p.~n", [{dns, ClusterDns, ManagerPort}]),
    DnsTimeout = State#state.settings#conn_settings.dns_timeout,
    case inet:getaddrs(ClusterDns, inet, DnsTimeout) of
        {ok, []} ->
            %io:format("DNS lookup of '~p' returned empty list of addresses.~n", [ClusterDns]),
            {error, {dns_lookup_failed, empty_addr_list}};
        {ok, Addrs} ->
            %io:format("DNS entry '~p' resolved into ~p.~n", [ClusterDns, Addrs]),
            SeedNodes = [{Ip, ManagerPort} || Ip <- Addrs],
            connect(State, ConnType, {cluster, SeedNodes}, none, Attempts);
        {error, Reason} ->
            %io:format("DNS lookup of '~p' failed, reason: ~p.~n", [ClusterDns, Reason]),
            {error, {dns_lookup_failed, Reason}}
    end;

connect(State, ConnType, {cluster, SeedNodes}, none, Attempts) ->
    %io:format("Connecting to ~p.~n", [{cluster, SeedNodes}]),
    Nodes = [#node{state=manager,
                   tcp_ip=none,
                   tcp_port=none,
                   http_ip=Ip,
                   http_port=Port} || {Ip, Port} <- SeedNodes],
    NewState = State#state{destination={gossip, Nodes}},
    connect(NewState, ConnType, {gossip, Nodes}, none, Attempts);

connect(State, ConnType, Dest={gossip, Nodes}, FailedEndpoint, Attempts) ->
    delay_connect_if_necessary(State#state.settings, ConnType, Dest),
    case discover_best_endpoint(State#state.settings, Nodes, FailedEndpoint) of
        {ok, {Ip, Port}, Gossip} ->
            %io:format("connect discovered best node ~p.~n", [{Ip, Port}]),
            NewState = State#state{destination={gossip, Gossip}},
            connect(NewState, ConnType, {node, Ip, Port}, none, Attempts);
        {error, _Reason} ->
            %io:format("connect discovering endpoint FAILED: ~p.~n", [_Reason]),
            connect(State, reconnect, State#state.destination, none, Attempts-1)
    end;

connect(State, ConnType, Dest={node, Ip, Port}, _FailedEndpoint, Attempts) ->
    S = State#state.settings,
    delay_connect_if_necessary(S, ConnType, Dest),
    %% ES prefix length is little-endian, but unfortunately Erlang supports only
    %% big-endianness so we have to do framing explicitly
    case gen_tcp:connect(Ip, Port, [binary, {active, false}], S#conn_settings.conn_timeout) of
        {ok, Socket} ->
            %io:format("Connection to [~p:~p] established.~n", [Ip, Port]),
            Pid = self(),
            _WorkerPid = spawn_link(fun() -> receive_loop(Pid, Socket) end),
            {ok, State#state{socket=Socket, cur_endpoint={Ip, Port}}};
        {error, _Reason} ->
            %io:format("Connection to [~p:~p] failed. Reason: ~p.~n", [Ip, Port, _Reason]),
            connect(State, reconnect, State#state.destination, {Ip, Port}, Attempts-1)
    end.


delay_connect_if_necessary(S, ConnType, _Destination) ->
    case ConnType of
        connect ->
            ok; %io:format("Connecting to ~p.~n", [Destination]);
        switch ->
            ok; %io:format("Switching to ~p.~n", [Destination]);
        reconnect ->
            timer:sleep(S#conn_settings.reconn_delay)
            %io:format("Reconnecting to ~p.~n", [Destination])
    end.


discover_best_endpoint(S, SeedNodes, {Ip, Port}) ->
    NotFailedPred = fun(N) -> N#node.tcp_ip =/= Ip orelse N#node.tcp_port =/= Port end,
    NotFailedNodes = lists:filter(NotFailedPred, SeedNodes),
    %io:format("discover_best_endpoint NotFailedNodes:~n~p~n", [NotFailedNodes]),
    discover_best_endpoint(S, NotFailedNodes, none);

discover_best_endpoint(S, SeedNodes, none) ->
    Seeds = arrange_gossip_candidates(SeedNodes),
    %io:format("discover_best_endpoint ArrangedSeeds:~n~p~n", [Seeds]),
    case get_gossip(Seeds, S#conn_settings.gossip_timeout) of
        {ok, Winner, Members} -> {ok, Winner, Members};
        {error, Reason}       -> {error, Reason}
    end.


arrange_gossip_candidates(Candidates) when is_list(Candidates) ->
    {Nodes, Managers} = lists:partition(fun(N) -> N#node.state =/= manager end, Candidates),
    All = erles_utils:shuffle(Nodes) ++ erles_utils:shuffle(Managers),
    [{N#node.http_ip, N#node.http_port} || N <- All].


get_gossip([], _Timeout) -> {error, no_more_seeds};

get_gossip([{Ip, Port} | SeedNodesTail], Timeout) ->
    case get_gossip(Ip, Port, Timeout) of
        {ok, []}         ->
            %io:format("get_gossip Members: EMPTY~n"),
            get_gossip(SeedNodesTail, Timeout);
        {ok, Members}    ->
            %io:format("get_gossip Members: ~p~n", [Members]),
            Ranked = lists:sort(fun(X, Y) -> rank_state(X#node.state) =< rank_state(Y#node.state) end,
                                [M || M <- Members, rank_state(M#node.state) > 0]),
            %io:format("get_gossip Ranked: ~p~n", [Ranked]),
            case Ranked of
                []           -> get_gossip(SeedNodesTail, Timeout);
                [N | _Other] -> {ok, {N#node.tcp_ip, N#node.tcp_port}, Members}
            end;
        {error, _Reason} ->
            %io:format("get_gossip FAILED: ~p~n", [_Reason]),
            get_gossip(SeedNodesTail, Timeout)
    end.


get_gossip(Ip, Port, Timeout) ->
    Url = lists:flatten(io_lib:format("http://~s:~B/gossip?format=json", [inet_parse:ntoa(Ip), Port])),
    %io:format("Getting gossip from ~p~n", [Url]),
    case httpc:request(get, {Url, []}, [{timeout, Timeout}], [{body_format, binary}]) of
        {ok, {{_Version, 200, _StatusDesc}, _Headers, Data}} ->
            Json = jsx:decode(Data),
            JsonMembers = proplists:get_value(<<"members">>, Json),
            AllMembers = [get_member(M) || M <- JsonMembers],
            %io:format("got_gossip ~p~n", [AllMembers]),
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

