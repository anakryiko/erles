-module(erlesque_conn).
-behavior(gen_server).

-export([start_link/2, connect/1, reconnect/3, stop/1, send/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlesque_internal.hrl").

-record(state, {esq_pid,
                socket=none,
                cur_ip=none,
                cur_port=none,
                settings}).

start_link(EsqPid, Settings=#conn_settings{}) ->
    State = #state{esq_pid=EsqPid, settings=Settings},
    gen_server:start_link(?MODULE, State, []).

connect(Pid) ->
    gen_server:cast(Pid, connect).

reconnect(Pid, Ip, Port) ->
    gen_server:cast(Pid, {reconnect, Ip, Port}).

stop(Pid) ->
    gen_server:call(Pid, stop).

send(Pid, Pkg) ->
    gen_server:cast(Pid, {send, Pkg}).


init(State=#state{}) ->
    {ok, State}.

handle_call(stop, _From, State) ->
    ok = gen_tcp:close(State#state.socket),
    State#state.esq_pid ! {closed, closed_by_client},
    {stop, normal, ok, State#state{socket=none, cur_ip=none, cur_port=none}};

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p~n State: ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast(connect, State=#state{settings=S}) ->
    io:format("Connection to ~p requested.~n", [S#conn_settings.destination]),
    case connect(State#state.esq_pid, connect, S, S#conn_settings.destination) of
        {ok, {NewSocket, _SockPid, CurIp, CurPort}} ->
            {noreply, State#state{socket=NewSocket, cur_ip=CurIp, cur_port=CurPort}};
        {error, _Reason} ->
            {stop, normal, State#state{socket=none, cur_ip=none, cur_port=none}}
    end;

handle_cast({reconnect, Ip, Port}, State=#state{cur_ip=Ip, cur_port=Port}) ->
    {noreply, State};

handle_cast({reconnect, Ip, Port}, State=#state{settings=S}) ->
    io:format("Reconnection to ~p requested.~n", [{node, Ip, Port}]),
    ok = gen_tcp:close(State#state.socket),
    case connect(State#state.esq_pid, reconnect, S, {node, Ip, Port}) of
        {ok, {NewSocket, _SockPid, CurIp, CurPort}} ->
            {noreply, State#state{socket=NewSocket, cur_ip=CurIp, cur_port=CurPort}};
        {error, _Reason} ->
            {stop, normal, State#state{socket=none, cur_ip=none, cur_port=none}}
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
    Pkg = erlesque_pkg:from_binary(Data),
    case Pkg of
        {pkg, heartbeat_req, CorrId, _Auth, Data} ->
            send_pkg(Socket, erlesque_pkg:create(heartbeat_resp, CorrId, Data));
        _Other ->
            State#state.esq_pid ! {package, Pkg}
    end,
    {noreply, State};

handle_info({tcp, _Socket, _Data}, State) ->
    {noreply, State};

handle_info({tcp_closed, Socket}, State=#state{socket=Socket, settings=S}) ->
    io:format("Connection to [~p:~p] closed.~n", [State#state.cur_ip, State#state.cur_port]),
    State#state.esq_pid ! {disconnected, {State#state.cur_ip, State#state.cur_port}, tcp_closed},
    case connect(State#state.esq_pid, reconnect, S, S#conn_settings.destination) of
        {ok, {NewSocket, _SockPid, CurIp, CurPort}} ->
            {noreply, State#state{socket=NewSocket, cur_ip=CurIp, cur_port=CurPort}};
        {error, _Reason} ->
            {stop, normal, State#state{socket=none, cur_ip=none, cur_port=none}}
    end;

handle_info({tcp_closed, _Socket}, State) ->
    {noreply, State};

handle_info({tcp_error, Socket, Reason}, State = #state{socket=Socket}) ->
    State#state.esq_pid ! {closed, {tcp_error, Reason}},
    {stop, normal, State#state{socket=none, cur_ip=none, cur_port=none}};

handle_info({tcp_error, _Socket, _Reason}, State) ->
    {noreply, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p~nState: ~p~n", [Msg, State]),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


connect(_EsqPid, _ConnType, _S=#conn_settings{}, {clusterdns, _ClusterDns, _ManagerPort}) ->
    throw(not_implemented);
connect(_EsqPid, _ConnType, _S=#conn_settings{}, {cluster, _SeedEndpoints}) ->
    throw(not_implemented);
connect(EsqPid, ConnType, S=#conn_settings{}, {node, Ip, Port}) ->
    connect_direct(EsqPid, ConnType, S#conn_settings.max_reconns + 1, S, Ip, Port).

connect_direct(EsqPid, _ConnType, 0, _S, _Ip, _Port) ->
    EsqPid ! {closed, reconnection_limit},
    {error, reconnection_limit};

connect_direct(EsqPid, ConnType, AttemptsLeft, S, Ip, Port)
        when AttemptsLeft > 0 ->
    case ConnType of
        connect ->
            io:format("Connecting to [~p:~p]...~n", [Ip, Port]);
        reconnect ->
            io:format("Reconnecting to [~p:~p]...~n", [Ip, Port]);
        retry ->
            timer:sleep(S#conn_settings.reconn_delay),
            io:format("Reconnecting to [~p:~p]...~n", [Ip, Port])
    end,
    %% ES prefix length is little-endian, but unfortunately Erlang supports only
    %% big-endianness so we have to do framing explicitly
    case gen_tcp:connect(Ip, Port, [binary, {active, false}], S#conn_settings.conn_timeout) of
        {ok, Socket} ->
            io:format("Connection to [~p:~p] established.~n", [Ip, Port]),
            EsqPid ! {connected, {Ip, Port}},
            Pid = self(),
            WorkerPid = spawn_link(fun() -> receive_loop(Pid, Socket) end),
            {ok, {Socket, WorkerPid, Ip, Port}};
        {error, Reason} ->
            io:format("Connection to [~p:~p] failed. Reason: ~p.~n", [Ip, Port, Reason]),
            connect_direct(EsqPid, retry, AttemptsLeft-1, S, Ip, Port)
    end.

send_pkg(Socket, Pkg) ->
    Bin = erlesque_pkg:to_binary(Pkg),
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
