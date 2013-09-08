-module(erlesque_conn).
-behavior(gen_server).

-export([start_link/2, connect/1, stop/1, send/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlesque_internal.hrl").

-record(state, {esq_pid,
                cur_ip,
                cur_port,
                socket=uninitialized,
                settings}).

start_link(EsqPid, Settings=#conn_settings{}) ->
    State = #state{esq_pid=EsqPid, settings=Settings},
    gen_server:start_link(?MODULE, State, []).

connect(Pid) ->
    gen_server:cast(Pid, connect).

stop(Pid) ->
    gen_server:call(Pid, stop).

send(Pid, Pkg) ->
    gen_server:cast(Pid, {send, Pkg}).


init(State=#state{}) ->
    {ok, State}.

handle_call(stop, _From, State=#state{esq_pid=EsqPid, socket=Socket}) ->
    ok = gen_tcp:close(Socket),
    EsqPid ! {closed, closed_by_client},
    {stop, normal, ok, State#state{socket=closed}};

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p~n State: ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast(connect, State=#state{esq_pid=EsqPid, settings=S}) ->
    {node, Ip, Port} = S#conn_settings.destination,
    case connect(EsqPid,
                 false,
                 S#conn_settings.max_reconns + 1,
                 S#conn_settings.conn_timeout,
                 S#conn_settings.reconn_delay,
                 Ip,
                 Port) of
        {ok, NewSocket} ->
            {noreply, State#state{socket=NewSocket, cur_ip=Ip, cur_port=Port}};
        {error, _Reason} ->
            {stop, normal, State#state{socket=closed, cur_ip=none, cur_port=none}}
    end;

handle_cast({send, Pkg}, State=#state{socket=Socket}) ->
    case Pkg of
        {pkg, heartbeat_resp, _, _, _} -> ok;
        _ -> io:format("Package sent: ~p~n", [Pkg])
    end,
    Bin = erlesque_pkg:to_binary(Pkg),
    PkgLen = byte_size(Bin),
    gen_tcp:send(Socket, <<PkgLen:32/unsigned-little-integer>>),
    gen_tcp:send(Socket, Bin),
    {noreply, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p~n State: ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({tcp, Socket, Data}, State=#state{esq_pid=EsqPid, socket=Socket}) ->
    Pkg = erlesque_pkg:from_binary(Data),
    EsqPid ! {package, Pkg},
    {noreply, State};

handle_info({tcp_closed, _Socket}, State=#state{esq_pid=EsqPid, settings=S}) ->
    io:format("Connection to [~p:~p] closed.~n", [State#state.cur_ip, State#state.cur_port]),
    EsqPid ! {disconnected, {State#state.cur_ip, State#state.cur_port}, tcp_closed},
    {node, Ip, Port} = S#conn_settings.destination,
    case connect(EsqPid,
                 true,
                 S#conn_settings.max_reconns + 1,
                 S#conn_settings.conn_timeout,
                 S#conn_settings.reconn_delay,
                 Ip,
                 Port) of
        {ok, NewSocket} ->
            {noreply, State#state{socket=NewSocket, cur_ip=Ip, cur_port=Port}};
        {error, _Reason} ->
            {stop, normal, State#state{socket=closed, cur_ip=none, cur_port=none}}
    end;

handle_info({tcp_error, _Socket, Reason}, State = #state{esq_pid=EsqPid}) ->
    EsqPid ! {closed, {tcp_error, Reason}},
    {stop, normal, State#state{socket=closed, cur_ip=none, cur_port=none}};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p~nState: ~p~n", [Msg, State]),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


connect(EsqPid, _IsReconnection, 0, _ConnTimeout, _ReconnDelay, _Ip, _Port) ->
    EsqPid ! {closed, reconnection_limit},
    {error, reconnection_limit};

connect(EsqPid, IsReconnection, AttemptsLeft, ConnTimeout, ReconnDelay, Ip, Port)
        when AttemptsLeft > 0 ->
    case IsReconnection of
        true ->
            timer:sleep(ReconnDelay),
            EsqPid ! {reconnecting, {Ip, Port}},
            io:format("Reconnecting to [~p:~p]...~n", [Ip, Port]);
        false ->
            ok
    end,
    %% ES prefix length is little-endian, but unfortunately Erlang supports only
    %% big-endianness so we have to do framing explicitly
    case gen_tcp:connect(Ip, Port, [binary, {active, false}], ConnTimeout) of
        {ok, Socket} ->
            io:format("Connection to [~p:~p] established.~n", [Ip, Port]),
            EsqPid ! {connected, {Ip, Port}},
            Pid = self(),
            spawn_link(fun() -> receive_loop(Pid, Socket) end),
            {ok, Socket};
        {error, Reason} ->
            io:format("Connection to [~p:~p] failed. Reason: ~p.~n", [Ip, Port, Reason]),
            connect(EsqPid, true, AttemptsLeft-1, ConnTimeout, ReconnDelay, Ip, Port)
    end.

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
