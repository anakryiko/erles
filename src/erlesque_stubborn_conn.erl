-module(erlesque_stubborn_conn).
-behavior(gen_server).

-export([start_link/3, stop/1, send/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(settings, {ip,
                   port,
                   conn_timeout = 1000,
                   max_conn_attempts = 10,
                   reconn_delay = 500}).

-record(state, {es_pid,
                socket=uninitialized,
                settings}).

start_link(EsPid, Ip, Port) ->
    State = #state{es_pid=EsPid, settings=#settings{ip=Ip, port=Port}},
    gen_server:start_link(?MODULE, State, []).

stop(Pid) ->
    gen_server:call(Pid, stop).

send(Pid, Bin) ->
    gen_server:cast(Pid, {send, Bin}).


init(State) ->
    self() ! connect,
    {ok, State}.


handle_call(stop, _From, State = #state{es_pid=EsPid, socket=Socket}) ->
    ok = gen_tcp:close(Socket),
    EsPid ! {closed, client_requested},
    {stop, normal, ok, State#state{socket=closed}};

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast({send, Bin}, State = #state{socket=Socket}) ->
    PkgLen = byte_size(Bin),
    gen_tcp:send(Socket, <<PkgLen:32/unsigned-little-integer>>),
    gen_tcp:send(Socket, Bin),
    {noreply, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


handle_info(connect, State = #state{es_pid=EsPid, settings=S}) ->
    case connect(EsPid,
                 false,
                 S#settings.max_conn_attempts,
                 S#settings.conn_timeout,
                 S#settings.reconn_delay,
                 S#settings.ip,
                 S#settings.port) of
        {ok, NewSocket} -> {noreply, State#state{socket=NewSocket}};
        {error, Reason} -> {stop, Reason, State#state{socket=closed}}
    end;

handle_info({tcp, Socket, Data}, State = #state{es_pid=EsPid, socket=Socket}) ->
    EsPid ! {package, Data},
    {noreply, State};

handle_info({tcp_closed, _Socket}, State = #state{es_pid=EsPid, settings=S}) ->
    io:format("Connection to [~p:~p] closed.~n", [S#settings.ip, S#settings.port]),
    EsPid ! {disconnected, connection_closed},
    case connect(EsPid,
                 true,
                 S#settings.max_conn_attempts,
                 S#settings.conn_timeout,
                 S#settings.reconn_delay,
                 S#settings.ip,
                 S#settings.port) of
        {ok, NewSocket} -> {noreply, State#state{socket=NewSocket}};
        {error, Reason} -> {stop, Reason, State#state{socket=closed}}
    end;

handle_info({tcp_error, _Socket, Reason}, State = #state{es_pid=EsPid}) ->
    EsPid ! {closed, Reason},
    {stop, Reason, State#state{socket=closed}};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.

terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


connect(EsPid, _IsReconnection, 0, _ConnTimeout, _ReconnDelay, _Ip, _Port) ->
    EsPid ! {closed, reconn_limit},
    {error, reconn_limit};

connect(EsPid, IsReconnection, AttemptsLeft, ConnTimeout, ReconnDelay, Ip, Port)
        when AttemptsLeft > 0 ->
    case IsReconnection of
        true ->
            timer:sleep(ReconnDelay),
            EsPid ! reconnecting,
            io:format("Reconnecting to [~p:~p]...~n", [Ip, Port]);
        false ->
            ok
    end,
    %% ES prefix length is little-endian, but Erlang supports only
    %% big-endianness, unfortunately, so we have to do framing explicitly
    case gen_tcp:connect(Ip, Port, [binary, {active, false}], ConnTimeout) of
        {ok, Socket} ->
            io:format("Connection to [~p:~p] established.~n", [Ip, Port]),
            EsPid ! {connected, Ip, Port},
            Pid = self(),
            spawn_link(fun() -> receive_loop(Pid, Socket) end),
            {ok, Socket};
        {error, Reason} ->
            io:format("Connection to [~p:~p] failed. Reason: ~p.~n", [Ip, Port, Reason]),
            connect(EsPid, true, AttemptsLeft-1, ConnTimeout, ReconnDelay, Ip, Port)
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
