-module(erlesque_chat_server).
-behavior(gen_server).

-export([start_link/1, broadcast/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {clients=[]}).

start_link(Port) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Port, []).

broadcast(Text) ->
    gen_server:cast(?MODULE, {broadcast, Text}).


init(Port) ->
    {ok, ListenSocket} = gen_tcp:listen(Port, [{active, true}, binary, {packet, 4}]),
    ServerPid = self(),
    spawn_link(fun() -> accept_connection(ListenSocket, ServerPid) end),
    {ok, #state{}}.

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast({new_connection, Socket}, State = #state{clients = Clients}) ->
    {noreply, State#state{clients = [{Socket, "unknown"} | Clients]}};

handle_cast({broadcast, Text}, State = #state{clients = Clients}) ->
    lists:foreach(fun({S,_N}) ->
        Msg = {message, "BROADCAST", Text},
        gen_tcp:send(S, term_to_binary(Msg))
    end, Clients),
    {noreply, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({tcp, Socket, Message}, State) ->
    Msg = binary_to_term(Message),
    NewState = handle_tcp_message(Socket, Msg, State),
    {noreply, NewState};

handle_info({tcp_closed, Socket}, State = #state{clients = Clients}) ->
    io:format("Connection ~p closed!~n", [Socket]),
    NewClients = case lists:filter(fun({S,_N}) -> S =:= Socket end, Clients) of
        [{_LeftSocket,LeftName}] ->
            NC = lists:filter(fun({S,_N}) -> S =/= Socket end, Clients),
            lists:foreach(fun({S,_N}) ->
                Msg = {message, "BROADCAST", io_lib:format("User ~s has left!", [LeftName])},
                gen_tcp:send(S, term_to_binary(Msg))
            end, NC),
            NC;
        [] ->
            Clients
    end,
    {noreply, State#state{clients=NewClients}};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_tcp_message(Socket, {enter, NewName}, State = #state{clients = Clients}) ->
    NewClients = lists:map(fun({S, N}) ->
        case S =:= Socket of
            true -> {S, NewName};
            false -> {S, N}
        end
    end, Clients),
    lists:foreach(fun({S,_N}) ->
        gen_tcp:send(S, term_to_binary({enter, NewName}))
    end, NewClients),
    State#state{clients = NewClients};

handle_tcp_message(_Socket, {message, Name, Text}, State = #state{clients = Clients}) ->
    lists:map(fun({S, _N}) ->
        gen_tcp:send(S, term_to_binary({message, Name, Text}))
    end, Clients),
    State.


accept_connection(ListenSocket, ServerPid) ->
    io:format("Accepting connection...~n", []),
    {ok, Socket} = gen_tcp:accept(ListenSocket),
    gen_tcp:controlling_process(Socket, ServerPid),
    io:format("Connection ~p accepted!~n", [Socket]),
    gen_server:cast(?MODULE, {new_connection, Socket}),
    accept_connection(ListenSocket, ServerPid).




