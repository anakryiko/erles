-module(erlesque_chat_client).
-behavior(gen_server).

-export([start_link/2, message/2, exit/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

start_link(Name, Port) ->
    gen_server:start_link(?MODULE, {Name, Port}, []).

message(Pid, Text) ->
    gen_server:cast(Pid, {message, Text}).

exit(Pid) ->
    gen_server:cast(Pid, exit).


init({Name, Port}) ->
    {ok, Socket} = gen_tcp:connect({127,0,0,1}, Port, [{active, true}, binary, {packet, 4}]),
    gen_tcp:send(Socket, term_to_binary({enter, Name})),
    {ok, {Name, Socket}}.

handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p, state ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast({message, Text}, {Name, Socket}) ->
    gen_tcp:send(Socket, term_to_binary({message, Name, Text})),
    {noreply, {Name, Socket}};

handle_cast(exit, {Name, Socket}) ->
    gen_tcp:close(Socket),
    {stop, normal, {Name, Socket}};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({tcp, _Socket, Message}, State) ->
    Msg = binary_to_term(Message),
    handle_tcp_message(Msg),
    {noreply, State};

handle_info({tcp_closed, _Socket}, State) ->
    io:format("Connection to server unexpectedly closed!!!~n", []),
    {stop, normal, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p, state ~p~n", [Msg, State]),
    {noreply, State}.


terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


handle_tcp_message({enter, NewName}) ->
    io:format("[~s] entered chat.~n", [NewName]);

handle_tcp_message({message, Name, Text}) ->
    io:format("[~s]: ~s.~n", [Name, Text]).

