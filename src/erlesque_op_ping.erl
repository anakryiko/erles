-module(erlesque_op_ping).
-export([start_link/2]).

start_link(SysParams, OpParams) ->
    {ok, spawn_link(fun() -> ping(SysParams, OpParams, 5) end)}.

ping(SysParams={CorrId, ClientPid, ConnPid}, OpParams, TrialsLeft) ->
    OperationTimeout = 3000,
    erlesque_conn:send(ConnPid, erlesque_pkg:create(ping, CorrId, <<1,2,3>>)),
    receive
        {pkg, pong, CorrId, _Auth, _Data} ->
            ClientPid ! {op_completed, CorrId, pong};
        pause ->
            wait_for_connection(SysParams, OpParams, TrialsLeft-1);
        abort ->
            ClientPid ! {op_completed, CorrId, {error, aborted}};
        Msg ->
            io:format("Unexpected message received: ~p.~n", [Msg])
        after OperationTimeout ->
            ClientPid ! {op_completed, CorrId, {error, timedout}}
    end.

wait_for_connection(_SysParams={CorrId, ClientPid, _ConnPid}, _OpParams, 0) ->
    ClientPid ! {op_completed, CorrId, {error, retries_limit}};

wait_for_connection(SysParams={CorrId, ClientPid, _ConnPid}, OpParams, TrialsLeft) ->
    receive
        {pkg, pong, CorrId, _Auth, _Data} ->
            ClientPid ! {op_completed, CorrId, pong};
        restart ->
            ping(SysParams, OpParams, TrialsLeft);
        abort ->
            ClientPid ! {op_completed, CorrId, {error, aborted}};
        Msg ->
            io:format("Unexpected message received: ~p.~n", [Msg])
    end.
