-module(erlesque_ops).
-export([start_link/4, restart/1, pause/1, abort/1, handle_pkg/2]).

start_link(ping, CorrId, ConnPid, Params) ->
    erlesque_op_ping:start_link(CorrId, ConnPid, Params);

start_link(write, CorrId, ConnPid, Params) ->
    erlesque_op_write:start_link(CorrId, ConnPid, Params);

start_link(Operation, CorrId, ConnPid, Params) ->
    io:format("Unsupported operation ~p with corrId ~p, conn_pid ~p and params ~p.~n", [Operation, CorrId, ConnPid, Params]),
    throw(not_supported).

restart(Pid) ->
    gen_server:cast(Pid, restart).

pause(Pid) ->
    gen_server:cast(Pid, pause).

abort(Pid) ->
    gen_server:cast(Pid, abort).

handle_pkg(Pid, Pkg) ->
    gen_server:cast(Pid, Pkg).
