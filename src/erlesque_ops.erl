-module(erlesque_ops).
-export([start_link/3, restart/1, pause/1, abort/1, handle_pkg/2]).

start_link(ping, SysParams, OpParams) ->
    erlesque_op_ping:start_link(SysParams, OpParams);

start_link(write, SysParams, OpParams) ->
    erlesque_op_base:start_link(write_events, SysParams, OpParams);

start_link(Operation, SysParams, OpParams) ->
    io:format("Unsupported operation ~p with sys_params ~p and op_params ~p.~n", [Operation, SysParams, OpParams]),
    throw(not_supported).

restart(Pid) ->
    Pid ! restart.

pause(Pid) ->
    Pid ! pause.

abort(Pid) ->
    Pid ! abort.

handle_pkg(Pid, Pkg) ->
    Pid ! Pkg.
