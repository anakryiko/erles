-module(erles_ops).
-export([start_link/3, connected/1, disconnected/1, aborted/2, handle_pkg/2]).

start_link(ReqCmd=ping, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=write_events, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=transaction_start, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=transaction_write, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=transaction_commit, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=delete_stream, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=read_event, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=read_stream_events_forward, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=read_stream_events_backward, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=read_all_events_forward, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=read_all_events_backward, SysParams, OpParams) ->
    gen_fsm:start_link(erles_reqs, {ReqCmd, SysParams, OpParams}, []);
start_link(ReqCmd=subscribe_to_stream, SysParams, OpParams) ->
    gen_fsm:start_link(erles_subs, {ReqCmd, SysParams, OpParams}, []).

connected(Pid) ->
    gen_fsm:send_event(Pid, connected).

disconnected(Pid) ->
    gen_fsm:send_event(Pid, disconnected).

aborted(Pid, Reason) ->
    gen_fsm:send_event(Pid, {aborted, Reason}).

handle_pkg(Pid, Pkg) ->
    gen_fsm:send_event(Pid, Pkg).
