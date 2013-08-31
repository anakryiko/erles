-module(erlesque).

-export([connect/1, close/1, start_link/1]).
-export([ping/1, append/4]).

-define(ExpectedVersionAny, -2).

connect(ConnSettings) -> start_link(ConnSettings).

close(Pid) ->
    gen_fsm:send_all_state_sync_event(Pid, close).

start_link(ConnSettings = {node, _Ip, _Port}) ->
    erlesque_fsm:start_link(ConnSettings);

start_link(ConnSettings = {cluster, _DnsEntry, _ManagerPort}) ->
    erlesque_fsm:start_link(ConnSettings).

ping(Pid) ->
    gen_fsm:sync_send_event(Pid, {op, ping, []}).

append(Pid, StreamId, any, Events) ->
    append(Pid, StreamId, ?ExpectedVersionAny, Events);
append(Pid, StreamId, ExpectedVersion, Events)
    when is_integer(ExpectedVersion),
         ExpectedVersion >= ?ExpectedVersionAny,
         is_list(Events) ->
    gen_fsm:sync_send_event(Pid, {op, write_events, {StreamId, ExpectedVersion, Events, true}}).
