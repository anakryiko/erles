-module(erlesque).

-export([connect/1, connect/2, close/1, start_link/1, start_link/2]).
-export([ping/1]).
-export([append/4, transaction_start/3, transaction_write/3, transaction_commit/2, delete/3]).
-export([read_event/3, read_event/4,
         read_stream_forward/4, read_stream_forward/5,
         read_stream_backward/4, read_stream_backward/5,
         read_all_forward/3, read_all_forward/4,
         read_all_backward/3, read_all_backward/4]).
-export([subscribe_to/4]).

-define(EXPECTED_VERSION_ANY, -2).
-define(REQUIRE_MASTER, true).
-define(LAST_EVENT_NUMBER, -1).
-define(LAST_LOG_POSITION, -1).

connect(Destination) -> start_link(Destination).

connect(Destination, Options) -> start_link(Destination, Options).

close(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, close).

start_link(Destination = {node, _Ip, _Port}) ->
    erlesque_fsm:start_link(Destination).

start_link(Destination = {node, _Ip, _Port}, Options) ->
    erlesque_fsm:start_link(Destination, Options).

ping(Pid) ->
    gen_fsm:sync_send_event(Pid, {op, temp, ping, []}).

append(Pid, StreamId, any, Events) ->
    append(Pid, StreamId, ?EXPECTED_VERSION_ANY, Events);
append(Pid, StreamId, ExpectedVersion, Events)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPECTED_VERSION_ANY,
             is_list(Events) ->
    gen_fsm:sync_send_event(Pid, {op, temp, write_events, {StreamId, ExpectedVersion, Events, ?REQUIRE_MASTER}}).

transaction_start(Pid, StreamId, any) ->
    transaction_start(Pid, StreamId, ?EXPECTED_VERSION_ANY);
transaction_start(Pid, StreamId, ExpectedVersion)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPECTED_VERSION_ANY ->
    gen_fsm:sync_send_event(Pid, {op, temp, transaction_start, {StreamId, ExpectedVersion, ?REQUIRE_MASTER}}).

transaction_write(Pid, TransactionId, Events)
        when is_integer(TransactionId), TransactionId >= 0,
             is_list(Events) ->
    gen_fsm:sync_send_event(Pid, {op, temp, transaction_write, {TransactionId, Events, ?REQUIRE_MASTER}}).

transaction_commit(Pid, TransactionId)
        when is_integer(TransactionId), TransactionId >= 0 ->
    gen_fsm:sync_send_event(Pid, {op, temp, transaction_commit, {TransactionId, ?REQUIRE_MASTER}}).

delete(Pid, StreamId, any) ->
    delete(Pid, StreamId, ?EXPECTED_VERSION_ANY);
delete(Pid, StreamId, ExpectedVersion)
        when is_integer(ExpectedVersion), ExpectedVersion >= ?EXPECTED_VERSION_ANY ->
    gen_fsm:sync_send_event(Pid, {op, temp, delete_stream, {StreamId, ExpectedVersion, ?REQUIRE_MASTER}}).

read_event(Pid, StreamId, last) ->
    read_event(Pid, StreamId, ?LAST_EVENT_NUMBER, false);
read_event(Pid, StreamId, EventNumber)
        when is_integer(EventNumber), EventNumber >= ?LAST_EVENT_NUMBER ->
    read_event(Pid, StreamId, EventNumber, false).
read_event(Pid, StreamId, last, ResolveLinks)
        when is_boolean(ResolveLinks) ->
    read_event(Pid, StreamId, ?LAST_EVENT_NUMBER, ResolveLinks);
read_event(Pid, StreamId, EventNumber, ResolveLinks)
        when is_integer(EventNumber), EventNumber >= ?LAST_EVENT_NUMBER,
             is_boolean(ResolveLinks) ->
    gen_fsm:sync_send_event(Pid, {op, temp, read_event, {StreamId, EventNumber, ResolveLinks, ?REQUIRE_MASTER}}).

read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount)
        when is_integer(FromEventNumber), FromEventNumber >= 0,
             is_integer(MaxCount), MaxCount > 0 ->
    read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount, false).
read_stream_forward(Pid, StreamId, FromEventNumber, MaxCount, ResolveLinks)
        when is_integer(FromEventNumber), FromEventNumber >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    gen_fsm:sync_send_event(Pid, {op, temp, read_stream_events_forward, {StreamId, FromEventNumber, MaxCount, ResolveLinks, ?REQUIRE_MASTER}}).

read_stream_backward(Pid, StreamId, last, MaxCount)
        when is_integer(MaxCount), MaxCount > 0 ->
    read_stream_backward(Pid, StreamId, ?LAST_EVENT_NUMBER, MaxCount, false);
read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount)
        when is_integer(FromEventNumber), FromEventNumber >= ?LAST_EVENT_NUMBER,
             is_integer(MaxCount), MaxCount > 0 ->
    read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount, false).
read_stream_backward(Pid, StreamId, last, MaxCount, ResolveLinks)
        when is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    read_stream_backward(Pid, StreamId, ?LAST_EVENT_NUMBER, MaxCount, ResolveLinks);
read_stream_backward(Pid, StreamId, FromEventNumber, MaxCount, ResolveLinks)
        when is_integer(FromEventNumber), FromEventNumber >= ?LAST_EVENT_NUMBER,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    gen_fsm:sync_send_event(Pid, {op, temp, read_stream_events_backward, {StreamId, FromEventNumber, MaxCount, ResolveLinks, ?REQUIRE_MASTER}}).

read_all_forward(Pid, first, MaxCount)
        when is_integer(MaxCount), MaxCount > 0 ->
    read_all_forward(Pid, {tfpos, 0, 0}, MaxCount, false);
read_all_forward(Pid, FromPos={tfpos, CommitPos, PreparePos}, MaxCount)
        when is_integer(CommitPos), CommitPos >= 0, is_integer(PreparePos), PreparePos >= 0,
             is_integer(MaxCount), MaxCount > 0 ->
    read_all_forward(Pid, FromPos, MaxCount, false).
read_all_forward(Pid, first, MaxCount, ResolveLinks)
        when is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    read_all_forward(Pid, {tfpos, 0, 0}, MaxCount, false);
read_all_forward(Pid, FromPos={tfpos, CommitPos, PreparePos}, MaxCount, ResolveLinks)
        when is_integer(CommitPos), CommitPos >= 0, is_integer(PreparePos), PreparePos >= 0,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    gen_fsm:sync_send_event(Pid, {op, temp, read_all_events_forward, {FromPos, MaxCount, ResolveLinks, ?REQUIRE_MASTER}}).

read_all_backward(Pid, last, MaxCount)
        when is_integer(MaxCount), MaxCount > 0 ->
    read_all_backward(Pid, {tfpos, ?LAST_LOG_POSITION, ?LAST_LOG_POSITION}, MaxCount, false);
read_all_backward(Pid, FromPos={tfpos, CommitPos, PreparePos}, MaxCount)
        when is_integer(CommitPos), CommitPos >= ?LAST_LOG_POSITION, is_integer(PreparePos), PreparePos >= ?LAST_LOG_POSITION,
             is_integer(MaxCount), MaxCount > 0 ->
    read_all_backward(Pid, FromPos, MaxCount, false).
read_all_backward(Pid, last, MaxCount, ResolveLinks)
        when is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    read_all_backward(Pid, {tfpos, ?LAST_LOG_POSITION, ?LAST_LOG_POSITION}, MaxCount, false);
read_all_backward(Pid, FromPos={tfpos, CommitPos, PreparePos}, MaxCount, ResolveLinks)
        when is_integer(CommitPos), CommitPos >= ?LAST_LOG_POSITION, is_integer(PreparePos), PreparePos >= ?LAST_LOG_POSITION,
             is_integer(MaxCount), MaxCount > 0,
             is_boolean(ResolveLinks) ->
    gen_fsm:sync_send_event(Pid, {op, temp, read_all_events_backward, {FromPos, MaxCount, ResolveLinks, ?REQUIRE_MASTER}}).

subscribe_to(Pid, StreamId, ResolveLinks, SubscriberPid) ->
    gen_fsm:sync_send_event(Pid, {op, perm, subscribe_to_stream, {StreamId, ResolveLinks, SubscriberPid}}).

