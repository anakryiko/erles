-module(erlesque_perm_sub).
-behavior(gen_server).

-export([start_link/3, stop/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-include("erlesque_internal.hrl").

-record(state, {esq_pid,
                sub_pid,
                sub_mon_ref}).

start_link(A, B, Options) where is_list(Options) ->
    gen_server:start_link(?MODULE, {A, B, Options}, []).

stop(Pid) ->
    gen_server:cast(Pid, stop).


init({A, B, Options}) ->
    process_flag(trap_exit, true),
    MonRef = erlang:monitor(process, SubPid),
    {ok, State#state{sub_mon_ref=MonRef}}.


handle_call(Msg, From, State) ->
    io:format("Unexpected CALL message ~p from ~p~n State: ~p~n", [Msg, From, State]),
    {noreply, State}.


handle_cast({start, FromPos}, State) ->
    start_from(FromPos),
    {noreply, State#state{worker_pid=WorkerPid}};

handle_cast(stop, State) ->
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    State#state.sub_pid ! {stopped, self(), requested_by_client},
    State#state.worker_pid ! stop,
    {stop, normal, State};

handle_cast({worker_stopped, Reason}, State) ->
    erlang:demonitor(State#state.sub_mon_ref, [flush]),
    State#state.sub_pid ! {stopped, self(), Reason},
    {stop, normal, State};

handle_cast(Msg, State) ->
    io:format("Unexpected CAST message ~p~n State: ~p~n", [Msg, State]),
    {noreply, State}.


handle_info({'DOWN', MonRef, process, SubPid, Reason}, StateName, State=#state{sub_pid=SubPid, sub_mon_ref=MonRef}) ->
    io:format("Stopping permanent subscription due to subscriber is DOWN. Reason: ~p.~n", [Reason]),
    State#state.worker_pid ! stop,
    {stop, normal, State};

handle_info(Msg, State) ->
    io:format("Unexpected INFO message ~p~nState: ~p~n", [Msg, State]),
    {noreply, State}.


terminate(normal, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

-record(worker, {perm_sub_pid,
                 esq_pid,
                 subscriber_pid,
                 max_count,
                 opts}).

catchup(State, Pos={_PosKind, StreamPos}) ->
    receive stop -> ok
    after 0 ->
        EsqPid = State#worker.esq_pid,
        StreamId = State#worker.stream_id,
        MaxCount = State#worker.max_count,
        Opts = State#worker.opts,
        case erlesque:read_stream_forward(EsqPid, StreamId, StreamPos, MaxCount, Opts) of
            {ok, Events, NextPos, false} ->
                send_events(State#worker.subscriber_pid, Events, Pos),
                catchup(State, {inclusive, NextPos});
            {ok, Events, NextPos, true} ->
                send_events(State#worker.subscriber_pid, Events, Pos),
                subscribe(State, {inclusive, NextPos});
            {error, Reason} ->
                stop(State#worker.perm_sub_pid, {read_failed, Reason})
        end
    end.

subscribe(State, Pos) ->
    receive stop -> ok
    after 0 ->
        EsqPid = State#worker.esq_pid,
        StreamId = State#worker.stream_id,
        Opts = State#worker.opts,
        case erlesque:subscribe(EsqPid, StreamId, Opts) of
            {ok, SubPid, _SubPos} ->
                switchover(State, Pos, SubPid);
            {error, Reason} ->
                stop(State#worker.perm_sub_pid, {subscription_failed, Reason})
        end
    end.

switchover(State, Pos={_PosKind, StreamPos}, SubscriptionPid) ->
    receive stop -> erlesque:unsubscribe(SubscriptionPid)
    after 0 ->
        EsqPid = State#worker.esq_pid,
        StreamId = State#worker.stream_id,
        MaxCount = State#worker.max_count,
        Opts = State#worker.opts,
        case erlesque:read_stream_forward(EsqPid, StreamId, StreamPos, MaxCount, Opts) of
            {ok, Events, NextPos, false} ->
                send_events(State#worker.subscriber_pid, Events, Pos),
                switchover(State, {inclusive, NextPos}, SubscriptionPid);
            {ok, Events, NextPos, true} ->
                send_events(State#worker.subscriber_pid, Events, Pos),
                live(State, {inclusive, NextPos}, SubscriptionPid);
            {error, Reason} ->
                erlesque:unsubscribe(SubscriptionPid),
                stop(State#worker.perm_sub_pid, {read_failed, Reason})
        end
    end.

live(State, Pos, SubscriptionPid) ->
    receive
        stop ->
            erlesque:unsubscribe(SubscriptionPid);
        {unsubscribed, {aborted, _}} ->
            stop(State#worker.perm_sub_pid, subscription_aborted);
        {unsubscribed, _Reason} ->
            catchup(State, Pos);
        {event, Event, NewPos} ->
            send_event(State#worker.subscriber_pid, Event, NewPos, Pos),
            live({exclusive, NewPos});
        Unexpected ->
            io:format("Unexpected event in live phase of perm_subscr: ~p.~n", [Unexpected])
    end.

stop(PermSubPid, Reason) -> PermSubPid ! {worker_stopped, self(), Reason}.

send_events(Pid, Events, Pos) when is_list(Events) ->
    lists:foreach(fun({event, E, P}) -> send_event(Pid, E, P, Pos) end, Events).

send_event(Pid, Event, EventPos, {inclusive, SubPos}) when EventPos >= SubPos ->
    Pid ! {event, Event, EventPos};

send_event(Pid, Event, EventPos, {exclusive, SubPos}) when EventPos > SubPos ->
    Pid ! {event, Event, EventPos};

send_event(_Pid, _Event, _EventPos, {_PosKind, SubPos}) ->
    ok.

