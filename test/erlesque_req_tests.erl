-module(erlesque_req_tests).
-include_lib("eunit/include/eunit.hrl").
-include("erlesque.hrl").

-compile(export_all).

reqs_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     fun(D) ->
        %{inparallel,
            [append_any(D),
             append_expver(D),
             append_empty(D),

             delete_expver(D),
             delete_any(D),

             transaction_expver(D),
             transaction_any(D),
             transaction_empty(D),

             reads(D),
             subscriptions(D)
            ]%}
     end}.

setup() ->
    %% here we assume that node on default IP and port is already started
    {ok, C} = erlesque:connect(node, {{127,0,0,1}, 1113}),
    C.

teardown(C) ->
    ok = erlesque:close(C).

append_any(C) ->
    Stream = gen_stream_id(),
    [?_assertEqual({ok, 0}, erlesque:append(C, Stream, any, [create_event()])),
     ?_assertEqual({ok, 1}, erlesque:append(C, Stream, -2, [create_event()]))
    ].

append_expver(C) ->
    Stream = gen_stream_id(),
    [?_assertEqual({ok, 0}, erlesque:append(C, Stream, -1, [create_event()])),
     ?_assertEqual({ok, 1}, erlesque:append(C, Stream,  0, [create_event()])),
     ?_assertEqual({error, wrong_expected_version},
                   erlesque:append(C, Stream, -1, [create_event()]))
    ].

append_empty(C) ->
    Stream = gen_stream_id(),
    [?_assertEqual({ok, -1}, erlesque:append(C, Stream, any, [])),
     ?_assertEqual({ok, -1}, erlesque:append(C, Stream, -1, []))
    ].


delete_any(C) ->
    Stream = gen_stream_id(),
    {ok, _NextExpVer} = erlesque:append(C, Stream, any, [create_event(), create_event()]),
    [?_assertEqual(ok, erlesque:delete(C, Stream, any)),
     ?_assertEqual({error, stream_deleted}, erlesque:read_event(C, Stream, last))
    ].

delete_expver(C) ->
    Stream = gen_stream_id(),
    {ok, NextExpVer} = erlesque:append(C, Stream, any, [create_event(), create_event()]),
    [?_assertEqual(ok, erlesque:delete(C, Stream, NextExpVer)),
     ?_assertEqual({error, stream_deleted}, erlesque:read_event(C, Stream, last))
    ].

transaction_expver(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erlesque:transaction_start(C, Stream, -1),
    [?_assertEqual(ok, erlesque:transaction_write(C, Tid, [create_event(), create_event()])),
     ?_assertEqual(ok, erlesque:transaction_write(C, Tid, [create_event()])),
     ?_assertEqual(ok, erlesque:transaction_write(C, Tid, [])),
     ?_assertEqual({ok, 2}, erlesque:transaction_commit(C, Tid))
    ].

transaction_any(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erlesque:transaction_start(C, Stream, any),
    [?_assertEqual(ok, erlesque:transaction_write(C, Tid, [create_event(), create_event()])),
     ?_assertEqual(ok, erlesque:transaction_write(C, Tid, [create_event()])),
     ?_assertEqual(ok, erlesque:transaction_write(C, Tid, [])),
     ?_assertEqual({ok, 2}, erlesque:transaction_commit(C, Tid))
    ].

transaction_empty(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erlesque:transaction_start(C, Stream, any),
    ?_assertEqual({ok, -1}, erlesque:transaction_commit(C, Tid)).

reads(C) ->
    S = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    RE1 = map_event(S, 0, E1),
    RE2 = map_event(S, 1, E2),
    RE3 = map_event(S, 2, E3),
    {ok, 2} = erlesque:append(C, S, -1, [E1, E2, E3]),
    {inparallel, [
     ?_assertEqual({ok, RE1}, erlesque:read_event(C, S, 0)),
     ?_assertEqual({ok, RE2}, erlesque:read_event(C, S, 1)),
     ?_assertEqual({ok, RE3}, erlesque:read_event(C, S, 2)),
     ?_assertEqual({ok, RE3}, erlesque:read_event(C, S, last)),
     ?_assertEqual({ok, RE3}, erlesque:read_event(C, S, -1)),

     ?_assertEqual({ok, {[RE1], 1, false}},           erlesque:read_stream_forward(C, S, 0, 1)),
     ?_assertEqual({ok, {[RE2], 2, false}},           erlesque:read_stream_forward(C, S, 1, 1)),
     ?_assertEqual({ok, {[RE3], 3, true}},            erlesque:read_stream_forward(C, S, 2, 1)),
     ?_assertEqual({ok, {[], 3, true}},               erlesque:read_stream_forward(C, S, 3, 1)),
     ?_assertEqual({ok, {[RE1, RE2], 2, false}},      erlesque:read_stream_forward(C, S, 0, 2)),
     ?_assertEqual({ok, {[RE2, RE3], 3, true}},       erlesque:read_stream_forward(C, S, 1, 2)),
     ?_assertEqual({ok, {[RE3], 3, true}},            erlesque:read_stream_forward(C, S, 2, 2)),
     ?_assertEqual({ok, {[], 3, true}},               erlesque:read_stream_forward(C, S, 3, 2)),

     ?_assertEqual({ok, {[RE1], -1, true}},           erlesque:read_stream_backward(C, S, 0, 1)),
     ?_assertEqual({ok, {[RE2], 0, false}},           erlesque:read_stream_backward(C, S, 1, 1)),
     ?_assertEqual({ok, {[RE3], 1, false}},           erlesque:read_stream_backward(C, S, 2, 1)),
     ?_assertEqual({ok, {[], 2, false}},              erlesque:read_stream_backward(C, S, 3, 1)),
     ?_assertEqual({ok, {[RE1], -1, true}},           erlesque:read_stream_backward(C, S, 0, 2)),
     ?_assertEqual({ok, {[RE2, RE1], -1, true}},      erlesque:read_stream_backward(C, S, 1, 2)),
     ?_assertEqual({ok, {[RE3, RE2], 0, false}},      erlesque:read_stream_backward(C, S, 2, 2)),
     ?_assertEqual({ok, {[RE3], 1, false}},           erlesque:read_stream_backward(C, S, 3, 2)),
     ?_assertEqual({ok, {[RE3, RE2, RE1], -1, true}}, erlesque:read_stream_backward(C, S, 5, 100)),
     ?_assertEqual({ok, {[RE3, RE2], 0, false}},      erlesque:read_stream_backward(C, S, last, 2)),
     ?_assertEqual({ok, {[RE3, RE2], 0, false}},      erlesque:read_stream_backward(C, S, -1, 2)),

     {timeout, 100, ?_assertEqual([RE1, RE2, RE3], read_all_forward_and_filter(C, S))},
     {timeout, 100, ?_assertEqual([RE3, RE2, RE1], read_all_backward_and_filter(C, S))}
    ]}.

read_all_forward_and_filter(C, S) ->
    Res = read_all_forward_and_filter(C, S, {tfpos, 0, 0}, []),
    lists:reverse(Res).

read_all_forward_and_filter(C, S, Pos, Acc) ->
    {ok, {Events, NextPos, EndOfStream}} = erlesque:read_all_forward(C, Pos, 100, [{auth, {<<"admin">>, <<"changeit">>}}]),
    NewAcc = lists:foldl(fun(E, A) ->
        case E#event.stream_id =:= S of
            true -> [E | A];
            false -> A
        end
    end, Acc, Events),
    case EndOfStream of
        false -> read_all_forward_and_filter(C, S, NextPos, NewAcc);
        true -> NewAcc
    end.

read_all_backward_and_filter(C, S) ->
    Res = read_all_backward_and_filter(C, S, last, []),
    lists:reverse(Res).

read_all_backward_and_filter(C, S, Pos, Acc) ->
    {ok, {Events, NextPos, EndOfStream}} = erlesque:read_all_backward(C, Pos, 100, [{auth, {<<"admin">>, <<"changeit">>}}]),
    NewAcc = lists:foldl(fun(E, A) ->
        case E#event.stream_id =:= S of
            true -> [E | A];
            false -> A
        end
    end, Acc, Events),
    case EndOfStream of
        false -> read_all_backward_and_filter(C, S, NextPos, NewAcc);
        true -> NewAcc
    end.

subscriptions(C) ->
    S1 = gen_stream_id(),
    S2 = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    E4 = create_event(),
    E5 = create_event(),
    RE1 = map_event(S1, 0, E1),
    RE2 = map_event(S1, 1, E2),
    RE3 = map_event(S1, 2, E3),
    RE4 = map_event(S2, 0, E4),
    RE5 = map_event(S2, 1, E5),
    Subscr1 = create_subscriber(C, S1, 3),
    Subscr2 = create_subscriber(C, S2, 2),
    {ok, 0} = erlesque:append(C, S1, -1, [E1]),
    {ok, 0} = erlesque:append(C, S2, -1, [E4]),
    {ok, 1} = erlesque:append(C, S1, 0, [E2]),
    {ok, 1} = erlesque:append(C, S2, 0, [E5]),
    {ok, 2} = erlesque:append(C, S1, 1, [E3]),
    SubRes1 = receive
                  {subscriber_done, Subscr1, Res1} -> Res1
                  after 5000 -> subscriber1_timeout
              end,
    SubRes2 = receive
                  {subscriber_done, Subscr2, Res2} -> Res2
                  after 5000 -> subscriber2_timeout
              end,
    {inparallel, [
     ?_assertEqual([RE1, RE2, RE3], SubRes1),
     ?_assertEqual([RE4, RE5], SubRes2)
    ]}.

create_subscriber(C, StreamId, EventCount) ->
    SelfPid = self(),
    SubPid = spawn_link(fun() -> subscriber(EventCount, SelfPid, []) end),
    {ok, {_LastCommitPos, _LastEventNumber}} = erlesque:subscribe(C, StreamId, [{subscriber, SubPid}]),
    SubPid.

subscriber(0, RespPid, Acc) ->
    Res = lists:reverse(Acc),
    RespPid ! {subscriber_done, self(), Res};
subscriber(EventsToGo, RespPid, Acc) ->
    receive
        {event, E} -> subscriber(EventsToGo-1, RespPid, [E | Acc]);
        Unexpected -> erlang:display({unexpected_msg, Unexpected})
    end.

create_event() ->
    #event_data{event_type = <<"test-type">>, data = <<"some data">>, metadata = <<"some meta">>}.

map_event(StreamId, EventNumber, EventData) ->
    #event{stream_id    = StreamId,
           event_number = EventNumber,
           event_id     = EventData#event_data.event_id,
           event_type   = EventData#event_data.event_type,
           data         = EventData#event_data.data,
           metadata     = EventData#event_data.metadata}.

gen_stream_id() ->
    UuidStr = erlesque_utils:uuid_to_string(erlesque_utils:gen_uuid()),
    iolist_to_binary(io_lib:format("esq-test-~s", [UuidStr])).
