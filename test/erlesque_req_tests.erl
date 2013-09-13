-module(erlesque_req_tests).
-include_lib("eunit/include/eunit.hrl").
-include("erlesque.hrl").

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

             reads(D)
            ]
            %}
     end}.

setup() ->
    %% here we assume that node on default IP and port is already started
    {ok, C} = erlesque:connect({node, {127,0,0,1}, 1113}),
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
    RE1 = #event{stream_id    = S,
                 event_number = 0,
                 event_id     = E1#event_data.event_id,
                 event_type   = E1#event_data.event_type,
                 data         = E1#event_data.data,
                 metadata     = E1#event_data.metadata},
    RE2 = #event{stream_id    = S,
                 event_number = 1,
                 event_id     = E2#event_data.event_id,
                 event_type   = E2#event_data.event_type,
                 data         = E2#event_data.data,
                 metadata     = E2#event_data.metadata},
    RE3 = #event{stream_id    = S,
                 event_number = 2,
                 event_id     = E3#event_data.event_id,
                 event_type   = E3#event_data.event_type,
                 data         = E3#event_data.data,
                 metadata     = E3#event_data.metadata},
    io:format("~p~n", [RE3]),
    {ok, 2} = erlesque:append(C, S, -1, [E1, E2, E3]),
    [?_assertEqual({ok, RE1}, erlesque:read_event(C, S, 0)),
     ?_assertEqual({ok, RE2}, erlesque:read_event(C, S, 1)),
     ?_assertEqual({ok, RE3}, erlesque:read_event(C, S, 2)),
     ?_assertEqual({ok, RE3}, erlesque:read_event(C, S, last))
    ].

create_event() ->
    #event_data{event_type = <<"test-type">>, data = <<"some data">>}.

gen_stream_id() ->
    iolist_to_binary(io_lib:format("esq-test-~s", [erlesque_utils:gen_uuid()])).
