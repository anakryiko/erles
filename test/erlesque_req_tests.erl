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
             append_expver(D)
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
    Stream = <<"any-stream">>,
    [?_assertMatch(ok, erlesque:append(C, Stream, any, [create_event()])),
     ?_assertMatch(ok, erlesque:append(C, Stream, any, [create_event()])),
     ?_assertMatch(ok, erlesque:append(C, Stream, -2, [create_event()]))
    ].

append_expver(C) ->
    Stream = <<"explicit-ver-stream">>,
    [?_assertMatch(ok, erlesque:append(C, Stream, -1, [create_event()])),
     ?_assertMatch(ok, erlesque:append(C, Stream,  0, [create_event()])),
     ?_assertMatch({error, wrong_expected_version},
                   erlesque:append(C, Stream, -1, [create_event()]))
    ].


create_event() ->
    #event_data{event_type = <<"test-type">>, data = <<"some data">>}.
