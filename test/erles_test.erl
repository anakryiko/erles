-module(erles_test).
-compile(export_all).

-include("erles.hrl").

test(Cnt) ->
    {ok, C} = erles:connect(node, {{127,0,0,1}, 1113}),
    {TimePar, _Value1} = timer:tc(fun() -> test_par(Cnt, Cnt, C) end),
    {TimeSeq, _Value2} = timer:tc(fun() -> test_seq(Cnt, Cnt, C) end),
    {TimePar, TimeSeq}.

test_par(0, 0, _C) -> ok;
test_par(0, Cnt2, C) -> receive done -> test_par(0, Cnt2-1, C) end;
test_par(Cnt1, Cnt2, C) ->
    Self = self(),
    spawn_link(fun() ->
        {ok, _} = erles:append(C, <<"test-event">>, any, [erles_req_tests:create_event()]),
        Self ! done,
        ok
    end),
    test_par(Cnt1-1, Cnt2, C).

test_seq(0, 0, _C) -> ok;
test_seq(0, Cnt2, C) -> receive done -> test_seq(0, Cnt2-1, C) end;
test_seq(Cnt1, Cnt2, C) ->
    {ok, _} = erles:append(C, <<"test-event">>, any, [erles_req_tests:create_event()]),
    self() ! done,
    test_seq(Cnt1-1, Cnt2, C).
