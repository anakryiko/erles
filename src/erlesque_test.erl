-module(erlesque_test).
-compile(export_all).

-include("erlesque.hrl").

event() ->
    #event_data{event_type = <<"test-type">>, data = <<"some data">>}.

test() ->
    {ok, C} = erlesque:connect({node, {127,0,0,1}, 1113}, []),
    erlesque:append(C, <<"test-stream">>, any, [event()]).

