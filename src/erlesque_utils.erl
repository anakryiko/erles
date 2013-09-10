-module(erlesque_utils).
-export([create_uuid_v4/0]).
-export([resolved_event/1, resolved_events/1]).

create_uuid_v4() ->
    <<R1:48, _:4, R2:12, _:2, R3:62>> = crypto:rand_bytes(16),
    <<R1:48, 0:1, 1:1, 0:1, 0:1, R2:12, 1:1, 0:1, R3:62>>.

resolved_events(Events) ->
    lists:map(fun(E) -> resolved_event(E) end, Events).

resolved_event(_EventDto) ->
    not_implemented.
