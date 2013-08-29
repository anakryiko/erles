-module(erlesque_utils).
-export([create_uuid_v4/0]).

create_uuid_v4() ->
    <<R1:48, _:4, R2:12, _:2, R3:62>> = crypto:rand_bytes(16),
    <<R1:48, 0:1, 1:1, 0:1, 0:1, R2:12, 1:1, 0:1, R3:62>>.
