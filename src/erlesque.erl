-module(erlesque).
-export([]).

create(Settings = #connection_settings{}, {node, IpAddr, Port}) ->
    thrown(not_implemented).

create(Settings = #connection_settings{}, {cluster, IpAddr, Port}) ->
    thrown(not_implemented).
