-module(erlesque).
-export([create/1]).

create({node, Ip, Port}) ->
    erlesque_fsm:start_link({node, Ip, Port});

create({cluster, ClusterDns, ManagerPort}) ->
    throw(not_implemented).
