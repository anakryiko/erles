-module(erlesque_ops_sup).
-behavior(supervisor).

-export([start_link/0, stop/1, start_operation/4]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, nothing).

stop(SupPid) ->
    supervisor:stop(SupPid).

start_operation(SupPid, Operation, SysParams, OpParams) ->
    supervisor:start_child(SupPid, [Operation, SysParams, OpParams]).


init(nothing) ->
    {ok, {{simple_one_for_one, 3, 60},
          [{operation,
            {erlesque_ops, start_link, []},
            transient, infinity, worker, [erlesque_operation]}
          ]}}.
