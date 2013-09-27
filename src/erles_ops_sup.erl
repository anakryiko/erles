-module(erles_ops_sup).
-behavior(supervisor).

-export([start_link/0, start_operation/4]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, nothing).

start_operation(SupPid, ReqCmd, SysParams, OpParams) ->
    supervisor:start_child(SupPid, [ReqCmd, SysParams, OpParams]).


init(nothing) ->
    {ok, {{simple_one_for_one, 3, 60},
          [{erles_ops,
            {erles_ops, start_link, []},
            transient, infinity, worker, [erles_ops]}
          ]}}.
