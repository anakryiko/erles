-module(erlesque_ops).
-export([start_link/3, connected/1, disconnected/1, aborted/1, handle_pkg/2]).

-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, code_change/4, terminate/3]).
-export([disconnected/2, disconnected/3,
         pending/2, pending/3,
         retry_pending/2, retry_pending/3]).

-include("../include/erlesque_clientapi_pb.hrl").
-include("../include/erlesque.hrl").

-record(state, {req_cmd,
                corr_id,
                client_pid,
                conn_pid,
                timeout,
                trials,
                retry_delay,
                op_params,
                timer_ref}).

start_link(ReqCmd, SysParams, OpParams) ->
    gen_fsm:start_link(?MODULE, {ReqCmd, SysParams, OpParams}, []).

connected(Pid) ->
    gen_fsm:send_event(Pid, connected).

disconnected(Pid) ->
    gen_fsm:send_event(Pid, disconnected).

aborted(Pid) ->
    gen_fsm:send_event(Pid, aborted).

handle_pkg(Pid, Pkg) ->
    gen_fsm:send_event(Pid, Pkg).



init({ReqCmd, {CorrId, ClientPid, ConnPid, Timeout, Trials, RetryDelay}, OpParams}) ->
    State = #state{req_cmd = ReqCmd,
                   corr_id = CorrId,
                   client_pid = ClientPid,
                   conn_pid = ConnPid,
                   timeout = Timeout,
                   trials = Trials,
                   retry_delay = RetryDelay,
                   op_params = OpParams,
                   timer_ref = none},
    {ok, disconnected, State}.


handle_event(closed, _StateName, State) ->
    complete(State, {error, connection_closed});

handle_event(Event, StateName, State) ->
    io:format("Unexpected GLOBAL ASYNC EVENT  ~p in state ~p, state data ~p~n", [Event, StateName, State]),
    {next_state, StateName, State}.

handle_sync_event(Event, From, StateName, State) ->
    io:format("Unexpected GLOBAL SYNC EVENT ~p from ~p in state ~p, state data ~p~n", [Event, From, StateName, State]),
    {next_state, StateName, State}.


disconnected(connected, State) ->
    issue_request(State);

disconnected(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, disconnected, State]),
    {next_state, disconnected, State}.

disconnected(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, disconnected, State]),
    {next_state, disconnected, State}.


pending({pkg, Cmd, CorrId, _Auth, Data}, State=#state{req_cmd=ReqCmd, corr_id=CorrId}) ->
    RespCmd = response_cmd(ReqCmd),
    case Cmd of
        RespCmd ->
            case deserialize_result(ReqCmd, RespCmd, Data) of
                {complete, Result} ->
                    complete(State, Result);
                {retry, Reason} ->
                    retry(Reason, State)
            end;
        not_handled ->
            not_handled(Data, State);
        not_authenticated ->
            complete(State, {not_authenticated, Data});
        bad_request ->
            complete(State, {bad_request, Data});
        _ ->
            io:format("Unexpected command received: ~p, data: ~p.~n", [Cmd, Data]),
            {next_state, pending, State}
    end;

pending(disconnected, State) ->
    Trials = State#state.trials - 1,
    case Trials > 0 of
        true ->
            cancel_timer(State#state.timer_ref),
            {next_state, disconnected, State#state{trials=Trials, timer_ref=none}};
        false ->
            complete(State, {error, retry_limit})
    end;

pending({timeout, TimerRef, timeout}, State=#state{timer_ref=TimerRef}) ->
    complete(State, {error, server_not_responded});

pending(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, pending, State]),
    {next_state, pending, State}.

pending(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, pending, State]),
    {next_state, pending, State}.


retry_pending({timeout, TimerRef, retry}, State=#state{timer_ref=TimerRef}) ->
    issue_request(State);

retry_pending(disconnected, State) ->
    cancel_timer(State#state.timer_ref),
    {next_state, disconnected, State#state{timer_ref=none}};

retry_pending(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, retry_pending, State]),
    {next_state, retry_pending, State}.

retry_pending(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, retry, State]),
    {next_state, retry, State}.


handle_info(TimerMsg={timer, _, _}, StateName, State) ->
    gen_fsm:send_event(self(), TimerMsg),
    {next_state, StateName, State};

handle_info(Msg, StateName, State) ->
    io:format("Unexpected INFO message ~p, state name ~p, state data ~p~n", [Msg, StateName, State]),
    {next_state, StateName, State}.


terminate(normal, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

issue_request(State) ->
    send_request(State),
    TimerRef = erlang:start_timer(State#state.timeout, self(), timeout),
    {next_state, pending, State#state{timer_ref=TimerRef}}.

send_request(#state{conn_pid=ConnPid, corr_id=CorrId, req_cmd=ReqCmd, op_params=OpParams}) ->
    Pkg = create_package(CorrId, noauth, ReqCmd, OpParams),
    erlesque_conn:send(ConnPid, Pkg).

complete(State, Result) ->
    cancel_timer(State#state.timer_ref),
    erlesque_fsm:operation_completed(State#state.client_pid, State#state.corr_id, Result),
    {stop, normal, State}.

not_handled(Data, State) ->
    #nothandled{reason=Reason, additional_info=AddInfo} = erlesque_clientapi_pb:decode_nothandled(Data),
    case Reason of
        'NotMaster' ->
            #nothandled_masterinfo{external_tcp_address=IpStr, external_tcp_port=Port} =
                erlesque_clientapi_pb:decode_nothandled_masterinfo(AddInfo),
            Ip = IpStr,%ipstr_to_ip(IpStr),
            erlesque_fsm:reconnect(State#state.client_pid, Ip, Port),
            cancel_timer(State#state.timer_ref),
            {next_state, disconnected, State#state{timer_ref=none}};
        _ ->
            retry(Reason, State)
    end.

retry(Reason, State) ->
    io:format("Retrying ~p because ~p.~n", [State#state.req_cmd, Reason]),
    Trials = State#state.trials - 1,
    case Trials > 0 of
        true ->
            cancel_timer(State#state.timer_ref),
            NewCorrId = erlesque_utils:create_uuid_v4(),
            erlesque_fsm:operation_restarted(State#state.client_pid, State#state.corr_id, NewCorrId),
            TimerRef = erlang:start_timer(State#state.retry_delay, self(), retry),
            {next_state, retry_pending, State#state{corr_id=NewCorrId, trials=Trials, timer_ref=TimerRef}};
        false ->
            complete(State, {error, retry_limit})
    end.

cancel_timer(none) -> ok;
cancel_timer(TimerRef) -> erlang:cancel_timer(TimerRef).

bool_to_int(Bool) when is_boolean(Bool) ->
    case Bool of
        true -> 1;
        false -> 0
    end.

response_cmd(write_events) ->                write_events_completed;
response_cmd(transaction_start) ->           transaction_start_completed;
response_cmd(transaction_write) ->           transaction_write_completed;
response_cmd(transaction_commit) ->          transaction_commit_completed;
response_cmd(delete_stream) ->               delete_stream_completed;
response_cmd(read_event) ->                  read_event_completed;
response_cmd(read_stream_events_forward) ->  read_stream_events_forward_completed;
response_cmd(read_stream_events_backward) -> read_stream_events_backward_completed;
response_cmd(read_all_events_forward) ->     read_all_events_forward_completed;
response_cmd(read_all_events_backward) ->    read_all_events_backward_completed.

create_package(CorrId, Auth, write_events, {StreamId, ExpectedVersion, Events, RequireMaster}) ->
    Dto = #writeevents{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        events = lists:map(fun(X) ->
            #newevent{event_id = X#event_data.event_id,
                      event_type = X#event_data.event_id,
                      data_content_type = bool_to_int(X#event_data.is_json),
                      metadata_content_type = 0,
                      data = X#event_data.data,
                      metadata = X#event_data.metadata}
        end, Events),
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_writeevents(Dto),
    erlesque_pkg:create(write_events, CorrId, Auth, Bin);

create_package(CorrId, Auth, transaction_start, {StreamId, ExpectedVersion, RequireMaster}) ->
    Dto = #transactionstart{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_transactionstart(Dto),
    erlesque_pkg:create(transaction_start, CorrId, Auth, Bin);

create_package(CorrId, Auth, transaction_write, {TransactionId, Events, RequireMaster}) ->
    Dto = #transactionwrite{
        transaction_id = TransactionId,
        events = lists:map(fun(X) ->
            #newevent{event_id = X#event_data.event_id,
                      event_type = X#event_data.event_id,
                      data_content_type = bool_to_int(X#event_data.is_json),
                      metadata_content_type = 0,
                      data = X#event_data.data,
                      metadata = X#event_data.metadata}
        end, Events),
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_transactionwrite(Dto),
    erlesque_pkg:create(transaction_write, CorrId, Auth, Bin);

create_package(CorrId, Auth, transaction_commit, {TransactionId, RequireMaster}) ->
    Dto = #transactioncommit{
        transaction_id = TransactionId,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_transactioncommit(Dto),
    erlesque_pkg:create(transaction_commit, CorrId, Auth, Bin);

create_package(CorrId, Auth, delete_stream, {StreamId, ExpectedVersion, RequireMaster}) ->
    Dto = #deletestream{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_deletestream(Dto),
    erlesque_pkg:create(deletestream, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_event, {StreamId, EventNumber, ResolveLinks, RequireMaster}) ->
    Dto = #readevent{
        event_stream_id = StreamId,
        event_number = EventNumber,
        resolve_link_tos = ResolveLinks,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_readevent(Dto),
    erlesque_pkg:create(read_event, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_stream_events_forward, {StreamId, FromEventNumber, MaxCount, ResolveLinks, RequireMaster}) ->
    Dto = #readstreamevents{
        event_stream_id = StreamId,
        from_event_number = FromEventNumber,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_readstreamevents(Dto),
    erlesque_pkg:create(read_stream_events_forward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_stream_events_backward, {StreamId, FromEventNumber, MaxCount, ResolveLinks, RequireMaster}) ->
    Dto = #readstreamevents{
        event_stream_id = StreamId,
        from_event_number = FromEventNumber,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_readstreamevents(Dto),
    erlesque_pkg:create(read_stream_events_backward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_all_events_forward, {CommitPos, PreparePos, MaxCount, ResolveLinks, RequireMaster}) ->
    Dto = #readallevents{
        commit_position = CommitPos,
        prepare_position = PreparePos,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_readallevents(Dto),
    erlesque_pkg:create(read_all_events_forward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_all_events_backward, {CommitPos, PreparePos, MaxCount, ResolveLinks, RequireMaster}) ->
    Dto = #readallevents{
        commit_position = CommitPos,
        prepare_position = PreparePos,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_readallevents(Dto),
    erlesque_pkg:create(read_all_events_backward, CorrId, Auth, Bin).


deserialize_result(write_events, write_events_completed, Data) ->
    Dto = erlesque_clientapi_pb:decode_writeeventscompleted(Data),
    case Dto#writeeventscompleted.result of
        'Success' ->              {complete, {ok, Dto#writeeventscompleted.first_event_number}};
        'PrepareTimeout' ->       {retry, prepare_timeout};
        'CommitTimeout' ->        {retry, commit_timeout};
        'ForwardTimeout' ->       {retry, forward_timeout};
        'WrongExpectedVersion' -> {complete, {error, wrong_expected_version}};
        'StreamDeleted' ->        {complete, {error, stream_deleted}};
        'InvalidTransaction' ->   {complete, {error, invalid_transaction}};
        'AccessDenied' ->         {complete, {error, access_denied}}
    end;

deserialize_result(transaction_start, transaction_start_completed, Data) ->
    erlesque_clientapi_pb:decode_transactionstartcompleted(Data);
deserialize_result(transaction_write, transaction_write_completed, Data) ->
    erlesque_clientapi_pb:decode_transactionwritecompleted(Data);
deserialize_result(transaction_commit, transaction_commit_completed, Data) ->
    erlesque_clientapi_pb:decode_transactioncommitcompleted(Data);
deserialize_result(delete_stream, delete_stream_completed, Data) ->
    erlesque_clientapi_pb:decode_deletestreamcompleted(Data);
deserialize_result(read_event, read_event_completed, Data) ->
    erlesque_clientapi_pb:decode_readeventcompleted(Data);
deserialize_result(read_stream_events_forward, read_stream_events_forward_completed, Data) ->
    erlesque_clientapi_pb:decode_readstreameventsforwardcompleted(Data);
deserialize_result(read_stream_events_backward, read_stream_events_backward_completed, Data) ->
    erlesque_clientapi_pb:decode_readstreameventsbackwardcompleted(Data);
deserialize_result(read_all_events_forward, read_all_events_forward_completed, Data) ->
    erlesque_clientapi_pb:decode_readalleventsforwardcompleted(Data);
deserialize_result(read_all_events_backward, read_all_events_backward_completed, Data) ->
    erlesque_clientapi_pb:decode_readalleventsbackwardcompleted(Data).
