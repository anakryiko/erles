-module(erlesque_reqs).

-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, code_change/4, terminate/3]).
-export([disconnected/2, disconnected/3,
         pending/2, pending/3,
         retry_pending/2, retry_pending/3]).

-include("erlesque_clientapi_pb.hrl").
-include("erlesque.hrl").
-include("erlesque_internal.hrl").

-record(state, {req_cmd,
                corr_id,
                esq_pid,
                conn_pid,
                reply_pid,
                timeout,
                retries,
                retry_delay,
                auth,
                op_params,
                timer_ref}).

init({ReqCmd, S=#sys_params{}, OpParams}) ->
    State = #state{req_cmd = ReqCmd,
                   corr_id = S#sys_params.corr_id,
                   esq_pid = S#sys_params.esq_pid,
                   conn_pid = S#sys_params.conn_pid,
                   reply_pid = S#sys_params.reply_pid,
                   timeout = S#sys_params.op_timeout,
                   retries = S#sys_params.op_retries,
                   retry_delay = S#sys_params.retry_delay,
                   auth = S#sys_params.auth,
                   op_params = OpParams,
                   timer_ref = none},
    {ok, disconnected, State}.


handle_event(Event, StateName, State) ->
    io:format("Unexpected GLOBAL ASYNC EVENT  ~p in state ~p, state data ~p~n", [Event, StateName, State]),
    {next_state, StateName, State}.

handle_sync_event(Event, From, StateName, State) ->
    io:format("Unexpected GLOBAL SYNC EVENT ~p from ~p in state ~p, state data ~p~n", [Event, From, StateName, State]),
    {next_state, StateName, State}.


disconnected(connected, State) ->
    issue_request(State);

disconnected({aborted, Reason}, State) ->
    abort(State, {error, {aborted, Reason}});

disconnected(Msg, State) ->
    io:format("Unexpected ASYNC EVENT ~p, state name ~p, state data ~p~n", [Msg, disconnected, State]),
    {next_state, disconnected, State}.

disconnected(Msg, From, State) ->
    io:format("Unexpected SYNC EVENT ~p from ~p, state name ~p, state data ~p~n", [Msg, From, disconnected, State]),
    {next_state, disconnected, State}.


pending({pkg, Cmd, CorrId, _Auth, Data}, State=#state{corr_id=CorrId}) ->
    ReqCmd = State#state.req_cmd,
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
    Retries = State#state.retries - 1,
    case Retries >= 0 of
        true ->
            cancel_timer(State#state.timer_ref),
            {next_state, disconnected, State#state{retries=Retries, timer_ref=none}};
        false ->
            complete(State, {error, retry_limit})
    end;

pending({aborted, Reason}, State) ->
    abort(State, {error, {aborted, Reason}});

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

retry_pending({aborted, Reason}, State) ->
    abort(State, {error, {aborted, Reason}});

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

send_request(S=#state{}) ->
    Pkg = create_package(S#state.corr_id, S#state.auth, S#state.req_cmd, S#state.op_params),
    erlesque_conn:send(S#state.conn_pid, Pkg).

complete(State=#state{}, Result) ->
    cancel_timer(State#state.timer_ref),
    gen_fsm:reply(State#state.reply_pid, Result),
    erlesque_fsm:operation_completed(State#state.esq_pid, State#state.corr_id),
    {stop, normal, State}.

abort(State=#state{}, Result) ->
    cancel_timer(State#state.timer_ref),
    gen_fsm:reply(State#state.reply_pid, Result),
    {stop, normal, State}.

not_handled(Data, State) ->
    Dto = erlesque_clientapi_pb:decode_nothandled(Data),
    case Dto#nothandled.reason of
        'NotMaster' ->
            MasterInfo = erlesque_clientapi_pb:decode_nothandled_masterinfo(Dto#nothandled.additional_info),
            IpStr = MasterInfo#nothandled_masterinfo.external_tcp_address,
            Ip = IpStr,%ipstr_to_ip(IpStr),
            Port = MasterInfo#nothandled_masterinfo.external_tcp_port,
            erlesque_fsm:reconnect(State#state.esq_pid, Ip, Port),
            cancel_timer(State#state.timer_ref),
            {next_state, disconnected, State#state{timer_ref=none}};
        Reason ->
            retry(Reason, State)
    end.

retry(Reason, State) ->
    io:format("Retrying ~p because ~p.~n", [State#state.req_cmd, Reason]),
    Retries = State#state.retries - 1,
    case Retries >= 0 of
        true ->
            cancel_timer(State#state.timer_ref),
            NewCorrId = erlesque_utils:gen_uuid(),
            erlesque_fsm:operation_restarted(State#state.esq_pid, State#state.corr_id, NewCorrId),
            TimerRef = erlang:start_timer(State#state.retry_delay, self(), retry),
            {next_state, retry_pending, State#state{corr_id=NewCorrId, retries=Retries, timer_ref=TimerRef}};
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
                      event_type = X#event_data.event_type,
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
                      event_type = X#event_data.event_type,
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
    erlesque_pkg:create(delete_stream, CorrId, Auth, Bin);

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

create_package(CorrId, Auth, read_all_events_forward, {{tfpos, CommitPos, PreparePos}, MaxCount, ResolveLinks, RequireMaster}) ->
    Dto = #readallevents{
        commit_position = CommitPos,
        prepare_position = PreparePos,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = RequireMaster
    },
    Bin = erlesque_clientapi_pb:encode_readallevents(Dto),
    erlesque_pkg:create(read_all_events_forward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_all_events_backward, {{tfpos, CommitPos, PreparePos}, MaxCount, ResolveLinks, RequireMaster}) ->
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
    io:format("~p~n", [Dto]),
    case Dto#writeeventscompleted.result of
        'Success' -> {complete, {ok, Dto#writeeventscompleted.last_event_number}};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(transaction_start, transaction_start_completed, Data) ->
    Dto = erlesque_clientapi_pb:decode_transactionstartcompleted(Data),
    case Dto#transactionstartcompleted.result of
        'Success' -> {complete, {ok, Dto#transactionstartcompleted.transaction_id}};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(transaction_write, transaction_write_completed, Data) ->
    Dto = erlesque_clientapi_pb:decode_transactionwritecompleted(Data),
    case Dto#transactionwritecompleted.result of
        'Success' -> {complete, ok};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(transaction_commit, transaction_commit_completed, Data) ->
    Dto = erlesque_clientapi_pb:decode_transactioncommitcompleted(Data),
    case Dto#transactioncommitcompleted.result of
        'Success' -> {complete, {ok, Dto#transactioncommitcompleted.last_event_number}};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(delete_stream, delete_stream_completed, Data) ->
    Dto = erlesque_clientapi_pb:decode_deletestreamcompleted(Data),
    case Dto#deletestreamcompleted.result of
        'Success' -> {complete, ok};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(read_event, read_event_completed, Data) ->
    Dto = erlesque_clientapi_pb:decode_readeventcompleted(Data),
    case Dto#readeventcompleted.result of
        'Success' ->       {complete, {ok, erlesque_utils:resolved_event(Dto#readeventcompleted.event)}};
        'NotFound' ->      {complete, {error, no_event}};
        'NoStream' ->      {complete, {error, no_stream}};
        'StreamDeleted' -> {complete, {error, stream_deleted}};
        'Error' ->         {complete, {error, Dto#readeventcompleted.error}};
        'AccessDenied' ->  {complete, {error, access_denied}}
    end;

deserialize_result(read_stream_events_forward, read_stream_events_forward_completed, Data) ->
    deserialize_streameventscompleted(Data);

deserialize_result(read_stream_events_backward, read_stream_events_backward_completed, Data) ->
    deserialize_streameventscompleted(Data);

deserialize_result(read_all_events_forward, read_all_events_forward_completed, Data) ->
    deserialize_alleventscompleted(Data);

deserialize_result(read_all_events_backward, read_all_events_backward_completed, Data) ->
    deserialize_alleventscompleted(Data).

decode_write_failure(OperationResult) ->
    case OperationResult of
        'PrepareTimeout' ->       {retry, prepare_timeout};
        'CommitTimeout' ->        {retry, commit_timeout};
        'ForwardTimeout' ->       {retry, forward_timeout};
        'WrongExpectedVersion' -> {complete, {error, wrong_expected_version}};
        'StreamDeleted' ->        {complete, {error, stream_deleted}};
        'InvalidTransaction' ->   {complete, {error, invalid_transaction}};
        'AccessDenied' ->         {complete, {error, access_denied}}
    end.

deserialize_streameventscompleted(Data) ->
    Dto = erlesque_clientapi_pb:decode_readstreameventscompleted(Data),
    case Dto#readstreameventscompleted.result of
        'Success' ->       {complete, {ok, {
            erlesque_utils:resolved_events(Dto#readstreameventscompleted.events),
            Dto#readstreameventscompleted.next_event_number,
            Dto#readstreameventscompleted.is_end_of_stream
        }}};
        'NoStream' ->      {complete, {error, no_stream}};
        'StreamDeleted' -> {complete, {error, stream_deleted}};
        'Error' ->         {complete, {error, Dto#readstreameventscompleted.error}};
        'AccessDenied' ->  {complete, {error, access_denied}}
    end.

deserialize_alleventscompleted(Data) ->
    Dto = erlesque_clientapi_pb:decode_readalleventscompleted(Data),
    case Dto#readalleventscompleted.result of
        'Success' ->       {complete, {ok, {
            erlesque_utils:resolved_events(Dto#readalleventscompleted.events),
            {tfpos, Dto#readalleventscompleted.next_commit_position, Dto#readalleventscompleted.next_prepare_position}
        }}};
        'Error' ->         {complete, {error, Dto#readalleventscompleted.error}};
        'AccessDenied' ->  {complete, {error, access_denied}}
    end.
