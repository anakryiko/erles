-module(erles_reqs).

-export([init/1, handle_sync_event/4, handle_event/3, handle_info/3, code_change/4, terminate/3]).
-export([disconnected/2, disconnected/3,
         pending/2, pending/3,
         retry_pending/2, retry_pending/3]).

-include("erles_clientapi_pb.hrl").
-include("erles.hrl").
-include("erles_internal.hrl").

-record(state, {req_cmd,
                corr_id,
                els_pid,
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
                   els_pid = S#sys_params.els_pid,
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

disconnected(disconnected, State) ->
    {next_state, disconnected, State};

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
            complete(State, {error, {not_authenticated, Data}});
        bad_request ->
            complete(State, {error, {bad_request, Data}});
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
    complete(State, {error, server_timeout});

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


handle_info(TimerMsg={timeout, _, _}, StateName, State) ->
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
    erles_conn:send(S#state.conn_pid, Pkg).

complete(State=#state{}, Result) ->
    cancel_timer(State#state.timer_ref),
    gen_fsm:reply(State#state.reply_pid, Result),
    erles_fsm:operation_completed(State#state.els_pid, State#state.corr_id),
    {stop, normal, State}.

abort(State=#state{}, Result) ->
    cancel_timer(State#state.timer_ref),
    gen_fsm:reply(State#state.reply_pid, Result),
    {stop, normal, State}.

not_handled(Data, State) ->
    Dto = erles_clientapi_pb:decode_nothandled(Data),
    case Dto#nothandled.reason of
        'NotMaster' ->
            MasterInfo = erles_clientapi_pb:decode_nothandled_masterinfo(Dto#nothandled.additional_info),
            Ip = erles_utils:parse_ip(MasterInfo#nothandled_masterinfo.external_tcp_address),
            Port = MasterInfo#nothandled_masterinfo.external_tcp_port,
            cancel_timer(State#state.timer_ref),
            case erles_conn:reconnect(State#state.conn_pid, Ip, Port) of
                already_connected ->
                    issue_request(State);
                ok ->
                    {next_state, disconnected, State#state{timer_ref=none}}
            end;
        Reason ->
            retry(Reason, State)
    end.

retry(Reason, State) ->
    io:format("Retrying ~p because ~p.~n", [State#state.req_cmd, Reason]),
    Retries = State#state.retries - 1,
    case Retries >= 0 of
        true ->
            cancel_timer(State#state.timer_ref),
            NewCorrId = erles_utils:gen_uuid(),
            erles_fsm:operation_restarted(State#state.els_pid, State#state.corr_id, NewCorrId),
            TimerRef = erlang:start_timer(State#state.retry_delay, self(), retry),
            {next_state, retry_pending, State#state{corr_id=NewCorrId, retries=Retries, timer_ref=TimerRef}};
        false ->
            complete(State, {error, retry_limit})
    end.

cancel_timer(none) -> ok;
cancel_timer(TimerRef) -> erlang:cancel_timer(TimerRef).

datatype_to_int(raw) -> 0;
datatype_to_int(json) -> 1.

response_cmd(ping) ->                        pong;
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

create_package(CorrId, Auth, ping, {}) ->
    erles_pkg:create(ping, CorrId, Auth, <<>>);

create_package(CorrId, Auth, write_events, {StreamId, ExpectedVersion, Events, MasterOnly}) ->
    Dto = #writeevents{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        events = lists:map(fun(X) ->
            #newevent{event_id = X#event_data.event_id,
                      event_type = X#event_data.event_type,
                      data_content_type = datatype_to_int(X#event_data.data_type),
                      metadata_content_type = 0,
                      data = X#event_data.data,
                      metadata = X#event_data.metadata}
        end, Events),
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_writeevents(Dto),
    erles_pkg:create(write_events, CorrId, Auth, Bin);

create_package(CorrId, Auth, transaction_start, {StreamId, ExpectedVersion, MasterOnly}) ->
    Dto = #transactionstart{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_transactionstart(Dto),
    erles_pkg:create(transaction_start, CorrId, Auth, Bin);

create_package(CorrId, Auth, transaction_write, {TransactionId, Events, MasterOnly}) ->
    Dto = #transactionwrite{
        transaction_id = TransactionId,
        events = lists:map(fun(X) ->
            #newevent{event_id = X#event_data.event_id,
                      event_type = X#event_data.event_type,
                      data_content_type = datatype_to_int(X#event_data.data_type),
                      metadata_content_type = 0,
                      data = X#event_data.data,
                      metadata = X#event_data.metadata}
        end, Events),
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_transactionwrite(Dto),
    erles_pkg:create(transaction_write, CorrId, Auth, Bin);

create_package(CorrId, Auth, transaction_commit, {TransactionId, MasterOnly}) ->
    Dto = #transactioncommit{
        transaction_id = TransactionId,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_transactioncommit(Dto),
    erles_pkg:create(transaction_commit, CorrId, Auth, Bin);

create_package(CorrId, Auth, delete_stream, {StreamId, ExpectedVersion, DeleteType, MasterOnly}) ->
    Dto = #deletestream{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        require_master = MasterOnly,
        hard_delete = DeleteType =:= 'perm'
    },
    Bin = erles_clientapi_pb:encode_deletestream(Dto),
    erles_pkg:create(delete_stream, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_event, {StreamId, EventNumber, ResolveLinks, MasterOnly}) ->
    Dto = #readevent{
        event_stream_id = StreamId,
        event_number = EventNumber,
        resolve_link_tos = ResolveLinks,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_readevent(Dto),
    erles_pkg:create(read_event, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_stream_events_forward, {StreamId, FromEventNumber, MaxCount, ResolveLinks, MasterOnly}) ->
    Dto = #readstreamevents{
        event_stream_id = StreamId,
        from_event_number = FromEventNumber,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_readstreamevents(Dto),
    erles_pkg:create(read_stream_events_forward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_stream_events_backward, {StreamId, FromEventNumber, MaxCount, ResolveLinks, MasterOnly}) ->
    Dto = #readstreamevents{
        event_stream_id = StreamId,
        from_event_number = FromEventNumber,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_readstreamevents(Dto),
    erles_pkg:create(read_stream_events_backward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_all_events_forward, {{tfpos, CommitPos, PreparePos}, MaxCount, ResolveLinks, MasterOnly}) ->
    Dto = #readallevents{
        commit_position = CommitPos,
        prepare_position = PreparePos,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_readallevents(Dto),
    erles_pkg:create(read_all_events_forward, CorrId, Auth, Bin);

create_package(CorrId, Auth, read_all_events_backward, {{tfpos, CommitPos, PreparePos}, MaxCount, ResolveLinks, MasterOnly}) ->
    Dto = #readallevents{
        commit_position = CommitPos,
        prepare_position = PreparePos,
        max_count = MaxCount,
        resolve_link_tos = ResolveLinks,
        require_master = MasterOnly
    },
    Bin = erles_clientapi_pb:encode_readallevents(Dto),
    erles_pkg:create(read_all_events_backward, CorrId, Auth, Bin).

deserialize_result(ping, pong, _Data) ->
    {complete, ok};

deserialize_result(write_events, write_events_completed, Data) ->
    Dto = erles_clientapi_pb:decode_writeeventscompleted(Data),
    case Dto#writeeventscompleted.result of
        'Success' -> {complete, {ok, Dto#writeeventscompleted.last_event_number}};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(transaction_start, transaction_start_completed, Data) ->
    Dto = erles_clientapi_pb:decode_transactionstartcompleted(Data),
    case Dto#transactionstartcompleted.result of
        'Success' -> {complete, {ok, Dto#transactionstartcompleted.transaction_id}};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(transaction_write, transaction_write_completed, Data) ->
    Dto = erles_clientapi_pb:decode_transactionwritecompleted(Data),
    case Dto#transactionwritecompleted.result of
        'Success' -> {complete, ok};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(transaction_commit, transaction_commit_completed, Data) ->
    Dto = erles_clientapi_pb:decode_transactioncommitcompleted(Data),
    case Dto#transactioncommitcompleted.result of
        'Success' -> {complete, {ok, Dto#transactioncommitcompleted.last_event_number}};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(delete_stream, delete_stream_completed, Data) ->
    Dto = erles_clientapi_pb:decode_deletestreamcompleted(Data),
    case Dto#deletestreamcompleted.result of
        'Success' -> {complete, ok};
        Other -> decode_write_failure(Other)
    end;

deserialize_result(read_event, read_event_completed, Data) ->
    Dto = erles_clientapi_pb:decode_readeventcompleted(Data),
    case Dto#readeventcompleted.result of
        'Success' ->       {complete, {ok, erles_utils:resolved_event(stream, Dto#readeventcompleted.event)}};
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
        'WrongExpectedVersion' -> {complete, {error, wrong_exp_ver}};
        'StreamDeleted' ->        {complete, {error, stream_deleted}};
        'InvalidTransaction' ->   {complete, {error, invalid_transaction}};
        'AccessDenied' ->         {complete, {error, access_denied}}
    end.

deserialize_streameventscompleted(Data) ->
    Dto = erles_clientapi_pb:decode_readstreameventscompleted(Data),
    case Dto#readstreameventscompleted.result of
        'Success' ->       {complete, {ok,
            [erles_utils:resolved_event(stream, E) || E <- Dto#readstreameventscompleted.events],
            Dto#readstreameventscompleted.next_event_number,
            Dto#readstreameventscompleted.is_end_of_stream
        }};
        'NoStream' ->      {complete, {error, no_stream}};
        'StreamDeleted' -> {complete, {error, stream_deleted}};
        'Error' ->         {complete, {error, Dto#readstreameventscompleted.error}};
        'AccessDenied' ->  {complete, {error, access_denied}}
    end.

deserialize_alleventscompleted(Data) ->
    Dto = erles_clientapi_pb:decode_readalleventscompleted(Data),
    case Dto#readalleventscompleted.result of
        'Success' ->       {complete, {ok,
            [erles_utils:resolved_event(all, E) || E <- Dto#readalleventscompleted.events],
            {tfpos, Dto#readalleventscompleted.next_commit_position, Dto#readalleventscompleted.next_prepare_position},
            Dto#readalleventscompleted.events =:= []
        }};
        'Error' ->         {complete, {error, Dto#readalleventscompleted.error}};
        'AccessDenied' ->  {complete, {error, access_denied}}
    end.
