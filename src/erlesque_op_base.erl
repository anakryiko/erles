-module(erlesque_op_base).
-export([start_link/3]).

-include("../include/erlesque_clientapi_pb.hrl").

start_link(ReqCmd, SysParams, OpParams) ->
    Pid = spawn_link(fun() -> do_request(ReqCmd, SysParams, OpParams, 5) end),
    {ok, Pid}.

do_request(_ReqCmd, {CorrId, ClientPid, _ConnPid}, _OpParams, 0) ->
    respond(ClientPid, CorrId, {error, retries_limit});

do_request(ReqCmd, SysParams={CorrId, ClientPid, ConnPid}, OpParams, TrialsLeft) ->
    Pkg = create_package(CorrId, ReqCmd, OpParams),
    erlesque_conn:send(ConnPid, Pkg),
    handle_message(ReqCmd, SysParams, OpParams, TrialsLeft).

handle_message(ReqCmd, SysParams={CorrId, ClientPid, ConnPid}, OpParams, TrialsLeft) ->
    ReqTimeout = 3000,
    receive
        {pkg, Cmd, CorrId, noauth, Data} ->
            handle_pkg({Cmd, Data}, ReqCmd, SysParams, OpParams, TrialsLeft);
        pause ->
            wait_for_connection(ReqCmd, SysParams, OpParams, TrialsLeft);
        abort ->
            respond(ClientPid, CorrId, {error, aborted});
        Msg ->
            io:format("Unexpected message received: ~p.~n", [Msg])
        after ReqTimeout ->
            ClientPid ! {op_completed, CorrId, {error, timedout}}
    end.

handle_pkg({Cmd, Data}, ReqCmd, SysParams={CorrId, ClientPid, ConnPid}, OpParams, TrialsLeft) ->
    RespCmd = response_cmd(ReqCmd),
    case Cmd of
        RespCmd ->
            Result = deserialize_result(ReqCmd, RespCmd, Data),
            respond(ClientPid, CorrId, {ok, Result});
%        not_handled ->
%            handle_not_handled(CorrId);
        not_authenticated ->
            respond(ClientPid, CorrId, {not_authenticated, Data});
        bad_request ->
            respond(ClientPid, CorrId, {bad_request, Data});
        _ ->
            io:format("Unexpected command received: ~p, data: ~p.~n", [Cmd, Data]),
            handle_message(ReqCmd, SysParams, OpParams, TrialsLeft)
    end.

wait_for_connection(ReqCmd, SysParams={CorrId, ClientPid, ConnPid}, OpParams, TrialsLeft) ->
    receive
        {pkg, Cmd, CorrId, noauth, Data} ->
            handle_pkg({Cmd, Data}, ReqCmd, SysParams, OpParams, TrialsLeft);
        restart ->
            NewCorrId = erlesque_utils:create_uuid_v4(),
            ClientPid ! {op_restarted, CorrId, NewCorrId},
            do_request(ReqCmd, {NewCorrId, ClientPid, ConnPid}, OpParams, TrialsLeft-1);
        abort ->
            respond(ClientPid, CorrId, {error, aborted});
        Msg ->
            io:format("Unexpected message received: ~p.~n", [Msg])
    end.

%handle_not_handled(CorrId, Data) ->
%    Dto = decode_nothandled(Data),

respond(ClientPid, CorrId, Result) ->
    ClientPid ! {op_completed, CorrId, Result}.

create_package(CorrId, write_events, {StreamId, ExpectedVersion}) ->
    Dto = #writeevents{
        event_stream_id = StreamId,
        expected_version = ExpectedVersion,
        require_master = false,
        events = [#newevent{
            event_id = erlesque_utils:create_uuid_v4(),
            event_type = <<"dummy_event_type">>,
            data_content_type = 1,
            metadata_content_type = 1,
            data = <<"{\"content\": \"Hello from Erlang!\"}">>
        }]},
    Bin = erlesque_clientapi_pb:encode_writeevents(Dto),
    erlesque_pkg:create(write_events, CorrId, Bin);

create_package(CorrId, transaction_start, OpParams) ->           erlang:error(not_implemented);
create_package(CorrId, transaction_write, OpParams) ->           erlang:error(not_implemented);
create_package(CorrId, transaction_commit, OpParams) ->          erlang:error(not_implemented);
create_package(CorrId, delete_stream, OpParams) ->               erlang:error(not_implemented);
create_package(CorrId, read_event, OpParams) ->                  erlang:error(not_implemented);
create_package(CorrId, read_stream_events_forward, OpParams) ->  erlang:error(not_implemented);
create_package(CorrId, read_stream_events_backward, OpParams) -> erlang:error(not_implemented);
create_package(CorrId, read_all_events_forward, OpParams) ->     erlang:error(not_implemented);
create_package(CorrId, read_all_events_backward, OpParams) ->    erlang:error(not_implemented).

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

deserialize_result(write_events, write_events_completed, Data) ->
    erlesque_clientapi_pb:decode_writeeventscompleted(Data);
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
