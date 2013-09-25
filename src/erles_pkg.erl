-module(erles_pkg).
-export([create/3, create/4, from_binary/1, to_binary/1]).

create(Cmd, CorrId, Data) ->
    {pkg, Cmd, CorrId, noauth, Data}.

create(Cmd, CorrId, noauth, Data) ->
    {pkg, Cmd, CorrId, noauth, Data};

create(Cmd, CorrId, {Login, Pass}, Data) ->
    {pkg, Cmd, CorrId, {Login, Pass}, Data}.


from_binary(<<Cmd:8, 0:1, _Flags:7, CorrId:16/binary, Rest/binary>>) ->
    {pkg, decode_cmd(Cmd), CorrId, noauth, Rest};

from_binary(<<Cmd:8, 1:1, _Flags:7, CorrId:16/binary, Rest/binary>>) ->
    case Rest of
        <<LoginLen:8, Login:LoginLen/binary, PassLen:8, Pass:PassLen/binary, Data/binary>> ->
            {pkg, decode_cmd(Cmd), CorrId, {Login, Pass}, Data};
        _ ->
            {error, invalid_auth}
    end;

from_binary(Bin) when is_binary(Bin) ->
    {error, invalid_binary}.


to_binary({pkg, Cmd, CorrId, noauth, Data}) ->
    EncCmd = encode_cmd(Cmd),
    BinData = iolist_to_binary(Data),
    <<EncCmd:8, 0:8, CorrId/binary, BinData/binary>>;

to_binary({pkg, Cmd, CorrId, {Login, Pass}, Data}) ->
    EncCmd = encode_cmd(Cmd),
    LoginLen = byte_size(Login),
    PassLen = byte_size(Pass),
    BinData = iolist_to_binary(Data),
    <<EncCmd:8, 1:8, CorrId/binary,
      LoginLen:8, Login/binary, PassLen:8, Pass/binary, BinData/binary>>.


decode_cmd(16#01) -> heartbeat_req;
decode_cmd(16#02) -> heartbeat_resp;
decode_cmd(16#03) -> ping;
decode_cmd(16#04) -> pong;

decode_cmd(16#82) -> write_events;
decode_cmd(16#83) -> write_events_completed;
decode_cmd(16#84) -> transaction_start;
decode_cmd(16#85) -> transaction_start_completed;
decode_cmd(16#86) -> transaction_write;
decode_cmd(16#87) -> transaction_write_completed;
decode_cmd(16#88) -> transaction_commit;
decode_cmd(16#89) -> transaction_commit_completed;
decode_cmd(16#8A) -> delete_stream;
decode_cmd(16#8B) -> delete_stream_completed;

decode_cmd(16#B0) -> read_event;
decode_cmd(16#B1) -> read_event_completed;
decode_cmd(16#B2) -> read_stream_events_forward;
decode_cmd(16#B3) -> read_stream_events_forward_completed;
decode_cmd(16#B4) -> read_stream_events_backward;
decode_cmd(16#B5) -> read_stream_events_backward_completed;
decode_cmd(16#B6) -> read_all_events_forward;
decode_cmd(16#B7) -> read_all_events_forward_completed;
decode_cmd(16#B8) -> read_all_events_backward;
decode_cmd(16#B9) -> read_all_events_backward_completed;

decode_cmd(16#C0) -> subscribe_to_stream;
decode_cmd(16#C1) -> subscription_confirmation;
decode_cmd(16#C2) -> stream_event_appeared;
decode_cmd(16#C3) -> unsubscribe_from_stream;
decode_cmd(16#C4) -> subscription_dropped;

decode_cmd(16#D0) -> scavenge_database;

decode_cmd(16#F0) -> bad_request;
decode_cmd(16#F1) -> not_handled;
decode_cmd(16#F2) -> authenticate;
decode_cmd(16#F3) -> authenticated;
decode_cmd(16#F4) -> not_authenticated;

decode_cmd(Cmd) when is_integer(Cmd) ->  Cmd.


encode_cmd(heartbeat_req) ->                         16#01;
encode_cmd(heartbeat_resp) ->                        16#02;
encode_cmd(ping) ->                                  16#03;
encode_cmd(pong) ->                                  16#04;

encode_cmd(write_events) ->                          16#82;
encode_cmd(write_events_completed) ->                16#83;
encode_cmd(transaction_start) ->                     16#84;
encode_cmd(transaction_start_completed) ->           16#85;
encode_cmd(transaction_write) ->                     16#86;
encode_cmd(transaction_write_completed) ->           16#87;
encode_cmd(transaction_commit) ->                    16#88;
encode_cmd(transaction_commit_completed) ->          16#89;
encode_cmd(delete_stream) ->                         16#8A;
encode_cmd(delete_stream_completed) ->               16#8B;

encode_cmd(read_event) ->                            16#B0;
encode_cmd(read_event_completed) ->                  16#B1;
encode_cmd(read_stream_events_forward) ->            16#B2;
encode_cmd(read_stream_events_forward_completed) ->  16#B3;
encode_cmd(read_stream_events_backward) ->           16#B4;
encode_cmd(read_stream_events_backward_completed) -> 16#B5;
encode_cmd(read_all_events_forward) ->               16#B6;
encode_cmd(read_all_events_forward_completed) ->     16#B7;
encode_cmd(read_all_events_backward) ->              16#B8;
encode_cmd(read_all_events_backward_completed) ->    16#B9;

encode_cmd(subscribe_to_stream) ->                   16#C0;
encode_cmd(subscription_confirmation) ->             16#C1;
encode_cmd(stream_event_appeared) ->                 16#C2;
encode_cmd(unsubscribe_from_stream) ->               16#C3;
encode_cmd(subscription_dropped) ->                  16#C4;

encode_cmd(scavenge_database) ->                     16#D0;

encode_cmd(bad_request) ->                           16#F0;
encode_cmd(not_handled) ->                           16#F1;
encode_cmd(authenticate) ->                          16#F2;
encode_cmd(authenticated) ->                         16#F3;
encode_cmd(not_authenticated) ->                     16#F4;

encode_cmd(Cmd) when is_integer(Cmd) ->              Cmd.
