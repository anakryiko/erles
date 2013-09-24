-module(erles_req_tests).
-include_lib("eunit/include/eunit.hrl").
-include("erles.hrl").

-compile(export_all).

reqs_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     fun(D) ->
        [append_any(D),
         append_expver(D),
         append_empty(D),

         delete_expver(D),
         delete_any(D),

         transaction_expver(D),
         transaction_any(D),
         transaction_empty(D),

         reads(D),
         subscriptions(D),

         metadata_raw_get_set_works(D),
         metadata_raw_get_unexisting(D),
         metadata_raw_setting_empty_works(D),
         metadata_raw_setting_with_wrong_expver_fails(D),
         metadata_raw_setting_for_deleted_stream_fails(D),
         metadata_raw_getting_for_deleted_stream_fails(D),
         metadata_raw_returns_last_meta(D),

         metadata_struct_set_get_empty_works(D),
         metadata_struct_get_unexisting_returns_empty(D),
         metadata_struct_get_nonjson_fails(D),
         metadata_struct_get_incomplete_json_fails(D),
         metadata_struct_set_raw_read_struct(D),
         metadata_struct_set_struct_read_struct(D),
         metadata_struct_set_struct_read_raw(D),
         metadata_struct_set_empty_acl_works(D)
        ]
     end}.

setup() ->
    %% here we assume that node on default IP and port is already started
    {ok, C} = erles:connect(node, {{127,0,0,1}, 1113}),
    C.

teardown(C) ->
    ok = erles:close(C).

append_any(C) ->
    Stream = gen_stream_id(),
    [?_assertEqual({ok, 0}, erles:append(C, Stream, any, [create_event()])),
     ?_assertEqual({ok, 1}, erles:append(C, Stream, -2, [create_event()]))
    ].

append_expver(C) ->
    Stream = gen_stream_id(),
    [?_assertEqual({ok, 0}, erles:append(C, Stream, -1, [create_event()])),
     ?_assertEqual({ok, 1}, erles:append(C, Stream,  0, [create_event()])),
     ?_assertEqual({error, wrong_expected_version},
                   erles:append(C, Stream, -1, [create_event()]))
    ].

append_empty(C) ->
    Stream = gen_stream_id(),
    [?_assertEqual({ok, -1}, erles:append(C, Stream, any, [])),
     ?_assertEqual({ok, -1}, erles:append(C, Stream, -1, []))
    ].


delete_any(C) ->
    Stream = gen_stream_id(),
    {ok, _} = erles:append(C, Stream, any, [create_event(), create_event()]),
    [?_assertEqual(ok, erles:delete(C, Stream, any)),
     ?_assertEqual({error, stream_deleted}, erles:read_event(C, Stream, last))
    ].

delete_expver(C) ->
    Stream = gen_stream_id(),
    {ok, NextExpVer} = erles:append(C, Stream, any, [create_event(), create_event()]),
    [?_assertEqual(ok, erles:delete(C, Stream, NextExpVer)),
     ?_assertEqual({error, stream_deleted}, erles:read_event(C, Stream, last))
    ].

transaction_expver(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erles:transaction_start(C, Stream, -1),
    [?_assertEqual(ok, erles:transaction_write(C, Tid, [create_event(), create_event()])),
     ?_assertEqual(ok, erles:transaction_write(C, Tid, [create_event()])),
     ?_assertEqual(ok, erles:transaction_write(C, Tid, [])),
     ?_assertEqual({ok, 2}, erles:transaction_commit(C, Tid))
    ].

transaction_any(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erles:transaction_start(C, Stream, any),
    [?_assertEqual(ok, erles:transaction_write(C, Tid, [create_event(), create_event()])),
     ?_assertEqual(ok, erles:transaction_write(C, Tid, [create_event()])),
     ?_assertEqual(ok, erles:transaction_write(C, Tid, [])),
     ?_assertEqual({ok, 2}, erles:transaction_commit(C, Tid))
    ].

transaction_empty(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erles:transaction_start(C, Stream, any),
    ?_assertEqual({ok, -1}, erles:transaction_commit(C, Tid)).

reads(C) ->
    S = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    RE1 = map_event(S, 0, E1),
    RE2 = map_event(S, 1, E2),
    RE3 = map_event(S, 2, E3),
    {ok, 2} = erles:append(C, S, -1, [E1, E2, E3]),
    {inparallel, [
     ?_assertEqual({ok, RE1}, erles:read_event(C, S, 0)),
     ?_assertEqual({ok, RE2}, erles:read_event(C, S, 1)),
     ?_assertEqual({ok, RE3}, erles:read_event(C, S, 2)),
     ?_assertEqual({ok, RE3}, erles:read_event(C, S, last)),
     ?_assertEqual({ok, RE3}, erles:read_event(C, S, -1)),

     ?_assertEqual({ok, [RE1], 1, false},           erles:read_stream(C, S, 0, 1)),
     ?_assertEqual({ok, [RE2], 2, false},           erles:read_stream(C, S, 1, 1)),
     ?_assertEqual({ok, [RE3], 3, true},            erles:read_stream(C, S, 2, 1)),
     ?_assertEqual({ok, [], 3, true},               erles:read_stream(C, S, 3, 1)),
     ?_assertEqual({ok, [RE1, RE2], 2, false},      erles:read_stream(C, S, 0, 2)),
     ?_assertEqual({ok, [RE2, RE3], 3, true},       erles:read_stream(C, S, 1, 2)),
     ?_assertEqual({ok, [RE3], 3, true},            erles:read_stream(C, S, 2, 2)),
     ?_assertEqual({ok, [], 3, true},               erles:read_stream(C, S, 3, 2)),

     ?_assertEqual({ok, [RE1], -1, true},           erles:read_stream(C, S, 0, 1, backward)),
     ?_assertEqual({ok, [RE2], 0, false},           erles:read_stream(C, S, 1, 1, backward)),
     ?_assertEqual({ok, [RE3], 1, false},           erles:read_stream(C, S, 2, 1, backward)),
     ?_assertEqual({ok, [], 2, false},              erles:read_stream(C, S, 3, 1, backward)),
     ?_assertEqual({ok, [RE1], -1, true},           erles:read_stream(C, S, 0, 2, backward)),
     ?_assertEqual({ok, [RE2, RE1], -1, true},      erles:read_stream(C, S, 1, 2, backward)),
     ?_assertEqual({ok, [RE3, RE2], 0, false},      erles:read_stream(C, S, 2, 2, backward)),
     ?_assertEqual({ok, [RE3], 1, false},           erles:read_stream(C, S, 3, 2, backward)),
     ?_assertEqual({ok, [RE3, RE2, RE1], -1, true}, erles:read_stream(C, S, 5, 100, backward)),
     ?_assertEqual({ok, [RE3, RE2], 0, false},      erles:read_stream(C, S, last, 2, backward)),
     ?_assertEqual({ok, [RE3, RE2], 0, false},      erles:read_stream(C, S, -1, 2, backward)),

     {timeout, 100, ?_assertEqual([RE1, RE2, RE3], read_all_and_filter(C, S, first, forward))},
     {timeout, 100, ?_assertEqual([RE3, RE2, RE1], read_all_and_filter(C, S, last, backward))}
    ]}.

read_all_and_filter(C, S, From, Dir) ->
    Res = read_all_and_filter(C, S, From, Dir, []),
    lists:reverse(Res).

read_all_and_filter(C, S, Pos, Dir, Acc) ->
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    {ok, Events, NextPos, EndOfStream} = erles:read_stream(C, all, Pos, 100, Dir, Opts),
    NewAcc = lists:foldl(fun({event, E, _P}, A) ->
        case E#event.stream_id =:= S of
            true -> [{event, E, E#event.event_number} | A];
            false -> A
        end
    end, Acc, Events),
    case EndOfStream of
        false -> read_all_and_filter(C, S, NextPos, Dir, NewAcc);
        true -> NewAcc
    end.


subscriptions(C) ->
    S1 = gen_stream_id(),
    S2 = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    E4 = create_event(),
    E5 = create_event(),
    RE1 = map_event(S1, 0, E1),
    RE2 = map_event(S1, 1, E2),
    RE3 = map_event(S1, 2, E3),
    RE4 = map_event(S2, 0, E4),
    RE5 = map_event(S2, 1, E5),
    {SubscriptionPid1, ListenerPid1} = create_subscriber(C, S1, 3),
    {SubscriptionPid2, ListenerPid2} = create_subscriber(C, S2, 2),
    {ok, 0} = erles:append(C, S1, -1, [E1]),
    {ok, 0} = erles:append(C, S2, -1, [E4]),
    {ok, 1} = erles:append(C, S1, 0, [E2]),
    {ok, 1} = erles:append(C, S2, 0, [E5]),
    {ok, 2} = erles:append(C, S1, 1, [E3]),
    SubRes1 = receive
                  {subscriber_done, ListenerPid1, Res1} -> Res1
                  after 5000 -> subscriber1_timeout
              end,
    SubRes2 = receive
                  {subscriber_done, ListenerPid2, Res2} -> Res2
                  after 5000 -> subscriber2_timeout
              end,
    {inparallel, [
     ?_assertEqual([RE1, RE2, RE3], SubRes1),
     ?_assertEqual([RE4, RE5], SubRes2),
     ?_assertEqual(ok, erles:unsubscribe(SubscriptionPid1)),
     ?_assertEqual(ok, erles:unsubscribe(SubscriptionPid2))
    ]}.

create_subscriber(C, StreamId, EventCount) ->
    SelfPid = self(),
    SubscriberPid = spawn_link(fun() -> subscriber(EventCount, SelfPid, []) end),
    {ok, SubscriptionPid, _SubPos} = erles:subscribe(C, StreamId, [{subscriber, SubscriberPid}]),
    {SubscriptionPid, SubscriberPid}.

subscriber(0, RespPid, Acc) ->
    Res = lists:reverse(Acc),
    RespPid ! {subscriber_done, self(), Res};
subscriber(EventsToGo, RespPid, Acc) ->
    receive
        E={event, _Event, _EventPos} -> subscriber(EventsToGo-1, RespPid, [E | Acc]);
        Unexpected -> erlang:display({unexpected_msg, Unexpected})
    end.

metadata_raw_get_set_works(C) ->
    S = gen_stream_id(),
    Bin = erles_utils:gen_uuid(),
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, Bin)),
     ?_assertEqual({ok, {meta, Bin, 0}}, erles:get_metadata(C, S, raw)),
     ?_assertMatch({ok, {event, #event{data=Bin}, 0}}, erles:read_event(C, <<"$$", S/binary>>, last))
    ].

metadata_raw_get_unexisting(C) ->
    S = gen_stream_id(),
    ?_assertEqual({ok, {meta, <<>>, -1}}, erles:get_metadata(C, S, raw)).

metadata_raw_setting_empty_works(C) ->
    S = gen_stream_id(),
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, <<>>)),
     ?_assertEqual({ok, {meta, <<>>, 0}}, erles:get_metadata(C, S, raw))
    ].

metadata_raw_setting_with_wrong_expver_fails(C) ->
    S = gen_stream_id(),
    [?_assertEqual({error, wrong_expected_version}, erles:set_metadata(C, S, 1, <<>>)),
     ?_assertEqual({ok, {meta, <<>>, -1}}, erles:get_metadata(C, S, raw))
    ].

metadata_raw_setting_for_deleted_stream_fails(C) ->
    S = gen_stream_id(),
    [?_assertEqual(ok, erles:delete(C, S, any)),
     ?_assertEqual({error, stream_deleted}, erles:set_metadata(C, S, any, <<>>))
    ].

metadata_raw_getting_for_deleted_stream_fails(C) ->
    S = gen_stream_id(),
    [?_assertEqual(ok, erles:delete(C, S, any)),
     ?_assertEqual({error, stream_deleted}, erles:get_metadata(C, S, raw))
    ].

metadata_raw_returns_last_meta(C) ->
    S = gen_stream_id(),
    Bin1 = erles_utils:gen_uuid(),
    Bin2 = erles_utils:gen_uuid(),
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, Bin1)),
     ?_assertEqual({ok, 1}, erles:set_metadata(C, S, 0, Bin2)),
     ?_assertEqual({ok, {meta, Bin2, 1}}, erles:get_metadata(C, S, raw))
    ].


metadata_struct_set_get_empty_works(C) ->
    S = gen_stream_id(),
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, #stream_meta{})),
     ?_assertEqual({ok, {meta, <<"{}">>, 0}}, erles:get_metadata(C, S, raw)),
     ?_assertEqual({ok, {meta, #stream_meta{}, 0}}, erles:get_metadata(C, S, struct))
    ].

metadata_struct_get_unexisting_returns_empty(C) ->
    S = gen_stream_id(),
    ?_assertEqual({ok, {meta, #stream_meta{}, -1}}, erles:get_metadata(C, S, struct)).

metadata_struct_get_nonjson_fails(C) ->
    S = gen_stream_id(),
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, <<"}abracadabra{">>)),
     ?_assertEqual({error, bad_json}, erles:get_metadata(C, S, struct))
    ].

metadata_struct_get_incomplete_json_fails(C) ->
    S = gen_stream_id(),
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, <<"{">>)),
     ?_assertEqual({error, bad_json}, erles:get_metadata(C, S, struct))
    ].

metadata_struct_set_raw_read_struct(C) ->
    S = gen_stream_id(),
    MetaBin = struct_meta_as_binary(),
    MetaStruct = struct_meta_expected(),
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaBin, Opts)),
     ?_assertEqual({ok, {meta, MetaStruct, 0}}, erles:get_metadata(C, S, struct, Opts))
    ].

metadata_struct_set_struct_read_struct(C) ->
    S = gen_stream_id(),
    MetaStruct = struct_meta_expected(),
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaStruct, Opts)),
     ?_assertEqual({ok, {meta, MetaStruct, 0}}, erles:get_metadata(C, S, struct, Opts))
    ].

metadata_struct_set_struct_read_raw(C) ->
    S = gen_stream_id(),
    MetaStruct = struct_meta_expected(),
    MetaBin = struct_meta_canon_binary(),
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaStruct, Opts)),
     ?_assertEqual({ok, {meta, MetaBin, 0}}, erles:get_metadata(C, S, raw, Opts))
    ].

metadata_struct_set_empty_acl_works(C) ->
    S = gen_stream_id(),
    MetaStruct = #stream_meta{acl=#stream_acl{}},
    MetaBin = <<"{\"$acl\":{}}">>,
    [?_assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaStruct)),
     ?_assertEqual({ok, {meta, MetaStruct, 0}}, erles:get_metadata(C, S, struct)),
     ?_assertEqual({ok, {meta, MetaBin, 0}}, erles:get_metadata(C, S, raw))
    ].

struct_meta_as_binary() ->
    <<"{"
      "  \"$maxCount\": 17,"
      "  \"$maxAge\": 123321,"
      "  \"$tb\": 23,"
      "  \"$cacheControl\": 7654321,"
      "  \"$acl\": {"
      "      \"$r\": \"r\","
      "      \"$w\": [\"w1\", \"w2\"],"
      "      \"$d\": \"d\","
      "      \"$mr\": [\"mr1\", \"mr2\"],"
      "      \"$mw\": \"mw\""
      "  },"
      "  \"customString\": \"some-string\","
      "  \"customInt\": -179,"
      "  \"customDouble\": 1.7,"
      "  \"customLong\": 123123123123123123,"
      "  \"customBool\": true,"
      "  \"customNullable\": null,"
      "  \"customRawJson\": {"
      "      \"subProperty\": 999"
      "  }"
      "}">>.

struct_meta_canon_binary() ->
    B = <<"{"
          "  \"$maxCount\": 17,"
          "  \"$maxAge\": 123321,"
          "  \"$tb\": 23,"
          "  \"$cacheControl\": 7654321,"
          "  \"$acl\": {"
          "      \"$r\": [\"r\"],"
          "      \"$w\": [\"w1\", \"w2\"],"
          "      \"$d\": [\"d\"],"
          "      \"$mr\": [\"mr1\", \"mr2\"],"
          "      \"$mw\": [\"mw\"]"
          "  },"
          "  \"customString\": \"some-string\","
          "  \"customInt\": -179,"
          "  \"customDouble\": 1.7,"
          "  \"customLong\": 123123123123123123,"
          "  \"customBool\": true,"
          "  \"customNullable\": null,"
          "  \"customRawJson\": {"
          "      \"subProperty\": 999"
          "  }"
          "}">>,
    << <<X:8>> || <<X:8>> <= B, X =/= $\s, X =/= $\t, X =/= $\r, X =/= $\n >>.

struct_meta_expected() ->
    #stream_meta{max_count=17,
                 max_age=123321,
                 truncate_before=23,
                 cache_control=7654321,
                 acl=#stream_acl{read_roles=[<<"r">>],
                                 write_roles=[<<"w1">>, <<"w2">>],
                                 delete_roles=[<<"d">>],
                                 metaread_roles=[<<"mr1">>, <<"mr2">>],
                                 metawrite_roles=[<<"mw">>]},
                 custom=[{<<"customString">>, <<"some-string">>},
                         {<<"customInt">>, -179},
                         {<<"customDouble">>, 1.7},
                         {<<"customLong">>, 123123123123123123},
                         {<<"customBool">>, true},
                         {<<"customNullable">>, null},
                         {<<"customRawJson">>, [{<<"subProperty">>, 999}]}
                        ]
                 }.


create_event() ->
    #event_data{event_type = <<"test-type">>,
                data = <<"some data">>,
                metadata = <<"some meta">>}.

map_event(StreamId, EventNumber, EventData) ->
    {event,
     #event{stream_id    = StreamId,
            event_number = EventNumber,
            event_id     = EventData#event_data.event_id,
            event_type   = EventData#event_data.event_type,
            data         = EventData#event_data.data,
            metadata     = EventData#event_data.metadata},
     EventNumber}.

gen_stream_id() ->
    UuidStr = erles_utils:uuid_to_string(erles_utils:gen_uuid()),
    iolist_to_binary(io_lib:format("esq-test-~s", [UuidStr])).
