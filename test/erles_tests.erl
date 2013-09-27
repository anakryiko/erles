-module(erles_tests).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include("erles.hrl").
-include("erles_internal.hrl").

-define(CONNECT_DEST, {{127,0,0,1}, 1113}).

erles_test_() ->
    [
     {foreach, fun setup/0, fun teardown/1,
      [{with, [T]} || T <- [fun append_any_works_always/1,
                            fun append_expver_works_if_correct/1,
                            fun append_empty_works/1,
                            fun perm_delete_expver_works/1,
                            fun perm_delete_any/1,
                            fun soft_delete_expver_works/1,
                            fun soft_delete_any/1,
                            fun transaction_expver/1,
                            fun transaction_any/1,
                            fun transaction_empty/1,
                            fun read_event/1,
                            fun read_stream/1,
                            fun prim_subscriptions/1,
                            fun perm_subscription/1,
                            fun append_security_works/1,
                            fun transaction_start_security_works/1,
                            fun transaction_commit_security_works/1,
                            fun delete_security_works/1,
                            fun read_event_security_works/1,
                            fun read_stream_security_works/1,
                            fun read_all_security_works/1,
                            fun subscription_security_works/1,
                            fun metadata_raw_get_set_works/1,
                            fun metadata_raw_get_unexisting/1,
                            fun metadata_raw_setting_empty_works/1,
                            fun metadata_raw_setting_with_wrong_expver_fails/1,
                            fun metadata_raw_setting_for_deleted_stream_fails/1,
                            fun metadata_raw_getting_for_deleted_stream_fails/1,
                            fun metadata_raw_returns_last_meta/1,
                            fun metadata_struct_set_get_empty_works/1,
                            fun metadata_struct_get_unexisting_returns_empty/1,
                            fun metadata_struct_get_nonjson_fails/1,
                            fun metadata_struct_get_incomplete_json_fails/1,
                            fun metadata_struct_set_raw_read_struct/1,
                            fun metadata_struct_set_struct_read_struct/1,
                            fun metadata_struct_set_struct_read_raw/1,
                            fun metadata_struct_set_empty_acl_works/1
                         ]] ++ [
                            %fun read_all_forward/1,
                            %fun read_all_backward/1
                         ]}
    ].

setup() ->
    %% here we assume that node on default IP and port is already started
    {ok, C} = erles:connect(node, ?CONNECT_DEST),
    C.

teardown(C) ->
    ok = erles:close(C).

append_any_works_always(C) ->
    Stream = gen_stream_id(),
    ?assertEqual({ok, 0}, erles:append(C, Stream, any, [create_event()])),
    ?assertEqual({ok, 1}, erles:append(C, Stream, -2, [create_event()])).

append_expver_works_if_correct(C) ->
    Stream = gen_stream_id(),
    ?assertEqual({ok, 0}, erles:append(C, Stream, -1, [create_event()])),
    ?assertEqual({ok, 1}, erles:append(C, Stream,  0, [create_event()])),
    ?assertEqual({error, wrong_exp_ver}, erles:append(C, Stream, -1, [create_event()])).

append_empty_works(C) ->
    Stream = gen_stream_id(),
    ?assertEqual({ok, -1}, erles:append(C, Stream, any, [])),
    ?assertEqual({ok, -1}, erles:append(C, Stream, -1, [])).

perm_delete_any(C) ->
    Stream = gen_stream_id(),
    {ok, _} = erles:append(C, Stream, any, [create_event(), create_event()]),
    ?assertEqual(ok, erles:delete(C, Stream, any, perm)),
    ?assertEqual({error, stream_deleted}, erles:read_event(C, Stream, last)).

perm_delete_expver_works(C) ->
    Stream = gen_stream_id(),
    {ok, NextExpVer} = erles:append(C, Stream, any, [create_event(), create_event()]),
    ?assertEqual({error, wrong_exp_ver}, erles:delete(C, Stream, -1, perm)),
    ?assertEqual(ok, erles:delete(C, Stream, NextExpVer, perm)),
    ?assertEqual({error, stream_deleted}, erles:read_event(C, Stream, last)).

soft_delete_any(C) ->
    Stream = gen_stream_id(),
    {ok, _} = erles:append(C, Stream, any, [create_event(), create_event()]),
    ?assertEqual(ok, erles:delete(C, Stream, any)),
    ?assertEqual({error, no_stream}, erles:read_event(C, Stream, last)).

soft_delete_expver_works(C) ->
    Stream = gen_stream_id(),
    {ok, NextExpVer} = erles:append(C, Stream, any, [create_event(), create_event()]),
    ?assertEqual({error, wrong_exp_ver}, erles:delete(C, Stream, -1)),
    ?assertEqual(ok, erles:delete(C, Stream, NextExpVer)),
    ?assertEqual({error, no_stream}, erles:read_event(C, Stream, last)).

transaction_expver(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erles:txn_start(C, Stream, -1),
    ?assertEqual(ok, erles:txn_append(C, Tid, [create_event(), create_event()])),
    ?assertEqual(ok, erles:txn_append(C, Tid, [create_event()])),
    ?assertEqual(ok, erles:txn_append(C, Tid, [])),
    ?assertEqual({ok, 2}, erles:txn_commit(C, Tid)).

transaction_any(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erles:txn_start(C, Stream, any),
    ?assertEqual(ok, erles:txn_append(C, Tid, [create_event(), create_event()])),
    ?assertEqual(ok, erles:txn_append(C, Tid, [create_event()])),
    ?assertEqual(ok, erles:txn_append(C, Tid, [])),
    ?assertEqual({ok, 2}, erles:txn_commit(C, Tid)).

transaction_empty(C) ->
    Stream = gen_stream_id(),
    {ok, Tid} = erles:txn_start(C, Stream, any),
    ?assertEqual({ok, -1}, erles:txn_commit(C, Tid)).

read_event(C) ->
    S = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    RE1 = map_event(S, 0, E1),
    RE2 = map_event(S, 1, E2),
    RE3 = map_event(S, 2, E3),
    {ok, 2} = erles:append(C, S, -1, [E1, E2, E3]),
    ?assertEqual({ok, RE1}, erles:read_event(C, S, 0)),
    ?assertEqual({ok, RE2}, erles:read_event(C, S, 1)),
    ?assertEqual({ok, RE3}, erles:read_event(C, S, 2)),
    ?assertEqual({ok, RE3}, erles:read_event(C, S, last)),
    ?assertEqual({ok, RE3}, erles:read_event(C, S, -1)).

read_stream(C) ->
    S = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    RE1 = map_event(S, 0, E1),
    RE2 = map_event(S, 1, E2),
    RE3 = map_event(S, 2, E3),
    {ok, 2} = erles:append(C, S, -1, [E1, E2, E3]),
    ?assertEqual({ok, [RE1], 1, false},           erles:read_stream(C, S, 0, 1)),
    ?assertEqual({ok, [RE2], 2, false},           erles:read_stream(C, S, 1, 1)),
    ?assertEqual({ok, [RE3], 3, true},            erles:read_stream(C, S, 2, 1)),
    ?assertEqual({ok, [], 3, true},               erles:read_stream(C, S, 3, 1)),
    ?assertEqual({ok, [RE1, RE2], 2, false},      erles:read_stream(C, S, 0, 2)),
    ?assertEqual({ok, [RE2, RE3], 3, true},       erles:read_stream(C, S, 1, 2)),
    ?assertEqual({ok, [RE3], 3, true},            erles:read_stream(C, S, 2, 2)),
    ?assertEqual({ok, [], 3, true},               erles:read_stream(C, S, 3, 2)),
    ?assertEqual({ok, [RE1], -1, true},           erles:read_stream(C, S, 0, 1, backward)),
    ?assertEqual({ok, [RE2], 0, false},           erles:read_stream(C, S, 1, 1, backward)),
    ?assertEqual({ok, [RE3], 1, false},           erles:read_stream(C, S, 2, 1, backward)),
    ?assertEqual({ok, [], 2, false},              erles:read_stream(C, S, 3, 1, backward)),
    ?assertEqual({ok, [RE1], -1, true},           erles:read_stream(C, S, 0, 2, backward)),
    ?assertEqual({ok, [RE2, RE1], -1, true},      erles:read_stream(C, S, 1, 2, backward)),
    ?assertEqual({ok, [RE3, RE2], 0, false},      erles:read_stream(C, S, 2, 2, backward)),
    ?assertEqual({ok, [RE3], 1, false},           erles:read_stream(C, S, 3, 2, backward)),
    ?assertEqual({ok, [RE3, RE2, RE1], -1, true}, erles:read_stream(C, S, 5, 100, backward)),
    ?assertEqual({ok, [RE3, RE2], 0, false},      erles:read_stream(C, S, last, 2, backward)),
    ?assertEqual({ok, [RE3, RE2], 0, false},      erles:read_stream(C, S, -1, 2, backward)).

read_all_forward(C) ->
    S = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    RE1 = map_event(S, 0, E1),
    RE2 = map_event(S, 1, E2),
    RE3 = map_event(S, 2, E3),
    {ok, 2} = erles:append(C, S, -1, [E1, E2, E3]),
    {timeout, 100, ?_assertEqual([RE1, RE2, RE3], read_all_and_filter(C, S, first, forward))}.

read_all_backward(C) ->
    S = gen_stream_id(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    RE1 = map_event(S, 0, E1),
    RE2 = map_event(S, 1, E2),
    RE3 = map_event(S, 2, E3),
    {ok, 2} = erles:append(C, S, -1, [E1, E2, E3]),
    {timeout, 100, ?_assertEqual([RE3, RE2, RE1], read_all_and_filter(C, S, last, backward))}.

read_all_and_filter(C, S, From, Dir) ->
    erlang:display({read_all_and_filter, S, From, Dir}),
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


prim_subscriptions(C) ->
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
    ListPid1 = create_listener([RE1, RE2, RE3]),
    ListPid2 = create_listener([RE4, RE5]),
    {ok, SubPid1, _SubPos1} = erles:subscribe(C, S1, [{subscriber, ListPid1}]),
    {ok, SubPid2, _SubPos2} = erles:subscribe(C, S2, [{subscriber, ListPid2}]),
    {ok, 0} = erles:append(C, S1, -1, [E1]),
    {ok, 0} = erles:append(C, S2, -1, [E4]),
    {ok, 1} = erles:append(C, S1, 0, [E2]),
    {ok, 1} = erles:append(C, S2, 0, [E5]),
    {ok, 2} = erles:append(C, S1, 1, [E3]),
    SubRes1 = receive
                  {done, ListPid1, Res1} -> Res1
                  after 5000 -> listener1_timeout
              end,
    SubRes2 = receive
                  {done, ListPid2, Res2} -> Res2
                  after 5000 -> listener2_timeout
              end,
    ?assertEqual(ok, SubRes1),
    ?assertEqual(ok, SubRes2),
    ?assertEqual(ok, erles:unsubscribe(SubPid1)),
    ?assertEqual(ok, erles:unsubscribe(SubPid2)),
    ListPid1 ! stop,
    ListPid2 ! stop.

perm_subscription(C) ->
    S = gen_stream_id(),
    E0 = create_event(),
    E1 = create_event(),
    E2 = create_event(),
    E3 = create_event(),
    E4 = create_event(),
    _RE0 = map_event(S, 0, E0),
    RE1 = map_event(S, 1, E1),
    RE2 = map_event(S, 2, E2),
    RE3 = map_event(S, 3, E3),
    RE4 = map_event(S, 4, E4),
    LiveListPid = create_listener([RE3, RE4]),
    InclListPid = create_listener([RE1, RE2, RE3, RE4]),
    ExclListPid = create_listener([RE2, RE3, RE4]),
    {ok, 0} = erles:append(C, S, any, [E0]),
    {ok, 1} = erles:append(C, S, any, [E1]),
    {ok, 2} = erles:append(C, S, any, [E2]),
    {ok, LiveSubPid} = erles:subscribe_perm(C, S, live, [{subscriber, LiveListPid}]),
    {ok, InclSubPid} = erles:subscribe_perm(C, S, {inclusive, 1}, [{subscriber, InclListPid}]),
    {ok, ExclSubPid} = erles:subscribe_perm(C, S, {exclusive, 1}, [{subscriber, ExclListPid}]),
    {ok, 3} = erles:append(C, S, any, [E3]),
    {ok, 4} = erles:append(C, S, any, [E4]),
    LiveSubRes = receive
                     {done, LiveListPid, LiveRes} -> LiveRes
                     after 5000 -> live_listener_timeout
                  end,
    InclSubRes = receive
                     {done, InclListPid, InclRes} -> InclRes
                     after 5000 -> incl_listener_timeout
                  end,
    ExclSubRes = receive
                     {done, ExclListPid, ExclRes} -> ExclRes
                     after 5000 -> excl_listener_timeout
                  end,
    ?assertEqual(ok, LiveSubRes),
    ?assertEqual(ok, InclSubRes),
    ?assertEqual(ok, ExclSubRes),
    ?assertEqual(ok, erles:unsubscribe_perm(LiveSubPid)),
    ?assertEqual(ok, erles:unsubscribe_perm(InclSubPid)),
    ?assertEqual(ok, erles:unsubscribe_perm(ExclSubPid)),
    LiveListPid ! stop,
    InclListPid ! stop,
    ExclListPid ! stop.

create_listener(ExpectedEvents) ->
    SelfPid = self(),
    spawn_link(fun() -> listener(ExpectedEvents, SelfPid) end).

listener([], RespPid) ->
    RespPid ! {done, self(), ok},
    receive
        stop -> ok;
        {unsubscribed, _SubPid, requested_by_client} -> listener([], RespPid);
        Unexpected -> erlang:error({listener_unexpected_msg, self(), Unexpected})
    after 5000 ->
        erlang:error({listener_timed_out, self()})
    end;

listener([CurEvent | EventsToGo], RespPid) ->
    receive
        {event, _SubPid, CurEvent} -> listener(EventsToGo, RespPid);
        Unexpected ->
            io:format("~100P ~n ~100P ~n", [Unexpected, 500, CurEvent, 500]),
            RespPid ! {done, self(), {unexpected, Unexpected, expected, CurEvent}}
    end.


append_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{write_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    ?assertEqual({error, access_denied}, erles:append(C, S, any, [E1])),
    ?assertEqual({ok, 0},                erles:append(C, S, any, [E1], Opts)).

transaction_start_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{write_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    ?assertEqual({error, access_denied}, erles:txn_start(C, S, any)),
    ?assertMatch({ok, _T},               erles:txn_start(C, S, any, Opts)).

transaction_commit_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{write_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    {ok, T1} =                           erles:txn_start(C, S, any, Opts),
    ?assertEqual(ok,                     erles:txn_append(C, T1, [E1])),
    ?assertEqual({error, access_denied}, erles:txn_commit(C, T1)),
    {ok, T2} =                           erles:txn_start(C, S, any, Opts),
    ?assertEqual(ok,                     erles:txn_append(C, T2, [E1])),
    ?assertEqual({ok, 0},                erles:txn_commit(C, T2, Opts)).

delete_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{write_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    ?assertEqual({error, access_denied}, erles:append(C, S, any, [E1])),
    ?assertEqual({ok, 0}, erles:append(C, S, any, [E1], Opts)).

read_event_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{read_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    RE1 = map_event(S, 0, E1),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    {ok, 0} = erles:append(C, S, any, [E1], Opts),
    ?assertEqual({error, access_denied}, erles:read_event(C, S, first)),
    ?assertEqual({ok, RE1},              erles:read_event(C, S, first, Opts)).

read_stream_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{read_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    RE1 = map_event(S, 0, E1),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    {ok, 0} = erles:append(C, S, any, [E1], Opts),
    ?assertEqual({error, access_denied},            erles:read_stream(C, S, first, 10)),
    ?assertEqual({error, access_denied},            erles:read_stream(C, S, last, 10, backward)),
    ?assertEqual({ok, [RE1], 1, true},  erles:read_stream(C, S, first, 10, forward, Opts)),
    ?assertEqual({ok, [RE1], -1, true}, erles:read_stream(C, S, last, 10, backward, Opts)).

read_all_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{read_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    {ok, 0} = erles:append(C, S, any, [E1], Opts),
    ?assertEqual({error, access_denied},           erles:read_stream(C, all, first, 10)),
    ?assertEqual({error, access_denied},           erles:read_stream(C, all, last, 10, backward)),
    ?assertMatch({ok, _, _, _},                    erles:read_stream(C, all, first, 10, forward, Opts)),
    ?assertMatch({ok, _, _, _},                    erles:read_stream(C, all, last, 10, backward, Opts)).

subscription_security_works(C) ->
    S = gen_stream_id(),
    Meta = #stream_meta{acl=#stream_acl{read_roles=[?ROLE_ADMINS]}},
    {ok, _} = erles:set_metadata(C, S, any, Meta),
    E1 = create_event(),
    Opts = [{auth, {"admin", <<"changeit">>}}],
    {ok, 0} = erles:append(C, S, any, [E1], Opts),
    ?assertEqual({error, access_denied}, erles:subscribe(C, S)),
    {ok, SubscrPid, _} =                 erles:subscribe(C, S, Opts),
    ?assertEqual(ok,                     erles:unsubscribe(SubscrPid)).


metadata_raw_get_set_works(C) ->
    S = gen_stream_id(),
    Bin = erles_utils:gen_uuid(),
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, Bin)),
    ?assertEqual({ok, {meta, Bin, 0}}, erles:get_metadata(C, S, raw)),
    ?assertMatch({ok, {event, #event{data=Bin}, 0}}, erles:read_event(C, <<"$$", S/binary>>, last)).

metadata_raw_get_unexisting(C) ->
    S = gen_stream_id(),
    ?assertEqual({ok, {meta, <<>>, -1}}, erles:get_metadata(C, S, raw)).

metadata_raw_setting_empty_works(C) ->
    S = gen_stream_id(),
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, <<>>)),
    ?assertEqual({ok, {meta, <<>>, 0}}, erles:get_metadata(C, S, raw)).

metadata_raw_setting_with_wrong_expver_fails(C) ->
    S = gen_stream_id(),
    ?assertEqual({error, wrong_exp_ver}, erles:set_metadata(C, S, 1, <<>>)),
    ?assertEqual({ok, {meta, <<>>, -1}}, erles:get_metadata(C, S, raw)).

metadata_raw_setting_for_deleted_stream_fails(C) ->
    S = gen_stream_id(),
    ?assertEqual(ok, erles:delete(C, S, any, perm)),
    ?assertEqual({error, stream_deleted}, erles:set_metadata(C, S, any, <<>>)).

metadata_raw_getting_for_deleted_stream_fails(C) ->
    S = gen_stream_id(),
    ?assertEqual(ok, erles:delete(C, S, any, perm)),
    ?assertEqual({error, stream_deleted}, erles:get_metadata(C, S, raw)).

metadata_raw_returns_last_meta(C) ->
    S = gen_stream_id(),
    Bin1 = erles_utils:gen_uuid(),
    Bin2 = erles_utils:gen_uuid(),
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, Bin1)),
    ?assertEqual({ok, 1}, erles:set_metadata(C, S, 0, Bin2)),
    ?assertEqual({ok, {meta, Bin2, 1}}, erles:get_metadata(C, S, raw)).


metadata_struct_set_get_empty_works(C) ->
    S = gen_stream_id(),
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, #stream_meta{})),
    ?assertEqual({ok, {meta, <<"{}">>, 0}}, erles:get_metadata(C, S, raw)),
    ?assertEqual({ok, {meta, #stream_meta{}, 0}}, erles:get_metadata(C, S, struct)).

metadata_struct_get_unexisting_returns_empty(C) ->
    S = gen_stream_id(),
    ?assertEqual({ok, {meta, #stream_meta{}, -1}}, erles:get_metadata(C, S, struct)).

metadata_struct_get_nonjson_fails(C) ->
    S = gen_stream_id(),
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, <<"}abracadabra{">>)),
    ?assertEqual({error, bad_json}, erles:get_metadata(C, S, struct)).

metadata_struct_get_incomplete_json_fails(C) ->
    S = gen_stream_id(),
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, <<"{">>)),
    ?assertEqual({error, bad_json}, erles:get_metadata(C, S, struct)).

metadata_struct_set_raw_read_struct(C) ->
    S = gen_stream_id(),
    MetaBin = struct_meta_as_binary(),
    MetaStruct = struct_meta_expected(),
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaBin, Opts)),
    ?assertEqual({ok, {meta, MetaStruct, 0}}, erles:get_metadata(C, S, struct, Opts)).

metadata_struct_set_struct_read_struct(C) ->
    S = gen_stream_id(),
    MetaStruct = struct_meta_expected(),
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaStruct, Opts)),
    ?assertEqual({ok, {meta, MetaStruct, 0}}, erles:get_metadata(C, S, struct, Opts)).

metadata_struct_set_struct_read_raw(C) ->
    S = gen_stream_id(),
    MetaStruct = struct_meta_expected(),
    MetaBin = struct_meta_canon_binary(),
    Opts = [{auth, {<<"admin">>, <<"changeit">>}}],
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaStruct, Opts)),
    ?assertEqual({ok, {meta, MetaBin, 0}}, erles:get_metadata(C, S, raw, Opts)).

metadata_struct_set_empty_acl_works(C) ->
    S = gen_stream_id(),
    MetaStruct = #stream_meta{acl=#stream_acl{}},
    MetaBin = <<"{\"$acl\":{}}">>,
    ?assertEqual({ok, 0}, erles:set_metadata(C, S, any, MetaStruct)),
    ?assertEqual({ok, {meta, MetaStruct, 0}}, erles:get_metadata(C, S, struct)),
    ?assertEqual({ok, {meta, MetaBin, 0}}, erles:get_metadata(C, S, raw)).

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
