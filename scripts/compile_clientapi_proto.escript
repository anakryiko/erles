#!/usr/bin/env escript

main([]) ->
    ScriptDir = filename:dirname(escript:script_name()),
    true = code:add_pathz(ScriptDir ++ "/../deps/protobuffs/ebin"),
    ProtoFile        = ScriptDir ++ "/../include/erles_clientapi.proto",
    OutputSrcDir     = ScriptDir ++ "/../src/",
    OutputIncludeDir = ScriptDir ++ "/../include/",
    protobuffs_compile:generate_source(ProtoFile,
                                       [{output_src_dir, OutputSrcDir},
                                        {output_include_dir, OutputIncludeDir}]);

main(Args) ->
    io:format("Usage: ~s~n", [filename:basename(escript:script_name())]).
