#!/usr/bin/env escript

main([ProtoFile, OutputSrcDir, OutputIncludeDir]) ->
    ProtobuffsDir = filename:dirname(escript:script_name()) ++ "/../deps/protobuffs/ebin",
    true = code:add_pathz(ProtobuffsDir),
    protobuffs_compile:generate_source(ProtoFile,
                                       [{output_src_dir, OutputSrcDir},
                                        {output_include_dir, OutputIncludeDir}]);

main(Args) ->
    io:format("Usage: ~s <protofile> <outputdir-src-dir> <output-include-dir>~n",
              [filename:basename(escript:script_name())]),
    io:format("Args: ~p~n", [Args]).
