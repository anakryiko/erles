#!/usr/bin/env escript

main([ProtoFile, OutputSrcDir, OutputIncludeDir]) ->
    protobuffs_compile:generate_source(ProtoFile,
                                       [{output_src_dir, OutputSrcDir},
                                        {output_include_dir, OutputIncludeDir}]);
    
main(Args) ->
    io:format("usage: ~s <protofile> <outputdir-src-dir> <output-include-dir>~n",
              [filename:basename (escript:script_name())]),
    io:format("args: ~p~n", [Args]).
