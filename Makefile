REBAR=`which rebar || printf ./rebar`

all:
    get-deps proto compile

get-deps:
    @$(REBAR) get-deps

proto:
    get-deps
    compile
    cd deps/protobuffs/ebin
    ../../../scripts/compile_proto.escript ../../../include/clientapi.proto ../../../src ../../../include/
    cd ../../../

compile:
    @$(REBAR) compile

clean:
    @$(REBAR) clean
ct:
    ./scripts/generate_emakefile.escript
    @$(REBAR) skip_deps=true ct

eunit:
    @$(REBAR) skip_deps=true eunit

test: eunit ct

