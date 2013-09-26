REBAR=`which rebar || printf ./rebar`

all:
    get-deps compile

get-deps:
    @$(REBAR) get-deps

proto:
    get-deps
    compile
    escript compile_clientapi_proto.escript

compile:
    @$(REBAR) compile

clean:
    @$(REBAR) clean

ct:
    @$(REBAR) skip_deps=true ct

eunit:
    @$(REBAR) skip_deps=true eunit

test: eunit ct

