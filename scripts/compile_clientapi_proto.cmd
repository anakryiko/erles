set CURDIR=%CD%

cd %~dp0..\deps\protobuffs\ebin
escript %~dp0compile_proto.escript %~dp0..\include\erlesque_clientapi.proto %~dp0..\src\ %~dp0..\include\

chdir /d %CURDIR%
