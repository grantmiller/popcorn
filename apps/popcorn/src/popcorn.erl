-module(popcorn).

-export([
         start/0,
         stop/0,
         test/0
        ]).

-define(SERVER, popcorn_server).

%% @spec start() -> ok
%% @doc Start the popcorn server.
start() 			-> [application:start(App) || App <-
										[ranch, cowboy, mustache,
										 protobuffs, inets, eleveldb,
										 webmachine, folsom, rps,
                     flake, popcorn
										 ]].

%% @spec stop() -> ok
%% @doc Stop the popcorn server.
stop() ->
    application:stop(popcorn).

%% @spec test() -> ok
%% @doc Test the popcorn server.
test() ->
    [eunit:test(Module) || Module <-
       [node_fsm_tests, popcorn_udp_tests]].
