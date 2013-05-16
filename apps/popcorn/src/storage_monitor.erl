%%%
%%% Copyright 2012
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%


%%%-------------------------------------------------------------------
%%% File:      storage_monitor.erl
%%% @author    Marc Campbell <marc.e.campbell@gmail.com>
%%% @author    Martin Hald <mhald@mac.com>
%%% @doc
%%% @end
%%%-----------------------------------------------------------------

-module(storage_monitor).
-author('marc.e.campbell@gmail.com').
-author('mhald@mac.com').
-behavior(gen_server).

-include("include/popcorn.hrl").

-define(WORKER_HEALTH_INTERVAL, 10000).

-export([start_link/0,
         start_workers/0,
         monitor_storage/1,
         get_storage_pid/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% TODO: use ETS to store workers with inherit rights to recover from self() death
-record(state, {workers = []}).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
start_workers() -> gen_server:cast(?MODULE, start_workers).

init([]) ->
    process_flag(trap_exit, true),

    ?POPCORN_DEBUG_MSG("#storage_monitor starting"),
    Workers = pg2:get_local_members(storage),
    ?POPCORN_INFO_MSG("Monitoring ~p", [Workers]),
    [spawn(?MODULE, monitor_storage, [Proc]) || Proc <- Workers],

    erlang:send_after(?WORKER_HEALTH_INTERVAL, self(), check_worker_health),

    {ok, #state{workers = Workers}}.

handle_call(Request, _From, State)  -> {stop, {unknown_call, Request}, State}.
handle_cast(_Msg, State)            -> {noreply, State}.

handle_info(check_worker_health, #state{workers = Workers} = State) ->
    Current_Workers = pg2:get_local_members(storage),
    Workers =/= Current_Workers andalso 
        begin
            publish_worker_change(Current_Workers),
            monitor_new_pids(Workers, Current_Workers)
        end,
    erlang:send_after(?WORKER_HEALTH_INTERVAL, self(), check_worker_health),
    {noreply, State#state{workers = Current_Workers}};

handle_info(_Msg, State)            -> {noreply, State}.

terminate(_Reason, _State)          -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

publish_worker_change(Workers) ->
    ?POPCORN_INFO_MSG("Workers changed, broadcasting new workers: ~p~n", [Workers]),
    pubsub:publish(storage, {new_storage_workers, Workers}).

monitor_storage(Proc) ->
  erlang:monitor(process,Proc),
  receive
    {'DOWN', Ref, process, Pid,  normal} ->
      ok;
    {'DOWN', Ref, process, Pid,  Reason} ->
      Workers = pg2:get_local_members(storage) -- [Proc],
      ?POPCORN_INFO_MSG("Detected down storage node - published reduced set of workers ~p", [Workers]),
      pubsub:publish(storage, {new_storage_workers, Workers})
  end.

monitor_new_pids(Known_Workers, Current_Workers) ->
  New_Workers = Current_Workers -- Known_Workers,
  [spawn(?MODULE, monitor_storage, [Proc]) || Proc <- New_Workers].

get_storage_pid([]) ->
  ?STORAGE_PID;
get_storage_pid(List) ->
  Index = random:uniform(length(List)),
  lists:nth(Index, List).
