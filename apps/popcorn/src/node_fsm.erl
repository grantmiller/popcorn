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
%%% File:      node_fsm.erl
%%% @author    Marc Campbell <marc.e.campbell@gmail.com>
%%% @doc
%%% @end
%%%-----------------------------------------------------------------

%%%
%%% IMPORTANT
%%% ---------
%%%
%%% A node_fsm can be a busy fsm, and making synchronous calls into it is highly
%%% discouraged.  It's better to leave this process alone to collect log messages
%%% and move other reading logic out
%%%

-module(node_fsm).
-author('marc.e.campbell@gmail.com').
-behavior(gen_fsm).

-define(COUNTER_WRITE_INTERVAL, 5000).

-include("include/popcorn.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-export([start_link/0]).

-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export([
    'LOGGING'/2,
    'LOGGING'/3]).

-record(state, {popcorn_node          :: #popcorn_node{},
                event_counter         :: number(),
                workers = []          :: list()}).

start_link() -> gen_fsm:start_link(?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),

    erlang:send_after(?COUNTER_WRITE_INTERVAL, self(), write_counter),
    pubsub:subscribe(storage, ?MODULE, self()),

    {ok, 'LOGGING', #state{event_counter = 0, workers = pg2:get_local_members(storage)}}.

'LOGGING'({log_message, Popcorn_Node, Log_Message, Is_New_Node}, #state{workers = Workers} = State) ->
    try
        %% log the message
        gen_server:cast(?CACHED_STORAGE_PID(Workers), {new_log_message, Log_Message}),

        %% notify the triage handler
        triage_handler:safe_notify(Popcorn_Node, self(), Log_Message, Is_New_Node),

        %% increment the total event counter
        ?INCREMENT_COUNTER(?TOTAL_EVENT_COUNTER),

        %% Notify any streams connected
        log_stream_manager:new_log_message(Log_Message, Popcorn_Node)
    catch
        _:Error ->
            io:format("Couldn't log message:~nMessage: ~p~nNode: ~p~nError: ~p~nStack: ~p~n",
                        [Log_Message, Popcorn_Node, Error, erlang:get_stacktrace()])
    end,
    {next_state, 'LOGGING', State#state{event_counter = State#state.event_counter + 1}}.

'LOGGING'({deserialize_popcorn_node, Popcorn_Node}, _From, State) ->
    ?POPCORN_INFO_MSG("#deserializing #node @~s", [binary_to_list(Popcorn_Node#popcorn_node.node_name)]),

    Node_Name        = Popcorn_Node#popcorn_node.node_name,
    Prefix           = <<"raw_logs__">>,

    ets:insert(current_roles, {Popcorn_Node#popcorn_node.role, self()}),

    {reply, ok, 'LOGGING', State#state{popcorn_node          = Popcorn_Node}};

'LOGGING'({set_popcorn_node, Popcorn_Node}, _From, #state{workers = Workers} = State) ->
    gen_server:cast(?CACHED_STORAGE_PID(Workers), {add_node, Popcorn_Node}),

    Node_Name        = Popcorn_Node#popcorn_node.node_name,
    Prefix           = <<"raw_logs__">>,

    %% add this node to the "roles" tets table
    ets:insert(current_roles, {Popcorn_Node#popcorn_node.role, self()}),

    {reply, ok, 'LOGGING', State#state{popcorn_node          = Popcorn_Node}}.

handle_event(decrement_counter, State_Name, State) ->
    {next_state, State_Name, State#state{event_counter = State#state.event_counter - 1}};

handle_event(Event, StateName, State)                 -> {stop, {StateName, undefined_event, Event}, State}.
handle_sync_event(Event, _From, StateName, State)     -> {stop, {StateName, undefined_event, Event}, State}.

handle_info({broadcast, {new_storage_workers, Workers}}, State_Name, State) ->
    ?POPCORN_INFO_MSG("~p accepted new list of workers ~p", [?MODULE, Workers]),
    {next_state, State_Name, State#state{workers = Workers}};

handle_info(write_counter, State_Name, #state{workers = Workers} = State) ->
    Popcorn_Node = State#state.popcorn_node,
    %io:format("About to call with ~p ~p~n", [Popcorn_Node, State]),
    gen_server:cast(?CACHED_STORAGE_PID(Workers), {increment_counter, ?NODE_EVENT_COUNTER(Popcorn_Node#popcorn_node.node_name), State#state.event_counter}),
    erlang:send_after(?COUNTER_WRITE_INTERVAL, self(), write_counter),

    {next_state, State_Name, State#state{event_counter = 0}};

handle_info(_Info, StateName, State) ->
    ?POPCORN_WARN_MSG("Unhandled info call ~p", [_Info]),
    {next_state, StateName, State}.
terminate(_Reason, _StateName, State)                 -> ok.
code_change(_OldVsn, StateName, StateData, _Extra)    -> {ok, StateName, StateData}.
