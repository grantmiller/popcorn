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

-record(state, {most_recent_version   :: string(),
                popcorn_node          :: #popcorn_node{}}).

start_link() -> gen_fsm:start_link(?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),

    {ok, 'LOGGING', #state{}}.

'LOGGING'({log_message, Popcorn_Node, Log_Message}, State) ->
    try
        %% log the message
        mnesia:dirty_write(popcorn_history, Log_Message) =:= ok orelse ?POPCORN_ERROR_MSG("failed to write log entry"),

        %% increment the total event counter
        mnesia:dirty_update_counter(popcorn_counters, ?TOTAL_EVENT_COUNTER, 1),

        %% increment the node event counter
        mnesia:dirty_update_counter(popcorn_counters, ?NODE_EVENT_COUNTER(Popcorn_Node#popcorn_node.node_name), 1),

        %% Notify any streams connected
        Log_Streams = ets:tab2list(current_log_streams),
        lists:foreach(fun(Log_Stream) ->
            gen_fsm:send_all_state_event(Log_Stream#stream.stream_pid, {new_message, newer, Log_Message})
          end, Log_Streams)
    catch
        _:Error ->
            io:format("Couldn't log message:~nMessage: ~p~nNode: ~p~nError: ~p~nStack: ~p~n",
                        [Log_Message, Popcorn_Node, Error, erlang:get_stacktrace()])
    end,
    {next_state, 'LOGGING', State}.

'LOGGING'({deserialize_popcorn_node, Popcorn_Node}, _From, State) ->
    Node_Name        = Popcorn_Node#popcorn_node.node_name,
    Prefix           = <<"raw_logs__">>,

    ets:insert(current_roles, {Popcorn_Node#popcorn_node.role, self()}),

    {reply, ok, 'LOGGING', State#state{popcorn_node          = Popcorn_Node}};

'LOGGING'({set_popcorn_node, Popcorn_Node}, _From, State) ->
    mnesia:dirty_write(known_nodes, Popcorn_Node),

    %% create the node counter
    mnesia:dirty_update_counter(popcorn_counters, ?NODE_EVENT_COUNTER(Popcorn_Node#popcorn_node.node_name), 0),

    Node_Name        = Popcorn_Node#popcorn_node.node_name,
    Prefix           = <<"raw_logs__">>,

    %% add this node to the "roles" tets table
    ets:insert(current_roles, {Popcorn_Node#popcorn_node.role, self()}),

    {reply, ok, 'LOGGING', State#state{popcorn_node          = Popcorn_Node}}.

handle_event(Event, StateName, State)                 -> {stop, {StateName, undefined_event, Event}, State}.
handle_sync_event(Event, _From, StateName, State)     -> {stop, {StateName, undefined_event, Event}, State}.
handle_info(_Info, StateName, State)                  -> {next_state, StateName, State}.
terminate(_Reason, _StateName, State)                 -> ok.
code_change(_OldVsn, StateName, StateData, _Extra)    -> {ok, StateName, StateData}.
