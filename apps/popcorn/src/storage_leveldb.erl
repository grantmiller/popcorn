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
%%% File:      storage_leveldb.erl
%%% @author    Marc Campbell <marc.e.campbell@gmail.com>
%%% @doc
%%% @end
%%%-----------------------------------------------------------------

-module(storage_leveldb).
-author('marc.e.campbell@gmail.com').
-behavior(gen_server).

-include("include/popcorn.hrl").

-export([start_link/0, start_worker/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {db_ref :: reference()}).

-define(DAY_IN_MILLIS, 24 * 60 * 60 * 1000000).
-define(PREFIX_MESSAGE, <<"1">>).
-define(PREFIX_ALERT, <<"2">>).
-define(PREFIX_EVENT, <<"3">>).
-define(PREFIX_COUNTER, <<"4">>).
-define(PREFIX_NODE, <<"5">>).
-define(PREFIX_COLLECTION, <<"6">>).
-define(PREFIX_EXPIRATION, <<"7">>).

-define(BOOKMARK_MESSAGE, key_name(message, base62:encode(0))).
-define(BOOKMARK_ALERT, key_name(alert, base62:encode(0))).
-define(BOOKMARK_NODE, key_name(node, base62:encode(0))).
-define(BOOKMARK_COLLECTION, key_name(collection, base62:encode(0))).
-define(BOOKMARK_EXPIRATION, key_name(expiration, base62:encode(0))).
-define(BOOKMARK_END, key_name(<<"9">>, base62:encode(99999999999999999999999999999999))).

start_link() -> gen_server:start_link(?MODULE, [], []).
start_worker() -> gen_server:start_link(?MODULE, [worker], []).

key_name(Type, Name) when is_binary(Name) =/= true ->
  key_name(Type, popcorn_util:any_to_bin(Name));
key_name(message, Name) ->
  <<?PREFIX_MESSAGE/binary, Name/binary>>;
key_name(alert, Name) ->
  <<?PREFIX_ALERT/binary, Name/binary>>;
key_name(event, Name) ->
  <<?PREFIX_EVENT/binary, Name/binary>>;
key_name(counter, Name) ->
  <<?PREFIX_COUNTER/binary, Name/binary>>;
key_name(node, Name) ->
  <<?PREFIX_NODE/binary, Name/binary>>;
key_name(collection, Name) ->
  <<?PREFIX_COLLECTION/binary, Name/binary>>;
key_name(expiration, Name) ->
  <<?PREFIX_EXPIRATION/binary, Name/binary>>;
key_name(O, Name) when O =:= <<"9">> ->
  <<O/binary, Name/binary>>.

init([]) ->
  {ok, undefined};

init([worker]) ->
  process_flag(trap_exit, true),
  {ok, Config} = application:get_env(popcorn, storage),
  Options = proplists:get_value(options, Config),
  Base_Dir = binary_to_list(proplists:get_value(storage_dir, Options)),

  Storage_Dir = filename:join(Base_Dir, "popcorn"),
  ok = filelib:ensure_dir(Storage_Dir),

  {ok, Db_Ref} = eleveldb:open(Storage_Dir, [{create_if_missing, true}]),

  %% create some bookmarks
  eleveldb:put(Db_Ref, ?BOOKMARK_MESSAGE, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_ALERT, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_NODE, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_COLLECTION, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_EXPIRATION, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_END, <<>>, []),

  pg2:join('storage', self()),
  {ok, #state{db_ref = Db_Ref}}.

handle_call(most_recent_log, _From, State) ->
  {ok, Iterator} = eleveldb:iterator(State#state.db_ref, []),
  {ok, _, _} = eleveldb:iterator_move(Iterator, ?BOOKMARK_MESSAGE),
  {ok, _, Log_Message} = eleveldb:iterator_move(Iterator, next),
  ?POPCORN_DEBUG_MSG("Log_Message = ~p", [binary_to_term(Log_Message)]),
  {reply, ok, State};
handle_call(first_node, _From, State) ->
  {ok, Iterator} = eleveldb:iterator(State#state.db_ref, []),
  {ok, K, _} = eleveldb:iterator_move(Iterator, ?BOOKMARK_NODE),
  {ok, K2, Node} = eleveldb:iterator_move(Iterator, next),
  ?POPCORN_DEBUG_MSG("K = ~p, K2 = ~p", [K, K2]),
  ?POPCORN_DEBUG_MSG("Node = ~p", [binary_to_term(Node)]),
  {reply, ok, State};

handle_call({counter_value, Counter}, _From, State) ->
  Counter_Value =
    case eleveldb:get(State#state.db_ref, key_name(counter, Counter), []) of
      {ok, V} ->
        V;
      not_found ->
        0
    end,
  {reply, Counter_Value, State};
handle_call({get_alert, Key}, _From, State) ->
  case eleveldb:get(State#state.db_ref, key_name(alert, Key), []) of
    {ok, V} ->
      {reply, V, State};
    not_found ->
      {reply, undefined, State}
  end;
handle_call({get_alerts, Severity, Sort}, _From, State) ->
  {reply, [], State};
handle_call({is_known_node, Node_Name}, _From, State) ->
  case eleveldb:get(State#state.db_ref, key_name(collection, <<"nodes">>), []) of
    {ok, Nodes_Bin} ->
      {reply, lists:member(popcorn_util:any_to_bin(Node_Name), binary_to_term(Nodes_Bin)), State};
    not_found ->
      {reply, false, State}
  end;

handle_call(Msg, _From, State) ->
  ?POPCORN_ERROR_MSG("Unknown Msg = ~p", [Msg]),
  {reply, undefined, State}.

handle_cast({expire_logs_matching, _}, State) ->
  %% TODO spawn this into a new process
  {ok, Iterator} = eleveldb:iterator(State#state.db_ref, []),
  {ok, _, _} = eleveldb:iterator_move(Iterator, ?BOOKMARK_EXPIRATION),
  expire_iterate(State#state.db_ref, Iterator, ?NOW),
  {noreply, State};
handle_cast({new_log_message, Log_Message}, State) ->
  Inverse_Id = popcorn_util:inverse_id(Log_Message#log_message.message_id),
  eleveldb:put(State#state.db_ref, key_name(message, Inverse_Id), erlang:term_to_binary(Log_Message), []),
  eleveldb:put(State#state.db_ref, key_name(expiration, Log_Message#log_message.expire_at), popcorn_util:any_to_bin(Inverse_Id), []),
  system_counters:increment_severity_counter(Log_Message#log_message.severity),
  {noreply, State};
handle_cast({delete_counter, Counter}, State) ->
  eleveldb:delete(State#state.db_ref, key_name(counter, Counter), []),
  {noreply, State};
handle_cast({increment_counter, Counter, Increment_By}, State) ->
  increment_counter(State#state.db_ref, key_name(counter, Counter), Increment_By),
  {noreply, State};
handle_cast({increment_counters, Counters}, State) ->
  [increment_counter(State#state.db_ref, Counter, Value) || {Counter, Value} <- Counters],
  system_counters:reset_interval(),
  {noreply, State};
handle_cast({add_node, Popcorn_Node}, State) ->
  Node_Name = Popcorn_Node#popcorn_node.node_name,
  case eleveldb:get(State#state.db_ref, key_name(collection, <<"nodes">>), []) of
    {ok, Last_Nodes_Bin} ->
      Last_Nodes = binary_to_term(Last_Nodes_Bin),
      eleveldb:put(State#state.db_ref, key_name(collection, <<"nodes">>), Last_Nodes ++ Node_Name, []);
    not_found ->
      eleveldb:put(State#state.db_ref, key_name(collection, <<"nodes">>), [Node_Name], [])
  end,

  eleveldb:put(State#state.db_ref, key_name(node, Node_Name), term_to_binary(Popcorn_Node), []),
  {noreply, State};
handle_cast({send_recent_matching_log_lines, Pid, Count, Filters}, State) ->
  %% TODO spawn this into a new process
  {ok, Iterator} = eleveldb:iterator(State#state.db_ref, []),
  {ok, _, _} = eleveldb:iterator_move(Iterator, ?BOOKMARK_MESSAGE),
  send_recent_log_line(State#state.db_ref, Iterator, Pid, Count, Filters),
  {noreply, State};

handle_cast(_Msg, State) -> {noreply, State}.
handle_info(_Msg, State) -> {noreply, State}.
terminate(_Reason, State) ->
  eleveldb:close(State#state.db_ref),
  ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

increment_counter(Db_Ref, Counter, Increment_By) ->
  case eleveldb:get(Db_Ref, key_name(counter, Counter), []) of
    not_found ->
      eleveldb:put(Db_Ref, key_name(counter, Counter), popcorn_util:any_to_bin(Increment_By), []);
    {ok, Last_Val} ->
      New_Val = popcorn_util:any_to_bin(list_to_integer(binary_to_list(Last_Val)) + Increment_By),
      eleveldb:put(Db_Ref, key_name(counter, Counter), New_Val, [])
  end.


send_recent_log_line(_, _, _, 0, _) -> ok;
send_recent_log_line(Db_Ref, Iterator, Pid, Count, Filters) ->
  Next_Bookmark = ?BOOKMARK_ALERT,
  case eleveldb:iterator_move(Iterator, next) of
    {error, invalid_iterator} ->
      ok;
    {ok, Next_Bookmark, _} ->
      ok;
    {ok, _, Log_Message_Bin} ->
      Log_Message = binary_to_term(Log_Message_Bin),
      {ok, Popcorn_Node_Bin} = eleveldb:get(Db_Ref, key_name(node, Log_Message#log_message.log_nodename), []),
      Popcorn_Node = binary_to_term(Popcorn_Node_Bin),
      gen_fsm:send_all_state_event(Pid, {message, Log_Message, Popcorn_Node}),
      send_recent_log_line(Db_Ref, Iterator, Pid, Count - 1, Filters)
  end.

expire_iterate(Db_Ref, Iterator, Now) ->
  Next_Bookmark = ?BOOKMARK_END,
  case eleveldb:iterator_move(Iterator, next) of
    {error, invalid_iterator} ->
      ok;
    {ok, Next_Bookmark, _} ->
      ok;
    {ok, Expire_Key, Message_Key} ->
      Expire_At = list_to_integer(binary_to_list(binary:part(Expire_Key, {1,  byte_size(Expire_Key) - 1}))),
      case Expire_At of
        Should_Expire when Should_Expire < Now ->
          eleveldb:delete(Db_Ref, Expire_Key, []),
          eleveldb:delete(Db_Ref, key_name(message, Message_Key), []),
          expire_iterate(Db_Ref, Iterator, Now);
        _ ->
          ok
      end
  end.
