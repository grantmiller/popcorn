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
-define(PREFIX_COLLECTION, <<"10">>).
-define(PREFIX_NODE, <<"11">>).
-define(PREFIX_COUNTER, <<"12">>).
-define(PREFIX_MESSAGE, <<"13">>).
-define(PREFIX_EXPIRATION, <<"14">>).
-define(PREFIX_ALERT, <<"15">>).
-define(PREFIX_ALERTKEY, <<"16">>).
-define(PREFIX_ALERTCOUNTER, <<"17">>).
-define(PREFIX_ALERTTIMESTAMP, <<"18">>).
-define(PREFIX_RELEASESCM, <<"19">>).
-define(PREFIX_RELEASESCMMAPPING, <<"20">>).

-define(BOOKMARK_COLLECTION, key_name(collection, "0")).
-define(BOOKMARK_NODE, key_name(node, "0")).
-define(BOOKMARK_COUNTER, key_name(counter, "0")).
-define(BOOKMARK_MESSAGE, key_name(message, "0")).
-define(BOOKMARK_EXPIRATION, key_name(expiration, "0")).
-define(BOOKMARK_ALERT, key_name(alert, "0")).
-define(BOOKMARK_ALERTKEY, key_name(alertkey, "0")).
-define(BOOKMARK_ALERTCOUNTER, key_name(alertcounter, "0")).
-define(BOOKMARK_ALERTTIMESTAMP, key_name(alerttimestamp, "0")).
-define(BOOKMARK_RELEASESCM, key_name(releasescm, "0")).
-define(BOOKMARK_RELEASESCMMAPPING, key_name(releasescmmapping, "0")).
-define(BOOKMARK_END, key_name(<<"99">>, "0")).

start_link() -> gen_server:start_link(?MODULE, [], []).
start_worker() -> gen_server:start_link(?MODULE, [worker], []).

key_name(Type, Name) when is_binary(Name) =/= true -> key_name(Type, popcorn_util:any_to_bin(Name));
key_name(collection, Name) -> <<?PREFIX_COLLECTION/binary, Name/binary>>;
key_name(node, Name) -> <<?PREFIX_NODE/binary, Name/binary>>;
key_name(counter, Name) -> <<?PREFIX_COUNTER/binary, Name/binary>>;
key_name(message, Name) -> <<?PREFIX_MESSAGE/binary, Name/binary>>;
key_name(expiration, Name) -> <<?PREFIX_EXPIRATION/binary, Name/binary>>;
key_name(alert, Name) -> <<?PREFIX_ALERT/binary, Name/binary>>;
key_name(alertkey, Name) -> <<?PREFIX_ALERT/binary, Name/binary>>;
key_name(alertcounter, Name) -> <<?PREFIX_ALERTCOUNTER/binary, Name/binary>>;
key_name(alerttimestamp, Name) -> <<?PREFIX_ALERTTIMESTAMP/binary, Name/binary>>;
key_name(releasescm, Name) -> <<?PREFIX_RELEASESCM/binary, Name/binary>>;
key_name(releasescmmapping, Name) -> <<?PREFIX_RELEASESCMMAPPING/binary, Name/binary>>;
key_name(O, Name) when O =:= <<"99">> -> <<O/binary, Name/binary>>.

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
  eleveldb:put(Db_Ref, ?BOOKMARK_COLLECTION, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_NODE, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_COUNTER, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_MESSAGE, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_EXPIRATION, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_ALERT, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_ALERTKEY, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_ALERTCOUNTER, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_ALERTTIMESTAMP, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_RELEASESCM, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_RELEASESCMMAPPING, <<>>, []),
  eleveldb:put(Db_Ref, ?BOOKMARK_END, <<>>, []),

  pg2:join('storage', self()),
  {ok, #state{db_ref = Db_Ref}}.

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
    {ok, Alert_Bin} ->
      {reply, binary_to_term(Alert_Bin), State};
    not_found ->
      {reply, undefined, State}
  end;
handle_call({get_alerts, Severities, Sort}, _From, State) ->
  case eleveldb:get(State#state.db_ref, key_name(collection, <<"alerts">>), []) of
    not_found ->
      {reply, [], State};
    {ok, Alerts_Bin} ->
      Alert_Locations = binary_to_term(Alerts_Bin),
      Alerts =
        lists:map(fun(Alert_Location) ->
          {ok, Alert_Bin} = eleveldb:get(State#state.db_ref, key_name(alert, Alert_Location), []),
          binary_to_term(Alert_Bin)
        end, Alert_Locations),
      case Severities of
        all ->
          {reply, Alerts, State};
        _ ->
          {reply,
           lists:filter(fun(Alert) ->
               Log_Message = Alert#alert.log,
               Severity = Log_Message#log_message.severity,
               lists:member(Severity, Severities)
             end, Alerts),
           State}
      end
  end;
handle_call({get_alert_keys, Type}, _From, State) ->
  {reply, [], State};
handle_call({get_release_module_link, Role, Version, Module}, _From, State) ->
  {reply, undefined, State};
handle_call({search_messages, {S, P, V, M, L, Page_Size, Starting_Timestamp}}, _From, State) ->
  {reply, [], State};
handle_call({get_alert_timestamps, Severities}, _From, State) ->
  {reply, [], State};
handle_call({is_known_node, Node_Name}, _From, State) ->
  case eleveldb:get(State#state.db_ref, key_name(collection, <<"nodes">>), []) of
    {ok, Nodes_Bin} ->
      {reply, lists:member(popcorn_util:any_to_bin(Node_Name), binary_to_term(Nodes_Bin)), State};
    not_found ->
      {reply, false, State}
  end;
handle_call(Request, _From, State) ->
  {stop, {unknown_call, Request}, State}.

handle_cast({expire_logs_matching, _}, State) ->
  %% TODO spawn this into a new process
  {ok, Iterator} = eleveldb:iterator(State#state.db_ref, []),
  {ok, _, _} = eleveldb:iterator_move(Iterator, ?BOOKMARK_EXPIRATION),
  Deleted_Counts = lists:map(fun({_, S}) -> {S, 0} end, popcorn_util:all_severities()),
  expire_iterate(State#state.db_ref, Iterator, ?NOW, Deleted_Counts),
  history_optimizer:expire_logs_complete(),
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
      eleveldb:put(State#state.db_ref, key_name(collection, <<"nodes">>), term_to_binary(Last_Nodes ++ [Node_Name]), []);
    not_found ->
      eleveldb:put(State#state.db_ref, key_name(collection, <<"nodes">>), term_to_binary([Node_Name]), [])
  end,

  eleveldb:put(State#state.db_ref, key_name(node, Node_Name), term_to_binary(Popcorn_Node), []),
  {noreply, State};
handle_cast({send_recent_matching_log_lines, Pid, Count, Filters}, State) ->
  %% TODO spawn this into a new process
  {ok, Iterator} = eleveldb:iterator(State#state.db_ref, []),
  {ok, _, _} = eleveldb:iterator_move(Iterator, ?BOOKMARK_MESSAGE),
  send_recent_log_line(State#state.db_ref, Iterator, Pid, Count, Filters),
  {noreply, State};
handle_cast({new_release_scm, Release_Scm}, State) ->
  eleveldb:put(State#state.db_ref, key_name(releasescm, Release_Scm#release_scm.key), term_to_binary(Release_Scm), []),
  {noreply, State};
handle_cast({new_alert, Key, #alert{} = Alert}, State) ->
  eleveldb:put(State#state.db_ref, key_name(alert, Key), term_to_binary(Alert#alert{location = Key}), []),
  case eleveldb:get(State#state.db_ref, key_name(collection, <<"alerts">>), []) of
    {ok, Last_Alerts_Bin} ->
      Last_Alerts = binary_to_term(Last_Alerts_Bin),
      eleveldb:put(State#state.db_ref, key_name(collection, <<"alerts">>), term_to_binary(Last_Alerts ++ [Key]), []);
    not_found ->
      eleveldb:put(State#state.db_ref, key_name(collection, <<"alerts">>), term_to_binary([Key]), [])
  end,
  {noreply, State};
handle_cast({new_alert_timestamp, Key, Severity, #alert{timestamp = Timestamp} = Record}, State) ->
  {noreply, State};
handle_cast({new_alert_key, Type, Key}, State) ->
  {noreply, State};
handle_cast({new_release_scm_mapping, Record}, State) ->
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

expire_iterate(Db_Ref, Iterator, Now, Deleted_Counts) ->
  Next_Bookmark = ?BOOKMARK_END,
  case eleveldb:iterator_move(Iterator, next) of
    {error, invalid_iterator} ->
      save_deleted_counts(Deleted_Counts),
      ok;
    {ok, Next_Bookmark, _} ->
      save_deleted_counts(Deleted_Counts),
      ok;
    {ok, Expire_Key, Message_Key} ->
      Expire_At = list_to_integer(binary_to_list(binary:part(Expire_Key, {1,  byte_size(Expire_Key) - 1}))),
      case Expire_At of
        Should_Expire when Should_Expire < Now ->
          eleveldb:delete(Db_Ref, Expire_Key, []),
          {ok, Log_Message_Bin} = eleveldb:get(Db_Ref, key_name(message, Message_Key), []),
          Log_Message = binary_to_term(Log_Message_Bin),
          eleveldb:delete(Db_Ref, key_name(message, Message_Key), []),
          Last_Value = proplists:get_value(Log_Message#log_message.severity, Deleted_Counts),
          New_Deleted_Counts = proplists:delete(Log_Message#log_message.severity, Deleted_Counts) ++ [{Log_Message#log_message.severity, Last_Value + 1}],
          expire_iterate(Db_Ref, Iterator, Now, New_Deleted_Counts);
        _ ->
          save_deleted_counts(Deleted_Counts),
          ok
      end
  end.

save_deleted_counts(Deleted_Counts) ->
  system_counters:set_severity_counters(Deleted_Counts),
  ok.
