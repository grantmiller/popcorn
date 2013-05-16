-module(triage_handler).
-author('martin@tigertext.com').
-author('marc.e.campbell@gamil.com').
-author('elbrujohalcon@inaka.net').

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).
-export([counter_data/1,
         all_alerts/2,
         alerts/3,
         recent_alerts/1,
         recent_alerts/2,
         alert_count_today/0,
         alert_count/0,
         clear_alert/1,
         safe_notify/4,
         log_messages/3,
         alert_properties/1,
         key/5]).

-define(UPDATE_COUNTERS_INTERVAL, 10000).

-include("include/popcorn.hrl").

-record(state, {incident = 1 :: integer(),
                update_counters_dirty = false :: boolean(),
                update_counters_timer :: reference(),
                storage_workers = [] :: list()}).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Exported function that callers should use to triage events
safe_notify(Popcorn_Node, Node_Pid, #log_message{} = Log_Message, Is_New_Node) ->
  gen_server:cast(?MODULE, {triage_logmessage, Popcorn_Node, Node_Pid, Log_Message, Is_New_Node}).

%% @doc Exported function to return the alert properties
alert_properties(Alert) ->
  Properties = location_as_strings(base64:decode(re:replace(Alert, "_", "=", [{return, binary}, global]))),
  case is_nonexistant_module_line(proplists:get_value(line, Properties)) of
    true ->
      Cleanup = proplists:delete(line, Properties),
      proplists:delete(module, Cleanup);
    false ->
      Module = proplists:get_value(name, Properties),
      Line = proplists:get_value(line, Properties),
      source_location(Module, Line) ++ Properties
  end.

is_nonexistant_module_line("0") -> true;
is_nonexistant_module_line("1") -> true;
is_nonexistant_module_line(_) -> false.

location_as_strings(Counter) ->
  lists:zipwith(
      fun(K, V) -> {K, binary_to_list(V)} end,
    [severity, product, version, name, line], split_location(Counter)).

split_location(Counter) ->
  re:split(Counter, <<":">>, [{return, binary}]).

log_messages(Alert, Starting_Timestamp, Page_Size) ->
  gen_server:call(?MODULE, {messages, base64:decode(re:replace(Alert, "_", "=", [{return, binary}, global])), Starting_Timestamp, Page_Size}).

counter_data(Counter) ->
  gen_server:call(?MODULE, {data, Counter}).

alert_count() ->
  gen_server:call(?MODULE, total_alerts).

alert_count_today() ->
  gen_server:call(?MODULE, alerts_for_today).

all_alerts(true = _Include_Cleared, Sort) ->
  gen_server:call(?MODULE, {alerts, all, all, Sort});
all_alerts(_, Sort) ->
  gen_server:call(?MODULE, {alerts, recent, all, Sort}).

alerts(true = _Include_Cleared, Severities, Sort) ->
  gen_server:call(?MODULE, {alerts, all, Severities, Sort});
alerts(_, Severities, Sort) ->
  gen_server:call(?MODULE, {alerts, recent, Severities, Sort}).

recent_alerts(Count) ->
  gen_server:call(?MODULE, {alerts, Count, all, time}).
recent_alerts(Count, Severities) ->
  gen_server:call(?MODULE, {alerts, Count, Severities, time}).

clear_alert(Alert) ->
  Counter = base64:decode(re:replace(Alert, "_", "=", [{return, binary}, global])),
  gen_server:call(?MODULE, {clear, binary_to_list(Counter)}).

init(_) ->
  Update_Counters_Timer = erlang:send_after(?UPDATE_COUNTERS_INTERVAL, self(), update_counters),
  pubsub:subscribe(storage, ?MODULE, self()),
  {ok, #state{update_counters_timer = Update_Counters_Timer}}.

%% @doc ??
handle_call({data, Counter}, _From, State) ->
  V =
    case gen_server:call(?CACHED_STORAGE_PID(State#state.storage_workers), {get_alert, Counter}) of
      #alert{} = Alert ->
        Alert;
      _ ->
        #alert{}
    end,
  {reply, alert_data(V), State};

%% @doc ??
handle_call(total_alerts, _From, State) ->
    Alert_Count = ?COUNTER_VALUE(?TOTAL_ALERT_COUNTER),
    {reply, Alert_Count, State};

%% @doc ??
handle_call(alerts_for_today, _From, State) ->
    Day_Alerts = ?COUNTER_VALUE(day_key()),
    {reply, Day_Alerts, State};

%% @doc ??
handle_call({alerts, Count, Severities, Sort}, _From, State) ->
  Alerts = gen_server:call(?CACHED_STORAGE_PID(State#state.storage_workers), {get_alerts, Severities, Sort}),
  {Small_List, _} = lists:split(erlang:min(Count, length(Alerts)), Alerts),
  Final_List = [alert_data(Alert) || Alert <- Small_List],
  {reply, Final_List, State};

%% @doc ??
handle_call({clear, Counter}, _From, State) ->
    Key = recent_key(Counter),

    gen_server:cast(?CACHED_STORAGE_PID(State#state.storage_workers), {delete_counter, Key}),
    ?DECREMENT_COUNTER(?TOTAL_ALERT_COUNTER),
    Total_Alert_Count = ?COUNTER_VALUE(?TOTAL_ALERT_COUNTER),
    NewCounters =
        [{counter,      Counter},
         {alert_count,  Total_Alert_Count}],
    dashboard_stream_fsm:broadcast({update_counters, NewCounters}),
    {reply, ok, reset_timer(State)};

%% @doc ??
handle_call({messages, Alert, Starting_Timestamp, Page_Size}, _From, State) ->
    [S, P, V, M, L] = split_location(Alert),
    Messages = gen_server:call(?CACHED_STORAGE_PID(State#state.storage_workers), {search_messages, {popcorn_util:severity_to_number(S), P, V, M, L, Page_Size, Starting_Timestamp}}),
    {reply, Messages, State};

%% @doc Stop if an unknown message is received
handle_call(Message, _From, State) ->
  {stop, {unknown_message, Message}, State}.

%% @doc Entry point for all log messages which meet the core criteria for alerts
handle_cast({triage_logmessage, #popcorn_node{} = Node, Node_Pid,
              #log_message{log_product=Product, log_version=Version, log_module=Module, log_line=Line, severity=Severity} = Log_Message,
              Is_New_Node}, #state{incident=Incident} = State)
        when Severity =< 16, Severity =/= 0, is_binary(Product), is_binary(Version), is_binary(Module), is_binary(Line) ->
  Storage_Pid = ?CACHED_STORAGE_PID(State#state.storage_workers),
  Alert_Location = key(Severity, Product, Version, Module, Line),
  case gen_server:call(Storage_Pid, {alert_exists, Alert_Location}) of
    true ->
      gen_server:cast(Storage_Pid, {new_alert_instance, Alert_Location, Log_Message#log_message.message_id});
    false ->
      gen_server:cast(Storage_Pid, {new_alert,
                                    #alert{location = Alert_Location,
                                           message_ids = [Log_Message#log_message.message_id],
                                           most_recent_timestamp = ?NOW},
                                    Log_Message#log_message.message_id})
  end,

  case Is_New_Node of
    true ->
      outbound_notifier:notify(new_node, as_proplist(Node)),
      dashboard_stream_fsm:broadcast({new_node, Node});
    false ->
      ok
  end,
  increment_counters_after_creating_alert(Node, Node_Pid, Severity, Product, Version, Module, Line, Storage_Pid, Log_Message#log_message.message),
  {noreply, reset_timer(State#state{update_counters_dirty = true})};

%% @doc If the event was not an alert, but it was a new node, trigger an alert that this node was created
handle_cast({triage_logmessage, #popcorn_node{} = Node, _Node_Pid, _Log_Message, true}, State) ->
  outbound_notifier:notify(new_node, as_proplist(Node)),
  dashboard_stream_fsm:broadcast({new_node, Node}),
  {noreply, State#state{update_counters_dirty = true}};

%% @doc All other events should not trigger alerts
handle_cast({triage_logmessage, #popcorn_node{}, _Node_Pid, _Log_Message, false}, State) ->
  {noreply, State};

handle_cast(Event, State) ->
  ?POPCORN_ERROR_MSG("Unexpected event in #triage_handler: ~p", [Event]),
  {noreply, State}.

%% @doc when the storage works change, this is called
handle_info({broadcast, {new_storage_workers, Storage_Workers}}, State) ->
  {noreply, State#state{storage_workers = Storage_Workers}};

%% @doc Timer will fire this to update all counters
handle_info(update_counters, State) when State#state.update_counters_dirty =:= true ->
  lists:foreach(
      fun({_, undefined}) ->
           ok;
         ({Node_Name, _}) ->
           NodeCounters =
             [{node_hash, re:replace(base64:encode(Node_Name), "=", "_", [{return, binary}, global])},
              {node_count, ?COUNTER_VALUE(?NODE_EVENT_COUNTER(Node_Name))}],
           dashboard_stream_fsm:broadcast({update_counters, NodeCounters})
        end, ets:tab2list(current_nodes)),

  Day_Key = day_key(),

  %% TODO, perhaps this should be optimized
  %gen_server:cast(?CACHED_STORAGE_PID(State#state.storage_workers), {new_alert_key, day, Day_Key}),

  Day_Count = ?COUNTER_VALUE(Day_Key),
  Event_Count = ?COUNTER_VALUE(?TOTAL_EVENT_COUNTER),
  Alert_Count = ?COUNTER_VALUE(?TOTAL_ALERT_COUNTER),

  NewCounters =
    [{event_count,       Event_Count},
     {alert_count_today, Day_Count},
     {alert_count,       Alert_Count}],

  dashboard_stream_fsm:broadcast({update_counters, NewCounters}),
  {noreply, reset_timer(State#state{update_counters_dirty = false})};
handle_info(update_counters, State) ->
  {noreply, reset_timer(State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% @doc Increment counters based on an alert received
increment_counters_after_creating_alert(Node, _Node_Pid, Severity, Product, Version, Module, Line, Storage_Pid, Message) ->
  Count_Key = key(Severity,Product,Version,Module,Line),
  Recent_Counter_Key = recent_key(Count_Key),
  Day_Key = day_key(),

  case ?COUNTER_VALUE(Count_Key) of
    0 ->
      ?INCREMENT_COUNTER(Day_Key);
    _ ->
      ok
  end,

  ?INCREMENT_COUNTER(Count_Key),
  ?INCREMENT_COUNTER(Recent_Counter_Key),

  case ?COUNTER_VALUE(Recent_Counter_Key) of
    1 ->
      outbound_notifier:notify(new_alert, location_as_strings(Count_Key) ++ [{message, binary_to_list(Message)}, {key, base64:encode(key(Severity,Product,Version,Module,Line))}]),
      ?INCREMENT_COUNTER(?TOTAL_ALERT_COUNTER);
    _ ->
      ok
  end,

  Day_Count = ?COUNTER_VALUE(Day_Key),
  Event_Count = ?COUNTER_VALUE(?TOTAL_EVENT_COUNTER),
  Alert_Count = ?COUNTER_VALUE(?TOTAL_ALERT_COUNTER),
  Node_Event_Count = ?COUNTER_VALUE(?NODE_EVENT_COUNTER(Node#popcorn_node.node_name)),

  NewCounters =
    [{node_hash, re:replace(base64:encode(Node#popcorn_node.node_name), "=", "_", [{return, binary}, global])},
     {node_count, Node_Event_Count},
     {counter, Count_Key},
     {event_count, Event_Count},
     {alert_count_today, Day_Count},
     {alert_count, Alert_Count}],

  outbound_notifier:notify(new_event, [{location, Count_Key} | location_as_strings(Count_Key)]),
  dashboard_stream_fsm:broadcast({update_counters, NewCounters}).

key(Severity,Product,Version,Module,Line) ->
    SeverityName = list_to_binary(popcorn_util:number_to_severity(Severity)),
    binary_to_list(<<SeverityName/binary, ":", Product/binary, ":", Version/binary, ":", Module/binary, ":", Line/binary>>).

recent_key(Counter) ->
  "recent:" ++ Counter.

%% @doc create a string based key using the current date
day_key() ->
  {{Year, Month, Day}, _} = calendar:now_to_universal_time(erlang:now()),
  integer_to_list(Year) ++ "-" ++ integer_to_list(Month) ++ "-" ++ integer_to_list(Day).

%% @doc Return the data for an alert
alert_data(#alert{location = undefined}) ->
  [];
alert_data(#alert{} = Alert) ->
  Timestamp = Alert#alert.most_recent_timestamp,
  UTC_Timestamp = calendar:now_to_universal_time({Timestamp div 1000000000000, 
                                                  Timestamp div 1000000 rem 1000000,
                                                  Timestamp rem 1000000}),
  {{Year, Month, Day}, {Hour, Minute, Second}} = UTC_Timestamp,
  Formatted_DateTime = lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0BZ", [Year, Month, Day, Hour, Minute, Second])),

  Most_Recent_Message_Id = lists:max(Alert#alert.message_ids),
  Log_Message = gen_server:call(?STORAGE_PID, {get_log_message, Most_Recent_Message_Id}),

  Basic_Properties = source_location(Log_Message#log_message.log_module, Log_Message#log_message.log_line) ++ [{'severity_num', Log_Message#log_message.severity}, {'message', list(Log_Message#log_message.message)}, {'datetime', Formatted_DateTime}],
  All_Properties = location_as_strings(Alert#alert.location) ++ Basic_Properties,

  Location_Count = ?COUNTER_VALUE(Alert#alert.location),
  Recent_Location_Count = ?COUNTER_VALUE(recent_key(Alert#alert.location)),

  [{location, re:replace(base64:encode(Alert#alert.location), "=", "_", [{return, list}, global])},
   {count, Location_Count},
   {recent, Recent_Location_Count}
   | All_Properties].


source_location(Module, Line) when Line =:= <<"1">>; Line =:= <<"0">> ->
  [];
source_location(Module, Line) ->
  [{source, binary_to_list(iolist_to_binary([<<" at ">>, Module, <<" line ">>, Line]))}].

list(B) when is_binary(B) ->
  binary_to_list(B);
list(_) ->
  "".

reverse_limit_and_filter(Alerts, all) ->
  [alert_data(Alert) || Alert <- lists:reverse(Alerts)];
reverse_limit_and_filter(Alerts, Count) ->
    reverse_limit_and_filter(lists:reverse(Alerts), Count, []).
reverse_limit_and_filter([], _Count, Acc) -> lists:reverse(Acc);
reverse_limit_and_filter(_Alerts, Count, Acc) when length(Acc) == Count -> lists:reverse(Acc);
reverse_limit_and_filter([Alert | Alerts], Count, Acc) ->
    Location_Count = ?COUNTER_VALUE(recent_key(Alert#alert.location)),

    reverse_limit_and_filter(
        Alerts, Count,
        case Location_Count of
            0 -> Acc;
            _ -> [alert_data(Alert) | Acc]
        end).

reset_timer(#state{incident=Incident} = State) ->
    erlang:cancel_timer(State#state.update_counters_timer),
    Update_Counters_Timer = erlang:send_after(?UPDATE_COUNTERS_INTERVAL, self(), update_counters),
    State#state{update_counters_timer = Update_Counters_Timer,
                incident=Incident+1}.

as_proplist(Node) when is_record(Node, popcorn_node) ->
    [popcorn_node | Props] = tuple_to_list(Node),
    lists:zip(record_info(fields, popcorn_node), Props).
