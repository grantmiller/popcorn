-module(view_dashboard).
-author('marc.e.campbell@gmail.com').

-include("include/popcorn.hrl").

-export([username/1,
         avatar_path/1,
         head_includes/1,
         node_count/0,
         event_count/0,
         alerts/0,
         hashtag_count/0,
         mention_count/0,
         alert_count_today/0,
         alert_count/0,
         alert_lines/0,
         known_nodes/0]).

-spec username(dict()) -> string().
username(Context) ->
  mustache:get(username, Context).

-spec avatar_path(dict()) -> string().
avatar_path(Context) ->
  "http://www.gravatar.com/avatar/" ++ popcorn_util:md5_hex(mustache:get(username, Context)).

-spec head_includes(dict()) -> list().
head_includes(_) ->
  popcorn_util:head_includes().

-spec alert_lines() -> pos_integer().
alert_lines() ->
  case application:get_env(popcorn, dashboard_alert_lines) of
    {ok, Value} -> Value;
    _ -> 3
  end.

-spec node_count() -> integer().
node_count() -> ets:info(current_nodes, size).

-spec event_count() -> integer().
event_count() ->
    ?COUNTER_VALUE(?TOTAL_EVENT_COUNTER).

-spec hashtag_count() -> integer().
hashtag_count() -> 0.

-spec mention_count() -> integer().
mention_count() -> 0.

-spec alert_count_today() -> integer().
alert_count_today() -> triage_handler:alert_count_today().

-spec alert_count() -> integer().
alert_count() -> triage_handler:alert_count().

alerts() ->
  [dict:from_list(Alert) || Alert <- triage_handler:recent_alerts(alert_lines(), [?POPCORN_ERROR])].

-spec known_nodes() -> list().
known_nodes() ->
    Total_Message_Count = event_count(),
    lists:map(fun({Node, Pid}) ->
        Node_Event_Count = ?COUNTER_VALUE(?NODE_EVENT_COUNTER(Node)),
        Node_List        = binary_to_list(Node),
        Node_Properties  = [{'node_name',             Node_List},
                            {'node_hash',             re:replace(base64:encode(Node), "=", "_", [{return, list}, global])},
                            {'total_messages',        Node_Event_Count},
                            {'percent_of_all_events', case Total_Message_Count of
                                                         0 -> 0;
                                                         _ -> ?PERCENT(Node_Event_Count / Total_Message_Count)
                                                      end},
                            {'alert_count',           0},
                            {'hashtag_count',         0},
                            {'mention_count',         0}],
        dict:from_list(Node_Properties)
      end, ets:tab2list(current_nodes)).

