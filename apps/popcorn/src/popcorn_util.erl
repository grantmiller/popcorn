-module(popcorn_util).
-author('marc.e.campbell@gmail.com').

-include("include/popcorn.hrl").

-export([node_event_counter/1,
         unique_id/0,
         hour/0,
         retention_time_to_microsec/1,
         last_24_hours/0,
         severity_to_number/1,
         number_to_severity/1,
         all_severities/0,
         alert_severities/0,
         random_id/0,
         format_summary_message/2,
         format_log_message/2,
         opt/2,
         head_includes/0,
         optional_env/2,
         hexstring/1,
         read/1,
         rps_enabled/0,
         md5_hex/1,
         jiffy_safe/1,
         jiffy_safe_array/1,
         jiffy_safe_proplist/1,
         top/0, top/1, top/2,
         ftop/0, ftop/1, ftop/2,
         any_to_bin/1,
         inverse_id/1
     ]).

inverse_id(Id) ->
  base62:encode(99999999999999999999999999999999 - base62:decode(binary_to_list(Id))).

any_to_bin(I) when is_list(I) ->
  list_to_binary(I);
any_to_bin(I) when is_atom(I) ->
  any_to_bin(atom_to_list(I));
any_to_bin(I) when is_integer(I) ->
  any_to_bin(integer_to_list(I));
any_to_bin(I) when is_float(I) ->
  any_to_bin(float_to_list(I));
any_to_bin(I) when is_binary(I) ->
  I.

node_event_counter(Node_Name) ->
    Prefix = <<"node_events__">>,
    <<Prefix/binary, Node_Name/binary>>.

unique_id() -> {_, S} = flake_server:id(62),
               list_to_binary(S).

hour() -> integer_to_list(erlang:trunc(folsom_utils:now_epoch() / 3600)).

seconds_to_microseconds(Seconds) -> Seconds * 1000000.

retention_time_to_microsec({minutes, Minutes}) -> seconds_to_microseconds(Minutes * 60);
retention_time_to_microsec({hours, Hours})     -> retention_time_to_microsec({minutes, Hours * 60});
retention_time_to_microsec({days, Days})       -> retention_time_to_microsec({hours, Days * 24});
retention_time_to_microsec({weeks, Weeks})     -> retention_time_to_microsec({days, Weeks * 7});
retention_time_to_microsec({months, Months})   -> retention_time_to_microsec({weeks, Months * 4}).

last_24_hours() ->
    lists:map(fun(Hours_Ago) ->
        integer_to_list(erlang:trunc(folsom_utils:now_epoch() / 3600) - Hours_Ago)
      end, lists:seq(0, 23)).

severity_to_number(Severity) when is_binary(Severity) -> severity_to_number(binary_to_atom(Severity, utf8));
severity_to_number(Severity) when is_list(Severity) -> severity_to_number(list_to_atom(Severity));
severity_to_number(Severity) ->
    try lager_util:level_to_num(Severity)
    catch
        _:_ -> -1
    end.

number_to_severity(N) ->
    try lager_util:num_to_level(N) of
        Level -> atom_to_list(Level)
    catch
        _:_ -> "?"
    end.

all_severities() -> [{atom_to_list(L), lager_util:level_to_num(L)} || L <- lager_util:levels()].
alert_severities() -> [{atom_to_list(L), lager_util:level_to_num(L)} || L <- [warning, error, critical, alert, emergency]].

random_id() ->
    Length = 64,
    AllowedChars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    random:seed(now()),
    New_Key = lists:foldl(fun(_, Acc) ->
                  [lists:nth(random:uniform(length(AllowedChars)), AllowedChars)] ++ Acc
                end, [], lists:seq(1, Length)),
    list_to_binary(New_Key).

optional_env(Key, Default) ->
    case application:get_env(popcorn, Key) of
        undefined -> Default;
        {ok, Val} -> Val
    end.

opt(<<>>, Default)      -> Default;
opt(undefined, Default) -> Default;
opt(Value, _)           -> Value.

-spec apply_links(list(), list(), binary()) -> binary().
apply_links([], [], In) -> In;
apply_links([], Topics, In) ->
    Topic = list_to_binary(lists:nth(1, Topics)),
    Symbol = <<"#">>,
    A = <<"<a href=\"#\" class=\"topic\" data=\"">>,
    B = <<"\"><span class=\"label label-info\">#">>,
    C = <<"</span></a>">>,
    Out = binary:replace(In, <<Symbol/binary, Topic/binary>>, <<A/binary, Topic/binary, B/binary, Topic/binary, C/binary>>),
    apply_links([], lists:nthtail(1, Topics), Out);
apply_links(Identities, Topics, In) ->
    Identity = list_to_binary(lists:nth(1, Identities)),
    Symbol = <<"@">>,
    A = <<"<a href=\"#\" class=\"identity\" data=\"">>,
    B = <<"\"><span class=\"label label-inverse\">@">>,
    C = <<"</span></a>">>,
    Out = binary:replace(In, <<Symbol/binary, Identity/binary>>, <<A/binary, Identity/binary, B/binary, Identity/binary, C/binary>>),
    apply_links(lists:nthtail(1, Identities), Topics, Out).

-spec format_summary_message(#log_message{}, #popcorn_node{} | undefined) -> list().
format_summary_message(#log_message{timestamp=Timestamp, log_module=Module, log_function=Function, log_line=Line, log_pid=Pid,
                                severity=Severity, message=Message, topics=Topics, identities=Identities, log_product=Product,
                                log_version=Version, message_id=Message_Id, log_nodename=Node_Name},
                   Popcorn_Node) ->
  UTC_Timestamp = calendar:now_to_universal_time({Timestamp div 1000000000000, 
                                                  Timestamp div 1000000 rem 1000000,
                                                  Timestamp rem 1000000}),
  {Role, Name} =
    case Popcorn_Node of
      undefined -> {undefined, undefined};
      _ -> {Popcorn_Node#popcorn_node.role, Popcorn_Node#popcorn_node.node_name}
    end,

  [{'timestamp', jiffy_safe(Timestamp)},
   {'role', jiffy_safe(opt(Role, <<"Unknown Role">>))},
   {'node', jiffy_safe(opt(Name, <<"Unknown Node">>))},
   {'severity', jiffy_safe(Severity)}].


-spec format_log_message(#log_message{}, #popcorn_node{} | undefined) -> list().
format_log_message(#log_message{timestamp=Timestamp, log_module=Module, log_function=Function, log_line=Line, log_pid=Pid,
                                severity=Severity, message=Message, topics=Topics, identities=Identities, log_product=Product,
                                log_version=Version, message_id=Message_Id, log_nodename=Node_Name},
                   Popcorn_Node) ->

  Millisecond_Timestamp = Timestamp * 1000,
  UTC_Timestamp = calendar:now_to_universal_time({Millisecond_Timestamp div 1000000000000, 
                                                  Millisecond_Timestamp div 1000000 rem 1000000,
                                                  Millisecond_Timestamp rem 1000000}),
  {{Year, Month, Day}, {Hour, Minute, Second}} = UTC_Timestamp,
  Formatted_DateTime = lists:flatten(io_lib:format("~4.10.0B-~2.10.0B-~2.10.0BT~2.10.0B:~2.10.0B:~2.10.0BZ", [Year, Month, Day, Hour, Minute, Second])),
  Formatted_Time     = lists:flatten(io_lib:format("~2.10.0B:~2.10.0B:~2.10.0B", [Hour, Minute, Second])),

  More_Html =
    "<strong>Message " ++ binary_to_list(Message_Id) ++ "</strong><hr />" ++
    "<label class='popover-label'><strong>Node: </strong>" ++ binary_to_list(Node_Name) ++ "</label>" ++
    "<label class='popover-label'><strong>Severity: </strong>" ++ number_to_severity(Severity) ++ "</label>" ++
    "<label class='popover-label'><strong>Module: </strong>" ++ binary_to_list(opt(Module, <<"Not set">>)) ++ "</label>" ++
    "<label class='popover-label'><strong>Function: </strong>" ++ binary_to_list(opt(Function, <<"Not set">>)) ++ "</label>" ++
    "<label class='popover-label'><strong>Line: </strong>" ++ binary_to_list(opt(Line, <<"?">>)) ++ " in " ++ binary_to_list(opt(Module, <<"not set">>)) ++ "</label>" ++
    "<label class='popover-label'><strong>Pid: </strong>" ++ binary_to_list(opt(Pid, <<"Not set">>)) ++ "</label>",

  Linked_Message = apply_links(Identities, Topics, Message),

  {Role, Name} =
    case Popcorn_Node of
      undefined -> {undefined, undefined};
      _ -> {Popcorn_Node#popcorn_node.role, Popcorn_Node#popcorn_node.node_name}
    end,

  [{'timestamp',        jiffy_safe(Timestamp)},
   {'role',             jiffy_safe(opt(Role, <<"Unknown Role">>))},
   {'node',             jiffy_safe(opt(Name, <<"Unknown Node">>))},
   {'topics',           jiffy_safe_array(Topics)},
   {'identities',       jiffy_safe_array(Identities)},
   {'time',             jiffy_safe(Formatted_Time)},
   {'datetime',         jiffy_safe(Formatted_DateTime)},
   {'find_more_html',   jiffy_safe(More_Html)},
   {'log_product',      jiffy_safe(opt(Product, <<"Unknown">>))},
   {'log_version',      jiffy_safe(opt(Version, <<"Unknown">>))},
   {'log_module',       jiffy_safe(opt(Module, <<"Unknown">>))},
   {'log_function',     jiffy_safe(opt(Function, <<"Unknown">>))},
   {'log_line',         jiffy_safe(opt(Line, <<"??">>))},
   {'log_pid',          jiffy_safe(opt(Pid, <<"?">>))},
   {'message_severity', jiffy_safe(number_to_severity(Severity))},
   {'message_severity_raw', jiffy_safe(Severity)},
   {'message',          jiffy_safe(Linked_Message)}].

css_file() ->
    case file:read_file_info(code:priv_dir(popcorn) ++ "/css/popcorn.css") of
        {error,enoent} -> "popcorn.less";
        {ok, _} -> "popcorn.css"
    end.

head_includes() ->
    Head_Includes = ["<link rel='stylesheet/less' href=\"/css/"++css_file()++"\" type=\"text/css\">",
                     "<script src=\"/js/less.js\" type=\"text/javascript\"></script>"],

    lists:map(fun(Include) ->
        dict:from_list([{'tag', Include}])
      end, Head_Includes).

%% For testing
read(Filename) ->
    case file:read_file(Filename) of
        {ok, Binary} -> Binary;
        {error, _}   -> <<"">>
    end.

hexstring(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X])).

rps_enabled() ->
    {ok, Rps_Options} = case application:get_env(popcorn, rps_tracking) of
                            undefined ->  {ok, [{enabled, false}]};
                            Rps_Config -> Rps_Config
                        end,
    proplists:get_value(enabled, Rps_Options).


md5_hex(S) ->
    Md5_bin  = erlang:md5(S),
    Md5_list = binary_to_list(Md5_bin),
    lists:flatten(list_to_hex(Md5_list)).
list_to_hex(L) -> lists:map(fun(X) -> int_to_hex(X) end, L).
int_to_hex(N) when N < 256 -> [hex(N div 16), hex(N rem 16)].
hex(N) when N < 10 -> $0+N;
hex(N) when N >= 10, N < 16 -> $a + (N-10).

jiffy_safe_array([]) -> [];
jiffy_safe_array(Values) when is_list(Values) -> [jiffy_safe(Value) || Value <- Values].
jiffy_safe(Value) when is_integer(Value) -> Value;
jiffy_safe(Value) when is_list(Value) -> list_to_binary(Value);
jiffy_safe(Value) when is_binary(Value) -> Value.
jiffy_safe_proplist(List) -> [{K, popcorn_util:jiffy_safe(V)} || {K,V} <- List].

-spec top() -> [tuple()].
top() -> top(10).

-spec top(pos_integer()) -> [tuple()].
top(Length) -> top(Length, mq).

-type top_field() :: function | fn | name | nm | messages | mq | reductions | rd | psize | ps.
-spec top(pos_integer(), top_field()) -> [tuple()].
top(Length, Sort) ->
    lager:info("Gathering Process List..."),
    {Time, Procs} =
        timer:tc(
            fun() ->
                lists:sublist(
                    lists:reverse(
                        lists:sort(
                            [{top_field(Sort,P), P}
                             || P <- pman_process:r_processes(node()),
                                not pman_process:is_system_process(P)])),
                        Length)
            end),
    lager:info("Process List gathered in ~p secs", [round(Time/1000000)]),
    [{Pid,
      [{nm, top_field(nm, Pid)},
       {mq, top_field(mq, Pid)},
       {rd, top_field(rd, Pid)},
       {ps, top_field(ps, Pid)},
       {fn, top_field(fn, Pid)}]} || {_, Pid} <- Procs].

top_field(mq, P) -> pman_process:msg(P);
top_field(rd, P) -> pman_process:reds(P);
top_field(ps, P) -> pman_process:psize(P);
top_field(nm, P) -> pman_process:get_name(P);
top_field(fn, P) -> pman_process:function_info(P);
top_field(messags, P)   -> pman_process:msg(P);
top_field(reductions, P)-> pman_process:reds(P);
top_field(psize, P)     -> pman_process:psize(P);
top_field(name, P)      -> pman_process:get_name(P);
top_field(function, P)  -> pman_process:function_info(P).

-spec ftop() -> ok.
ftop() -> ftop(10).

-spec ftop(pos_integer()) -> ok.
ftop(Length) -> ftop(Length, mq).

-spec ftop(pos_integer(), top_field()) -> ok.
ftop(Length, Sort) ->
    Data = top(Length, Sort),
    io:format("~-15s | ~-25s | ~-7s | ~-7s | ~-7s | ~s~n", [pid, name, msgq, reds, psize, function]),
    io:format("~100c~n", [$-]),
    lists:foreach(
        fun({Pid, Fields}) -> io:format("~15s | ~-25s | ~7w | ~7w | ~7w | ~w~n", [pid_to_list(Pid) | [F || {_, F} <- Fields]]) end,
        Data).
