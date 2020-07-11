%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(kafka_interworking_proxy).

-include_lib("emqx/include/emqx.hrl").

-export([ load/1
        , unload/0
        ]).

%% Client Lifecircle Hooks
-export([ on_client_connack/4
        , on_client_disconnected/4
        ]).

%% Session Lifecircle Hooks
-export([ on_session_subscribed/4
        , on_session_unsubscribed/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        , on_message_delivered/3
        , on_message_acked/3
        ]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqx:hook('client.connack',      {?MODULE, on_client_connack, [Env]}),
    emqx:hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    emqx:hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    emqx:hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    emqx:hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
    emqx:hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
    emqx:hook('message.acked',       {?MODULE, on_message_acked, [Env]}).
    
%%--------------------------------------------------------------------
%% Client Lifecircle Hooks
%%--------------------------------------------------------------------


on_client_connack(ConnInfo = #{clientid := ClientId, username := Username }, Rc, Props, _Env) ->
    Json = jsx:encode([
            {broker, list_to_binary(hostName())},
            {hook, list_to_binary("on_client_connected")},
            {timestamp, list_to_binary(timestamp())},
            {clientId, ClientId },
            {username, Username},    
            {result, Rc}
        ]),
        sendMsgToKafka(Json),

        %%----------------------------------------------------
        %% Remaining for other usage
        %%----------------------------------------------------
            %%Status = Rc =:= "not_authorized",
            %%io:format ("~p~n", [Status]),
            %%case Status of
                %% when the result code is <not_authorized>
                %%  true -> 
                %%    io:format("in true ~p~n",[Status]);
                    
                %%false ->
                    %%io:format("in false ~p~n",[Status])
            %%end,
    {ok, Props}.

on_client_disconnected(ClientInfo = #{clientid := ClientId, username := Username}, ReasonCode, ConnInfo, _Env) ->
    Json = jsx:encode([
            {broker, list_to_binary(hostName())},
            {hook, list_to_binary("on_client_disconnected")},
            {timestamp, list_to_binary(timestamp())},
            {clientId, ClientId },
            {username, Username},    
            {reason, ReasonCode}
        ]),
        sendMsgToKafka(Json).

%%--------------------------------------------------------------------
%% Session Lifecircle Hooks
%%--------------------------------------------------------------------

on_session_subscribed(#{clientid := ClientId, username := Username}, Topic, SubOpts, _Env) ->
    Json = jsx:encode([
                {broker, list_to_binary(hostName())},
                {hook, list_to_binary("on_session_subscribe")},
                {timestamp, list_to_binary(timestamp())},
                {clientId, ClientId },
                {username, Username},
                {topic, Topic}
            ]),
            sendMsgToKafka(Json).

on_session_unsubscribed(#{clientid := ClientId, username := Username}, Topic, Opts, _Env) ->
     Json = jsx:encode([
                {broker, list_to_binary(hostName())},
                {hook, list_to_binary("on_session_unsubscribe")},
                {timestamp, list_to_binary(timestamp())},
                {clientId, ClientId },
                {username, Username},
                {topic, Topic}
            ]),
            sendMsgToKafka(Json).


%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{topic = Topic, payload = Payload, qos = Qos, from = From, headers = Headers}, _Env) ->
    %% TODO : Topic-ignored code is needed

    Checker1 = string:find(From, "smart-fleet-vse-firehouse-alert-") =:= From,
    Checker2 = string:find(From, "smart-fleet-vse-ex-cits-alert-") =:= From,
    Checker3 = string:find(From, "smart-fleet-ovs-") =:= From,
    Checker4 = string:find(Topic, "ovs/location") =:= Topic, 

    if
        Checker1 =:= true orelse Checker2 =:= true orelse Checker3 =:= true orelse Checker4 =:=true ->
            ok;
        true ->
            Json = jsx:encode([
                        {broker, list_to_binary(hostName())},
                        {hook, list_to_binary("on_message_publish")},
                        {timestamp, list_to_binary(timestamp())},
                        {clientId, From },
                        {username, emqx_message:get_header(username, Message, undefined)},
                        {topic, Topic},
                        {payload, Payload},
                        {qos, Qos}
                    ]),
            sendMsgToKafka(Json)
    end.
    

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
    ok;
on_message_dropped(Message, _By = #{node := Node}, Reason, _Env) ->
    io:format("Message dropped by node ~s due to ~s: ~s~n",
              [Node, Reason, emqx_message:format(Message)]).

on_message_delivered(_ClientInfo = #{clientid := ClientId, username := Username}, Message = #message{topic = Topic, from = From, payload = Payload}, _Env) ->

    Checker1 = string:find(From, "smart-fleet-ovs-") =:= From,

    if 
        Checker1 =:= true ->
            Json = jsx:encode([
                    {broker, list_to_binary(hostName())},
                    {hook, list_to_binary("on_message_delivered")},
                    {from, From},
                    {fromUser, emqx_message:get_header(username, Message, undefined)},
                    {to, ClientId },
                    {toUser, Username},
                    {topic, Topic},
                    {payload, Payload},
                    {timestamp, list_to_binary(timestamp())}
                ]),
                sendMsgToKafka(Json);
        true -> ok
    end.

on_message_acked(_ClientInfo = #{clientid := ClientId, username := Username}, Message = #message{topic = Topic, from = From, payload = Payload}, _Env) ->

    Checker1 = string:find(From, "smart-fleet-ovs-") =:= From,

    if
        Checker1 =:= true ->
           Json = jsx:encode([
                {broker, list_to_binary(hostName())},
                {hook, list_to_binary("on_message_acked")},
                {from, From},
                {fromUser, emqx_message:get_header(username, Message, undefined)},
                {to, ClientId },
                {toUser, Username},
                {topic, Topic},
                {payload, Payload},
                {timestamp, list_to_binary(timestamp())}
            ]),
            sendMsgToKafka(Json);    
        true -> ok
            
    end.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connack',      {?MODULE, on_client_connack}),
    emqx:unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    emqx:unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    emqx:unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    emqx:unhook('message.publish',     {?MODULE, on_message_publish}),
    emqx:unhook('message.delivered',   {?MODULE, on_message_delivered}),
    emqx:unhook('message.acked',       {?MODULE, on_message_acked}).

    
%% Init kafka Server Connection
ekaf_init(_Env) ->
    application:load(ekaf),
    {ok, Values} = application:get_env(kafka_interworking_proxy, values),
    BootstrapBroker = proplists:get_value(bootstrap_broker, Values),
    PartitionStrategy= proplists:get_value(partition_strategy, Values),
    PartitionWorkers = proplists:get_value(partition_workers, Values),
    PartitionWorkersMax = proplists:get_value(partition_workers_max, Values),
    DownTimeBufferSize = proplists:get_value(downtime_buffer_size, Values),
    application:set_env(ekaf, ekaf_partition_strategy, PartitionStrategy),
    application:set_env(ekaf, ekaf_bootstrap_broker, BootstrapBroker),
    application:set_env(ekaf, ekaf_per_partition_workers, PartitionWorkers),
    application:set_env(ekaf, ekaf_per_partition_workers_max, PartitionWorkersMax),
    application:set_env(ekaf, ekaf_max_downtime_buffer_size, DownTimeBufferSize),
    {ok, _} = application:ensure_all_started(ekaf),
    io:format("Initialized ekaf with ~p~n", [BootstrapBroker]).        

%% sending JSON messasge toward kafka server
sendMsgToKafka(Msg) ->
    {ok, KafkaTopic} = application:get_env(kafka_interworking_proxy, values),
    %% FailMessagePath = proplists:get_value(failMessagePath, KafkaTopic),
    ProduceTopic = proplists:get_value(kafka_producer_topic, KafkaTopic),   
    try ekaf:produce_sync(ProduceTopic, Msg) of
        _ -> ok
    catch
        error:Msg -> io:format("kafka sending : error : ~p~n", [Msg])
    end.
    
    %% TODO : file logging when the kafka broker is down. --> DONE
    %% ekaf fault tolerant concept
    %% If all brokers/partitions are unavailable, will start buffering messages in-memory, 
    %% and replay them when the broker is back. 
    %% See ekaf_max_downtime_buffer_size for configuring this options. 
    %% By default this is not set, since kafka restarts are usually quick, 
    %% and possibility of all brokers down is low.

timestamp() -> integer_to_list(erlang:system_time(millisecond)).

hostName() -> {ok, Hostname} = inet:gethostname(), Hostname.

msgWriteFile(FilePath, Msg) ->
    case file:open(FilePath ++ "-" ++ dateFormat(), [append]) of
        {ok, IoDevice} ->
            io:format(IoDevice, "~s~n", [Msg]),
            file:close(IoDevice);
        {error, Reason} ->
            io:error("~s open error  reason:~s~n", [FilePath, Reason])
    end.

dateFormat() ->
	{{Year,Month,Day},{Hour, _Min, _Sec}} = erlang:localtime(),
	lists:flatten(io_lib:format('~4..0b~2..0b~2..0b~2..0b', [Year, Month, Day, Hour])).