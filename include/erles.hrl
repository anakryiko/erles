-type uuid()               :: <<_:128>>.
-type stream_id()          :: nonempty_string() | binary().
-type all_stream_id()      :: 'all' | stream_id().
-type stream_ver()         :: -1 | non_neg_integer().
-type exp_ver_any()        :: 'any' | -2.
-type exp_ver()            :: stream_ver() | exp_ver_any().
-type trans_id()           :: non_neg_integer().

-type event_num()          :: non_neg_integer().
-type tfpos()              :: {'tfpos', non_neg_integer(), non_neg_integer()}.
-type stream_pos()         :: event_num() | tfpos().
-type read_dir()           :: 'forward' | 'backward'.
-type subscr_pos()         :: 'live' | {'inclusive', stream_pos()} | {'exclusive', stream_pos()}.

-type operation_error()    :: 'retry_limit'
                            | 'server_timeout'
                            | {'aborted', term()}
                            | {'not_authenticated', term()}
                            | {'bad_request', term()}.

-type read_stream_error()  :: 'no_stream' | 'stream_deleted' | 'access_denied' | operation_error().
-type read_event_error()   :: 'no_event' | read_stream_error().
-type write_error()        :: 'wrong_exp_ver' | 'access_denied' | 'stream_deleted' | 'invalid_transaction' | operation_error().
-type subscr_error()       :: 'access_denied' | {'subscriber_down', term()}  | operation_error().

-type auth()               :: 'noauth' | 'defauth' | {Login :: string() | binary(), Pass :: string() | binary()}.
-type connect_option()     :: {'default_auth',       auth()}
                            | {'max_server_ops',     pos_integer()}
                            | {'operation_timeout',  pos_integer()}
                            | {'operation_retries',  non_neg_integer()}
                            | {'retry_delay',        non_neg_integer()}
                            | {'connection_timeout', pos_integer()}
                            | {'reconnection_delay', non_neg_integer()}
                            | {'max_conn_retries',   non_neg_integer()}
                            | {'heartbeat_period',   pos_integer()}
                            | {'heartbeat_timeout',  pos_integer()}
                            | {'dns_timeout',        pos_integer()}
                            | {'gossip_timeout',     pos_integer()}.

-type write_option()       :: {auth, auth()} | {master_only, boolean()}.
-type read_option()        :: {auth, auth()} | {master_only, boolean()} | {resolve, boolean()}.
-type subscr_option()      :: {auth, auth()}
                            | {resolve, boolean()}
                            | {subscriber, boolean()}
                            | {max_count, pos_integer()}.
-type subscr_prim_option() :: {auth, auth()} | {resolve, boolean()} | {subscriber, boolean()}.

%% Event data to send for write to Event Store
-record(event_data,
        {
            event_id = erles_utils:gen_uuid()                 :: uuid(),
            event_type = erlang:error({required, event_type}) :: binary() | string(),
            data_type = raw                                   :: 'raw' | 'json',
            data = erlang:error({required, data})             :: binary(),
            metadata = <<>>                                   :: binary()
        }).

%% Committed event data returned from read requests and subscriptions
-record(event,
        {
            stream_id         :: binary(),
            event_number      :: event_num(),
            event_id          :: uuid(),
            event_type        :: binary(),
            data              :: binary(),
            metadata = <<>>   :: binary()
        }).

%% Stream ACL structure
-record(stream_acl,
        {
            read_roles      :: 'undefined' | [binary()],
            write_roles     :: 'undefined' | [binary()],
            delete_roles    :: 'undefined' | [binary()],
            metaread_roles  :: 'undefined' | [binary()],
            metawrite_roles :: 'undefined' | [binary()]
        }).

%% Stream metadata structure
-record(stream_meta,
        {
            max_count       :: 'undefined' | pos_integer(),
            max_age         :: 'undefined' | pos_integer(),
            truncate_before :: 'undefined' | non_neg_integer(),
            cache_control   :: 'undefined' | pos_integer(),
            acl             :: 'undefined' | #stream_acl{},
            custom          :: 'undefined' | jsx:json_term()
        }).

-type event_data()       :: #event_data{}.
-type event()            :: #event{}.
-type stream_acl()       :: #stream_acl{}.
-type stream_meta()      :: #stream_meta{}.
-type stream_event_res() :: {'event', event(), event_num()}.
-type all_event_res()    :: {'event', event(), tfpos()}.
-type event_res()        :: stream_event_res() | all_event_res().
-type struct_meta_res()  :: {'meta', stream_meta(), stream_ver()}.
-type raw_meta_res()     :: {'meta', binary(), stream_ver()}.
