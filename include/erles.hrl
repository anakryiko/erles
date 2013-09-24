-type uuid() :: <<_:128>>.

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
            event_number      :: non_neg_integer(),
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

