-ifndef(NEWEVENT_PB_H).
-define(NEWEVENT_PB_H, true).
-record(newevent, {
    event_id = erlang:error({required, event_id}),
    event_type = erlang:error({required, event_type}),
    data_content_type = erlang:error({required, data_content_type}),
    metadata_content_type = erlang:error({required, metadata_content_type}),
    data = erlang:error({required, data}),
    metadata
}).
-endif.

-ifndef(EVENTRECORD_PB_H).
-define(EVENTRECORD_PB_H, true).
-record(eventrecord, {
    event_stream_id = erlang:error({required, event_stream_id}),
    event_number = erlang:error({required, event_number}),
    event_id = erlang:error({required, event_id}),
    event_type = erlang:error({required, event_type}),
    data_content_type = erlang:error({required, data_content_type}),
    metadata_content_type = erlang:error({required, metadata_content_type}),
    data = erlang:error({required, data}),
    metadata
}).
-endif.

-ifndef(RESOLVEDINDEXEDEVENT_PB_H).
-define(RESOLVEDINDEXEDEVENT_PB_H, true).
-record(resolvedindexedevent, {
    event = erlang:error({required, event}),
    link
}).
-endif.

-ifndef(RESOLVEDEVENT_PB_H).
-define(RESOLVEDEVENT_PB_H, true).
-record(resolvedevent, {
    event = erlang:error({required, event}),
    link,
    commit_position = erlang:error({required, commit_position}),
    prepare_position = erlang:error({required, prepare_position})
}).
-endif.

-ifndef(WRITEEVENTS_PB_H).
-define(WRITEEVENTS_PB_H, true).
-record(writeevents, {
    event_stream_id = erlang:error({required, event_stream_id}),
    expected_version = erlang:error({required, expected_version}),
    events = [],
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(WRITEEVENTSCOMPLETED_PB_H).
-define(WRITEEVENTSCOMPLETED_PB_H, true).
-record(writeeventscompleted, {
    result = erlang:error({required, result}),
    message,
    first_event_number = erlang:error({required, first_event_number})
}).
-endif.

-ifndef(DELETESTREAM_PB_H).
-define(DELETESTREAM_PB_H, true).
-record(deletestream, {
    event_stream_id = erlang:error({required, event_stream_id}),
    expected_version = erlang:error({required, expected_version}),
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(DELETESTREAMCOMPLETED_PB_H).
-define(DELETESTREAMCOMPLETED_PB_H, true).
-record(deletestreamcompleted, {
    result = erlang:error({required, result}),
    message
}).
-endif.

-ifndef(TRANSACTIONSTART_PB_H).
-define(TRANSACTIONSTART_PB_H, true).
-record(transactionstart, {
    event_stream_id = erlang:error({required, event_stream_id}),
    expected_version = erlang:error({required, expected_version}),
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(TRANSACTIONSTARTCOMPLETED_PB_H).
-define(TRANSACTIONSTARTCOMPLETED_PB_H, true).
-record(transactionstartcompleted, {
    transaction_id = erlang:error({required, transaction_id}),
    result = erlang:error({required, result}),
    message
}).
-endif.

-ifndef(TRANSACTIONWRITE_PB_H).
-define(TRANSACTIONWRITE_PB_H, true).
-record(transactionwrite, {
    transaction_id = erlang:error({required, transaction_id}),
    events = [],
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(TRANSACTIONWRITECOMPLETED_PB_H).
-define(TRANSACTIONWRITECOMPLETED_PB_H, true).
-record(transactionwritecompleted, {
    transaction_id = erlang:error({required, transaction_id}),
    result = erlang:error({required, result}),
    message
}).
-endif.

-ifndef(TRANSACTIONCOMMIT_PB_H).
-define(TRANSACTIONCOMMIT_PB_H, true).
-record(transactioncommit, {
    transaction_id = erlang:error({required, transaction_id}),
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(TRANSACTIONCOMMITCOMPLETED_PB_H).
-define(TRANSACTIONCOMMITCOMPLETED_PB_H, true).
-record(transactioncommitcompleted, {
    transaction_id = erlang:error({required, transaction_id}),
    result = erlang:error({required, result}),
    message
}).
-endif.

-ifndef(READEVENT_PB_H).
-define(READEVENT_PB_H, true).
-record(readevent, {
    event_stream_id = erlang:error({required, event_stream_id}),
    event_number = erlang:error({required, event_number}),
    resolve_link_tos = erlang:error({required, resolve_link_tos}),
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(READEVENTCOMPLETED_PB_H).
-define(READEVENTCOMPLETED_PB_H, true).
-record(readeventcompleted, {
    result = erlang:error({required, result}),
    event = erlang:error({required, event}),
    error
}).
-endif.

-ifndef(READSTREAMEVENTS_PB_H).
-define(READSTREAMEVENTS_PB_H, true).
-record(readstreamevents, {
    event_stream_id = erlang:error({required, event_stream_id}),
    from_event_number = erlang:error({required, from_event_number}),
    max_count = erlang:error({required, max_count}),
    resolve_link_tos = erlang:error({required, resolve_link_tos}),
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(READSTREAMEVENTSCOMPLETED_PB_H).
-define(READSTREAMEVENTSCOMPLETED_PB_H, true).
-record(readstreameventscompleted, {
    events = [],
    result = erlang:error({required, result}),
    next_event_number = erlang:error({required, next_event_number}),
    last_event_number = erlang:error({required, last_event_number}),
    is_end_of_stream = erlang:error({required, is_end_of_stream}),
    last_commit_position = erlang:error({required, last_commit_position}),
    error
}).
-endif.

-ifndef(READALLEVENTS_PB_H).
-define(READALLEVENTS_PB_H, true).
-record(readallevents, {
    commit_position = erlang:error({required, commit_position}),
    prepare_position = erlang:error({required, prepare_position}),
    max_count = erlang:error({required, max_count}),
    resolve_link_tos = erlang:error({required, resolve_link_tos}),
    require_master = erlang:error({required, require_master})
}).
-endif.

-ifndef(READALLEVENTSCOMPLETED_PB_H).
-define(READALLEVENTSCOMPLETED_PB_H, true).
-record(readalleventscompleted, {
    commit_position = erlang:error({required, commit_position}),
    prepare_position = erlang:error({required, prepare_position}),
    events = [],
    next_commit_position = erlang:error({required, next_commit_position}),
    next_prepare_position = erlang:error({required, next_prepare_position}),
    result = 'Success',
    error
}).
-endif.

-ifndef(SUBSCRIBETOSTREAM_PB_H).
-define(SUBSCRIBETOSTREAM_PB_H, true).
-record(subscribetostream, {
    event_stream_id = erlang:error({required, event_stream_id}),
    resolve_link_tos = erlang:error({required, resolve_link_tos})
}).
-endif.

-ifndef(SUBSCRIPTIONCONFIRMATION_PB_H).
-define(SUBSCRIPTIONCONFIRMATION_PB_H, true).
-record(subscriptionconfirmation, {
    last_commit_position = erlang:error({required, last_commit_position}),
    last_event_number
}).
-endif.

-ifndef(STREAMEVENTAPPEARED_PB_H).
-define(STREAMEVENTAPPEARED_PB_H, true).
-record(streameventappeared, {
    event = erlang:error({required, event})
}).
-endif.

-ifndef(UNSUBSCRIBEFROMSTREAM_PB_H).
-define(UNSUBSCRIBEFROMSTREAM_PB_H, true).
-record(unsubscribefromstream, {
    
}).
-endif.

-ifndef(SUBSCRIPTIONDROPPED_PB_H).
-define(SUBSCRIPTIONDROPPED_PB_H, true).
-record(subscriptiondropped, {
    reason = 'Unsubscribed'
}).
-endif.

-ifndef(NOTHANDLED_PB_H).
-define(NOTHANDLED_PB_H, true).
-record(nothandled, {
    reason = erlang:error({required, reason}),
    additional_info
}).
-endif.

-ifndef(SCAVENGEDATABASE_PB_H).
-define(SCAVENGEDATABASE_PB_H, true).
-record(scavengedatabase, {
    
}).
-endif.

-ifndef(SCAVENGEDATABASECOMPLETED_PB_H).
-define(SCAVENGEDATABASECOMPLETED_PB_H, true).
-record(scavengedatabasecompleted, {
    result = erlang:error({required, result}),
    error,
    total_time_ms = erlang:error({required, total_time_ms}),
    total_space_saved = erlang:error({required, total_space_saved})
}).
-endif.

-ifndef(NOTHANDLED_MASTERINFO_PB_H).
-define(NOTHANDLED_MASTERINFO_PB_H, true).
-record(nothandled_masterinfo, {
    external_tcp_address = erlang:error({required, external_tcp_address}),
    external_tcp_port = erlang:error({required, external_tcp_port}),
    external_http_address = erlang:error({required, external_http_address}),
    external_http_port = erlang:error({required, external_http_port}),
    external_secure_tcp_address,
    external_secure_tcp_port
}).
-endif.

