-file("src/erlesque_clientapi_pb.erl", 1).

-module(erlesque_clientapi_pb).

-export([encode_nothandled_masterinfo/1,
	 decode_nothandled_masterinfo/1,
	 delimited_decode_nothandled_masterinfo/1,
	 encode_scavengedatabasecompleted/1,
	 decode_scavengedatabasecompleted/1,
	 delimited_decode_scavengedatabasecompleted/1,
	 encode_scavengedatabase/1, decode_scavengedatabase/1,
	 delimited_decode_scavengedatabase/1,
	 encode_nothandled/1, decode_nothandled/1,
	 delimited_decode_nothandled/1,
	 encode_subscriptiondropped/1,
	 decode_subscriptiondropped/1,
	 delimited_decode_subscriptiondropped/1,
	 encode_unsubscribefromstream/1,
	 decode_unsubscribefromstream/1,
	 delimited_decode_unsubscribefromstream/1,
	 encode_streameventappeared/1,
	 decode_streameventappeared/1,
	 delimited_decode_streameventappeared/1,
	 encode_subscriptionconfirmation/1,
	 decode_subscriptionconfirmation/1,
	 delimited_decode_subscriptionconfirmation/1,
	 encode_subscribetostream/1, decode_subscribetostream/1,
	 delimited_decode_subscribetostream/1,
	 encode_readalleventscompleted/1,
	 decode_readalleventscompleted/1,
	 delimited_decode_readalleventscompleted/1,
	 encode_readallevents/1, decode_readallevents/1,
	 delimited_decode_readallevents/1,
	 encode_readstreameventscompleted/1,
	 decode_readstreameventscompleted/1,
	 delimited_decode_readstreameventscompleted/1,
	 encode_readstreamevents/1, decode_readstreamevents/1,
	 delimited_decode_readstreamevents/1,
	 encode_readeventcompleted/1,
	 decode_readeventcompleted/1,
	 delimited_decode_readeventcompleted/1,
	 encode_readevent/1, decode_readevent/1,
	 delimited_decode_readevent/1,
	 encode_transactioncommitcompleted/1,
	 decode_transactioncommitcompleted/1,
	 delimited_decode_transactioncommitcompleted/1,
	 encode_transactioncommit/1, decode_transactioncommit/1,
	 delimited_decode_transactioncommit/1,
	 encode_transactionwritecompleted/1,
	 decode_transactionwritecompleted/1,
	 delimited_decode_transactionwritecompleted/1,
	 encode_transactionwrite/1, decode_transactionwrite/1,
	 delimited_decode_transactionwrite/1,
	 encode_transactionstartcompleted/1,
	 decode_transactionstartcompleted/1,
	 delimited_decode_transactionstartcompleted/1,
	 encode_transactionstart/1, decode_transactionstart/1,
	 delimited_decode_transactionstart/1,
	 encode_deletestreamcompleted/1,
	 decode_deletestreamcompleted/1,
	 delimited_decode_deletestreamcompleted/1,
	 encode_deletestream/1, decode_deletestream/1,
	 delimited_decode_deletestream/1,
	 encode_writeeventscompleted/1,
	 decode_writeeventscompleted/1,
	 delimited_decode_writeeventscompleted/1,
	 encode_writeevents/1, decode_writeevents/1,
	 delimited_decode_writeevents/1, encode_resolvedevent/1,
	 decode_resolvedevent/1,
	 delimited_decode_resolvedevent/1,
	 encode_resolvedindexedevent/1,
	 decode_resolvedindexedevent/1,
	 delimited_decode_resolvedindexedevent/1,
	 encode_eventrecord/1, decode_eventrecord/1,
	 delimited_decode_eventrecord/1, encode_newevent/1,
	 decode_newevent/1, delimited_decode_newevent/1]).

-export([has_extension/2, extension_size/1,
	 get_extension/2, set_extension/3]).

-export([decode_extensions/1]).

-export([encode/1, decode/2, delimited_decode/2]).

-record(nothandled_masterinfo,
	{external_tcp_address, external_tcp_port,
	 external_http_address, external_http_port,
	 external_secure_tcp_address, external_secure_tcp_port}).

-record(scavengedatabasecompleted,
	{result, error, total_time_ms, total_space_saved}).

-record(scavengedatabase, {}).

-record(nothandled, {reason, additional_info}).

-record(subscriptiondropped, {reason}).

-record(unsubscribefromstream, {}).

-record(streameventappeared, {event}).

-record(subscriptionconfirmation,
	{last_commit_position, last_event_number}).

-record(subscribetostream,
	{event_stream_id, resolve_link_tos}).

-record(readalleventscompleted,
	{commit_position, prepare_position, events,
	 next_commit_position, next_prepare_position, result,
	 error}).

-record(readallevents,
	{commit_position, prepare_position, max_count,
	 resolve_link_tos, require_master}).

-record(readstreameventscompleted,
	{events, result, next_event_number, last_event_number,
	 is_end_of_stream, last_commit_position, error}).

-record(readstreamevents,
	{event_stream_id, from_event_number, max_count,
	 resolve_link_tos, require_master}).

-record(readeventcompleted, {result, event, error}).

-record(readevent,
	{event_stream_id, event_number, resolve_link_tos,
	 require_master}).

-record(transactioncommitcompleted,
	{transaction_id, result, message}).

-record(transactioncommit,
	{transaction_id, require_master}).

-record(transactionwritecompleted,
	{transaction_id, result, message}).

-record(transactionwrite,
	{transaction_id, events, require_master}).

-record(transactionstartcompleted,
	{transaction_id, result, message}).

-record(transactionstart,
	{event_stream_id, expected_version, require_master}).

-record(deletestreamcompleted, {result, message}).

-record(deletestream,
	{event_stream_id, expected_version, require_master}).

-record(writeeventscompleted,
	{result, message, first_event_number}).

-record(writeevents,
	{event_stream_id, expected_version, events,
	 require_master}).

-record(resolvedevent,
	{event, link, commit_position, prepare_position}).

-record(resolvedindexedevent, {event, link}).

-record(eventrecord,
	{event_stream_id, event_number, event_id, event_type,
	 data_content_type, metadata_content_type, data,
	 metadata}).

-record(newevent,
	{event_id, event_type, data_content_type,
	 metadata_content_type, data, metadata}).

encode([]) -> [];
encode(Records) when is_list(Records) ->
    delimited_encode(Records);
encode(Record) -> encode(element(1, Record), Record).

encode_nothandled_masterinfo(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_nothandled_masterinfo(Record)
    when is_record(Record, nothandled_masterinfo) ->
    encode(nothandled_masterinfo, Record).

encode_scavengedatabasecompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_scavengedatabasecompleted(Record)
    when is_record(Record, scavengedatabasecompleted) ->
    encode(scavengedatabasecompleted, Record).

encode_scavengedatabase(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_scavengedatabase(Record)
    when is_record(Record, scavengedatabase) ->
    encode(scavengedatabase, Record).

encode_nothandled(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_nothandled(Record)
    when is_record(Record, nothandled) ->
    encode(nothandled, Record).

encode_subscriptiondropped(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_subscriptiondropped(Record)
    when is_record(Record, subscriptiondropped) ->
    encode(subscriptiondropped, Record).

encode_unsubscribefromstream(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_unsubscribefromstream(Record)
    when is_record(Record, unsubscribefromstream) ->
    encode(unsubscribefromstream, Record).

encode_streameventappeared(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_streameventappeared(Record)
    when is_record(Record, streameventappeared) ->
    encode(streameventappeared, Record).

encode_subscriptionconfirmation(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_subscriptionconfirmation(Record)
    when is_record(Record, subscriptionconfirmation) ->
    encode(subscriptionconfirmation, Record).

encode_subscribetostream(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_subscribetostream(Record)
    when is_record(Record, subscribetostream) ->
    encode(subscribetostream, Record).

encode_readalleventscompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_readalleventscompleted(Record)
    when is_record(Record, readalleventscompleted) ->
    encode(readalleventscompleted, Record).

encode_readallevents(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_readallevents(Record)
    when is_record(Record, readallevents) ->
    encode(readallevents, Record).

encode_readstreameventscompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_readstreameventscompleted(Record)
    when is_record(Record, readstreameventscompleted) ->
    encode(readstreameventscompleted, Record).

encode_readstreamevents(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_readstreamevents(Record)
    when is_record(Record, readstreamevents) ->
    encode(readstreamevents, Record).

encode_readeventcompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_readeventcompleted(Record)
    when is_record(Record, readeventcompleted) ->
    encode(readeventcompleted, Record).

encode_readevent(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_readevent(Record)
    when is_record(Record, readevent) ->
    encode(readevent, Record).

encode_transactioncommitcompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_transactioncommitcompleted(Record)
    when is_record(Record, transactioncommitcompleted) ->
    encode(transactioncommitcompleted, Record).

encode_transactioncommit(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_transactioncommit(Record)
    when is_record(Record, transactioncommit) ->
    encode(transactioncommit, Record).

encode_transactionwritecompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_transactionwritecompleted(Record)
    when is_record(Record, transactionwritecompleted) ->
    encode(transactionwritecompleted, Record).

encode_transactionwrite(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_transactionwrite(Record)
    when is_record(Record, transactionwrite) ->
    encode(transactionwrite, Record).

encode_transactionstartcompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_transactionstartcompleted(Record)
    when is_record(Record, transactionstartcompleted) ->
    encode(transactionstartcompleted, Record).

encode_transactionstart(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_transactionstart(Record)
    when is_record(Record, transactionstart) ->
    encode(transactionstart, Record).

encode_deletestreamcompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_deletestreamcompleted(Record)
    when is_record(Record, deletestreamcompleted) ->
    encode(deletestreamcompleted, Record).

encode_deletestream(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_deletestream(Record)
    when is_record(Record, deletestream) ->
    encode(deletestream, Record).

encode_writeeventscompleted(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_writeeventscompleted(Record)
    when is_record(Record, writeeventscompleted) ->
    encode(writeeventscompleted, Record).

encode_writeevents(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_writeevents(Record)
    when is_record(Record, writeevents) ->
    encode(writeevents, Record).

encode_resolvedevent(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_resolvedevent(Record)
    when is_record(Record, resolvedevent) ->
    encode(resolvedevent, Record).

encode_resolvedindexedevent(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_resolvedindexedevent(Record)
    when is_record(Record, resolvedindexedevent) ->
    encode(resolvedindexedevent, Record).

encode_eventrecord(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_eventrecord(Record)
    when is_record(Record, eventrecord) ->
    encode(eventrecord, Record).

encode_newevent(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_newevent(Record)
    when is_record(Record, newevent) ->
    encode(newevent, Record).

encode(newevent, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(newevent, Record) ->
    [iolist(newevent, Record) | encode_extensions(Record)];
encode(eventrecord, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(eventrecord, Record) ->
    [iolist(eventrecord, Record)
     | encode_extensions(Record)];
encode(resolvedindexedevent, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(resolvedindexedevent, Record) ->
    [iolist(resolvedindexedevent, Record)
     | encode_extensions(Record)];
encode(resolvedevent, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(resolvedevent, Record) ->
    [iolist(resolvedevent, Record)
     | encode_extensions(Record)];
encode(writeevents, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(writeevents, Record) ->
    [iolist(writeevents, Record)
     | encode_extensions(Record)];
encode(writeeventscompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(writeeventscompleted, Record) ->
    [iolist(writeeventscompleted, Record)
     | encode_extensions(Record)];
encode(deletestream, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(deletestream, Record) ->
    [iolist(deletestream, Record)
     | encode_extensions(Record)];
encode(deletestreamcompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(deletestreamcompleted, Record) ->
    [iolist(deletestreamcompleted, Record)
     | encode_extensions(Record)];
encode(transactionstart, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(transactionstart, Record) ->
    [iolist(transactionstart, Record)
     | encode_extensions(Record)];
encode(transactionstartcompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(transactionstartcompleted, Record) ->
    [iolist(transactionstartcompleted, Record)
     | encode_extensions(Record)];
encode(transactionwrite, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(transactionwrite, Record) ->
    [iolist(transactionwrite, Record)
     | encode_extensions(Record)];
encode(transactionwritecompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(transactionwritecompleted, Record) ->
    [iolist(transactionwritecompleted, Record)
     | encode_extensions(Record)];
encode(transactioncommit, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(transactioncommit, Record) ->
    [iolist(transactioncommit, Record)
     | encode_extensions(Record)];
encode(transactioncommitcompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(transactioncommitcompleted, Record) ->
    [iolist(transactioncommitcompleted, Record)
     | encode_extensions(Record)];
encode(readevent, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(readevent, Record) ->
    [iolist(readevent, Record) | encode_extensions(Record)];
encode(readeventcompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(readeventcompleted, Record) ->
    [iolist(readeventcompleted, Record)
     | encode_extensions(Record)];
encode(readstreamevents, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(readstreamevents, Record) ->
    [iolist(readstreamevents, Record)
     | encode_extensions(Record)];
encode(readstreameventscompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(readstreameventscompleted, Record) ->
    [iolist(readstreameventscompleted, Record)
     | encode_extensions(Record)];
encode(readallevents, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(readallevents, Record) ->
    [iolist(readallevents, Record)
     | encode_extensions(Record)];
encode(readalleventscompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(readalleventscompleted, Record) ->
    [iolist(readalleventscompleted, Record)
     | encode_extensions(Record)];
encode(subscribetostream, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(subscribetostream, Record) ->
    [iolist(subscribetostream, Record)
     | encode_extensions(Record)];
encode(subscriptionconfirmation, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(subscriptionconfirmation, Record) ->
    [iolist(subscriptionconfirmation, Record)
     | encode_extensions(Record)];
encode(streameventappeared, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(streameventappeared, Record) ->
    [iolist(streameventappeared, Record)
     | encode_extensions(Record)];
encode(unsubscribefromstream, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(unsubscribefromstream, Record) ->
    [iolist(unsubscribefromstream, Record)
     | encode_extensions(Record)];
encode(subscriptiondropped, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(subscriptiondropped, Record) ->
    [iolist(subscriptiondropped, Record)
     | encode_extensions(Record)];
encode(nothandled, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(nothandled, Record) ->
    [iolist(nothandled, Record)
     | encode_extensions(Record)];
encode(scavengedatabase, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(scavengedatabase, Record) ->
    [iolist(scavengedatabase, Record)
     | encode_extensions(Record)];
encode(scavengedatabasecompleted, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(scavengedatabasecompleted, Record) ->
    [iolist(scavengedatabasecompleted, Record)
     | encode_extensions(Record)];
encode(nothandled_masterinfo, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(nothandled_masterinfo, Record) ->
    [iolist(nothandled_masterinfo, Record)
     | encode_extensions(Record)].

encode_extensions(_) -> [].

delimited_encode(Records) ->
    lists:map(fun (Record) ->
		      IoRec = encode(Record),
		      Size = iolist_size(IoRec),
		      [protobuffs:encode_varint(Size), IoRec]
	      end,
	      Records).

iolist(newevent, Record) ->
    [pack(1, required,
	  with_default(Record#newevent.event_id, none), bytes,
	  []),
     pack(2, required,
	  with_default(Record#newevent.event_type, none), string,
	  []),
     pack(3, required,
	  with_default(Record#newevent.data_content_type, none),
	  int32, []),
     pack(4, required,
	  with_default(Record#newevent.metadata_content_type,
		       none),
	  int32, []),
     pack(5, required,
	  with_default(Record#newevent.data, none), bytes, []),
     pack(6, optional,
	  with_default(Record#newevent.metadata, none), bytes,
	  [])];
iolist(eventrecord, Record) ->
    [pack(1, required,
	  with_default(Record#eventrecord.event_stream_id, none),
	  string, []),
     pack(2, required,
	  with_default(Record#eventrecord.event_number, none),
	  int32, []),
     pack(3, required,
	  with_default(Record#eventrecord.event_id, none), bytes,
	  []),
     pack(4, required,
	  with_default(Record#eventrecord.event_type, none),
	  string, []),
     pack(5, required,
	  with_default(Record#eventrecord.data_content_type,
		       none),
	  int32, []),
     pack(6, required,
	  with_default(Record#eventrecord.metadata_content_type,
		       none),
	  int32, []),
     pack(7, required,
	  with_default(Record#eventrecord.data, none), bytes, []),
     pack(8, optional,
	  with_default(Record#eventrecord.metadata, none), bytes,
	  [])];
iolist(resolvedindexedevent, Record) ->
    [pack(1, required,
	  with_default(Record#resolvedindexedevent.event, none),
	  eventrecord, []),
     pack(2, optional,
	  with_default(Record#resolvedindexedevent.link, none),
	  eventrecord, [])];
iolist(resolvedevent, Record) ->
    [pack(1, required,
	  with_default(Record#resolvedevent.event, none),
	  eventrecord, []),
     pack(2, optional,
	  with_default(Record#resolvedevent.link, none),
	  eventrecord, []),
     pack(3, required,
	  with_default(Record#resolvedevent.commit_position,
		       none),
	  int64, []),
     pack(4, required,
	  with_default(Record#resolvedevent.prepare_position,
		       none),
	  int64, [])];
iolist(writeevents, Record) ->
    [pack(1, required,
	  with_default(Record#writeevents.event_stream_id, none),
	  string, []),
     pack(2, required,
	  with_default(Record#writeevents.expected_version, none),
	  int32, []),
     pack(3, repeated,
	  with_default(Record#writeevents.events, none), newevent,
	  []),
     pack(4, required,
	  with_default(Record#writeevents.require_master, none),
	  bool, [])];
iolist(writeeventscompleted, Record) ->
    [pack(1, required,
	  with_default(Record#writeeventscompleted.result, none),
	  operationresult, []),
     pack(2, optional,
	  with_default(Record#writeeventscompleted.message, none),
	  string, []),
     pack(3, required,
	  with_default(Record#writeeventscompleted.first_event_number,
		       none),
	  int32, [])];
iolist(deletestream, Record) ->
    [pack(1, required,
	  with_default(Record#deletestream.event_stream_id, none),
	  string, []),
     pack(2, required,
	  with_default(Record#deletestream.expected_version,
		       none),
	  int32, []),
     pack(3, required,
	  with_default(Record#deletestream.require_master, none),
	  bool, [])];
iolist(deletestreamcompleted, Record) ->
    [pack(1, required,
	  with_default(Record#deletestreamcompleted.result, none),
	  operationresult, []),
     pack(2, optional,
	  with_default(Record#deletestreamcompleted.message,
		       none),
	  string, [])];
iolist(transactionstart, Record) ->
    [pack(1, required,
	  with_default(Record#transactionstart.event_stream_id,
		       none),
	  string, []),
     pack(2, required,
	  with_default(Record#transactionstart.expected_version,
		       none),
	  int32, []),
     pack(3, required,
	  with_default(Record#transactionstart.require_master,
		       none),
	  bool, [])];
iolist(transactionstartcompleted, Record) ->
    [pack(1, required,
	  with_default(Record#transactionstartcompleted.transaction_id,
		       none),
	  int64, []),
     pack(2, required,
	  with_default(Record#transactionstartcompleted.result,
		       none),
	  operationresult, []),
     pack(3, optional,
	  with_default(Record#transactionstartcompleted.message,
		       none),
	  string, [])];
iolist(transactionwrite, Record) ->
    [pack(1, required,
	  with_default(Record#transactionwrite.transaction_id,
		       none),
	  int64, []),
     pack(2, repeated,
	  with_default(Record#transactionwrite.events, none),
	  newevent, []),
     pack(3, required,
	  with_default(Record#transactionwrite.require_master,
		       none),
	  bool, [])];
iolist(transactionwritecompleted, Record) ->
    [pack(1, required,
	  with_default(Record#transactionwritecompleted.transaction_id,
		       none),
	  int64, []),
     pack(2, required,
	  with_default(Record#transactionwritecompleted.result,
		       none),
	  operationresult, []),
     pack(3, optional,
	  with_default(Record#transactionwritecompleted.message,
		       none),
	  string, [])];
iolist(transactioncommit, Record) ->
    [pack(1, required,
	  with_default(Record#transactioncommit.transaction_id,
		       none),
	  int64, []),
     pack(2, required,
	  with_default(Record#transactioncommit.require_master,
		       none),
	  bool, [])];
iolist(transactioncommitcompleted, Record) ->
    [pack(1, required,
	  with_default(Record#transactioncommitcompleted.transaction_id,
		       none),
	  int64, []),
     pack(2, required,
	  with_default(Record#transactioncommitcompleted.result,
		       none),
	  operationresult, []),
     pack(3, optional,
	  with_default(Record#transactioncommitcompleted.message,
		       none),
	  string, [])];
iolist(readevent, Record) ->
    [pack(1, required,
	  with_default(Record#readevent.event_stream_id, none),
	  string, []),
     pack(2, required,
	  with_default(Record#readevent.event_number, none),
	  int32, []),
     pack(3, required,
	  with_default(Record#readevent.resolve_link_tos, none),
	  bool, []),
     pack(4, required,
	  with_default(Record#readevent.require_master, none),
	  bool, [])];
iolist(readeventcompleted, Record) ->
    [pack(1, required,
	  with_default(Record#readeventcompleted.result, none),
	  readeventcompleted_readeventresult, []),
     pack(2, required,
	  with_default(Record#readeventcompleted.event, none),
	  resolvedindexedevent, []),
     pack(3, optional,
	  with_default(Record#readeventcompleted.error, none),
	  string, [])];
iolist(readstreamevents, Record) ->
    [pack(1, required,
	  with_default(Record#readstreamevents.event_stream_id,
		       none),
	  string, []),
     pack(2, required,
	  with_default(Record#readstreamevents.from_event_number,
		       none),
	  int32, []),
     pack(3, required,
	  with_default(Record#readstreamevents.max_count, none),
	  int32, []),
     pack(4, required,
	  with_default(Record#readstreamevents.resolve_link_tos,
		       none),
	  bool, []),
     pack(5, required,
	  with_default(Record#readstreamevents.require_master,
		       none),
	  bool, [])];
iolist(readstreameventscompleted, Record) ->
    [pack(1, repeated,
	  with_default(Record#readstreameventscompleted.events,
		       none),
	  resolvedindexedevent, []),
     pack(2, required,
	  with_default(Record#readstreameventscompleted.result,
		       none),
	  readstreameventscompleted_readstreamresult, []),
     pack(3, required,
	  with_default(Record#readstreameventscompleted.next_event_number,
		       none),
	  int32, []),
     pack(4, required,
	  with_default(Record#readstreameventscompleted.last_event_number,
		       none),
	  int32, []),
     pack(5, required,
	  with_default(Record#readstreameventscompleted.is_end_of_stream,
		       none),
	  bool, []),
     pack(6, required,
	  with_default(Record#readstreameventscompleted.last_commit_position,
		       none),
	  int64, []),
     pack(7, optional,
	  with_default(Record#readstreameventscompleted.error,
		       none),
	  string, [])];
iolist(readallevents, Record) ->
    [pack(1, required,
	  with_default(Record#readallevents.commit_position,
		       none),
	  int64, []),
     pack(2, required,
	  with_default(Record#readallevents.prepare_position,
		       none),
	  int64, []),
     pack(3, required,
	  with_default(Record#readallevents.max_count, none),
	  int32, []),
     pack(4, required,
	  with_default(Record#readallevents.resolve_link_tos,
		       none),
	  bool, []),
     pack(5, required,
	  with_default(Record#readallevents.require_master, none),
	  bool, [])];
iolist(readalleventscompleted, Record) ->
    [pack(1, required,
	  with_default(Record#readalleventscompleted.commit_position,
		       none),
	  int64, []),
     pack(2, required,
	  with_default(Record#readalleventscompleted.prepare_position,
		       none),
	  int64, []),
     pack(3, repeated,
	  with_default(Record#readalleventscompleted.events,
		       none),
	  resolvedevent, []),
     pack(4, required,
	  with_default(Record#readalleventscompleted.next_commit_position,
		       none),
	  int64, []),
     pack(5, required,
	  with_default(Record#readalleventscompleted.next_prepare_position,
		       none),
	  int64, []),
     pack(6, optional,
	  with_default(Record#readalleventscompleted.result,
		       'Success'),
	  readalleventscompleted_readallresult, []),
     pack(7, optional,
	  with_default(Record#readalleventscompleted.error, none),
	  string, [])];
iolist(subscribetostream, Record) ->
    [pack(1, required,
	  with_default(Record#subscribetostream.event_stream_id,
		       none),
	  string, []),
     pack(2, required,
	  with_default(Record#subscribetostream.resolve_link_tos,
		       none),
	  bool, [])];
iolist(subscriptionconfirmation, Record) ->
    [pack(1, required,
	  with_default(Record#subscriptionconfirmation.last_commit_position,
		       none),
	  int64, []),
     pack(2, optional,
	  with_default(Record#subscriptionconfirmation.last_event_number,
		       none),
	  int32, [])];
iolist(streameventappeared, Record) ->
    [pack(1, required,
	  with_default(Record#streameventappeared.event, none),
	  resolvedevent, [])];
iolist(unsubscribefromstream, _Record) -> [];
iolist(subscriptiondropped, Record) ->
    [pack(1, optional,
	  with_default(Record#subscriptiondropped.reason,
		       'Unsubscribed'),
	  subscriptiondropped_subscriptiondropreason, [])];
iolist(nothandled, Record) ->
    [pack(1, required,
	  with_default(Record#nothandled.reason, none),
	  nothandled_nothandledreason, []),
     pack(2, optional,
	  with_default(Record#nothandled.additional_info, none),
	  bytes, [])];
iolist(scavengedatabase, _Record) -> [];
iolist(scavengedatabasecompleted, Record) ->
    [pack(1, required,
	  with_default(Record#scavengedatabasecompleted.result,
		       none),
	  scavengedatabasecompleted_scavengeresult, []),
     pack(2, optional,
	  with_default(Record#scavengedatabasecompleted.error,
		       none),
	  string, []),
     pack(3, required,
	  with_default(Record#scavengedatabasecompleted.total_time_ms,
		       none),
	  int32, []),
     pack(4, required,
	  with_default(Record#scavengedatabasecompleted.total_space_saved,
		       none),
	  int64, [])];
iolist(nothandled_masterinfo, Record) ->
    [pack(1, required,
	  with_default(Record#nothandled_masterinfo.external_tcp_address,
		       none),
	  string, []),
     pack(2, required,
	  with_default(Record#nothandled_masterinfo.external_tcp_port,
		       none),
	  int32, []),
     pack(3, required,
	  with_default(Record#nothandled_masterinfo.external_http_address,
		       none),
	  string, []),
     pack(4, required,
	  with_default(Record#nothandled_masterinfo.external_http_port,
		       none),
	  int32, []),
     pack(5, optional,
	  with_default(Record#nothandled_masterinfo.external_secure_tcp_address,
		       none),
	  string, []),
     pack(6, optional,
	  with_default(Record#nothandled_masterinfo.external_secure_tcp_port,
		       none),
	  int32, [])].

with_default(Default, Default) -> undefined;
with_default(Val, _) -> Val.

pack(_, optional, undefined, _, _) -> [];
pack(_, repeated, undefined, _, _) -> [];
pack(_, repeated_packed, undefined, _, _) -> [];
pack(_, repeated_packed, [], _, _) -> [];
pack(FNum, required, undefined, Type, _) ->
    exit({error,
	  {required_field_is_undefined, FNum, Type}});
pack(_, repeated, [], _, Acc) -> lists:reverse(Acc);
pack(FNum, repeated, [Head | Tail], Type, Acc) ->
    pack(FNum, repeated, Tail, Type,
	 [pack(FNum, optional, Head, Type, []) | Acc]);
pack(FNum, repeated_packed, Data, Type, _) ->
    protobuffs:encode_packed(FNum, Data, Type);
pack(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName | _] = tuple_to_list(Data),
    protobuffs:encode(FNum, encode(RecName, Data), bytes);
pack(FNum, _, Data, Type, _)
    when Type =:= bool;
	 Type =:= int32;
	 Type =:= uint32;
	 Type =:= int64;
	 Type =:= uint64;
	 Type =:= sint32;
	 Type =:= sint64;
	 Type =:= fixed32;
	 Type =:= sfixed32;
	 Type =:= fixed64;
	 Type =:= sfixed64;
	 Type =:= string;
	 Type =:= bytes;
	 Type =:= float;
	 Type =:= double ->
    protobuffs:encode(FNum, Data, Type);
pack(FNum, _, Data, Type, _) when is_atom(Data) ->
    protobuffs:encode(FNum, enum_to_int(Type, Data), enum).

enum_to_int(scavengedatabasecompleted_scavengeresult,
	    'Failed') ->
    2;
enum_to_int(scavengedatabasecompleted_scavengeresult,
	    'InProgress') ->
    1;
enum_to_int(scavengedatabasecompleted_scavengeresult,
	    'Success') ->
    0;
enum_to_int(nothandled_nothandledreason, 'NotMaster') ->
    2;
enum_to_int(nothandled_nothandledreason, 'TooBusy') ->
    1;
enum_to_int(nothandled_nothandledreason, 'NotReady') ->
    0;
enum_to_int(subscriptiondropped_subscriptiondropreason,
	    'AccessDenied') ->
    1;
enum_to_int(subscriptiondropped_subscriptiondropreason,
	    'Unsubscribed') ->
    0;
enum_to_int(readalleventscompleted_readallresult,
	    'AccessDenied') ->
    3;
enum_to_int(readalleventscompleted_readallresult,
	    'Error') ->
    2;
enum_to_int(readalleventscompleted_readallresult,
	    'NotModified') ->
    1;
enum_to_int(readalleventscompleted_readallresult,
	    'Success') ->
    0;
enum_to_int(readstreameventscompleted_readstreamresult,
	    'AccessDenied') ->
    5;
enum_to_int(readstreameventscompleted_readstreamresult,
	    'Error') ->
    4;
enum_to_int(readstreameventscompleted_readstreamresult,
	    'NotModified') ->
    3;
enum_to_int(readstreameventscompleted_readstreamresult,
	    'StreamDeleted') ->
    2;
enum_to_int(readstreameventscompleted_readstreamresult,
	    'NoStream') ->
    1;
enum_to_int(readstreameventscompleted_readstreamresult,
	    'Success') ->
    0;
enum_to_int(readeventcompleted_readeventresult,
	    'AccessDenied') ->
    5;
enum_to_int(readeventcompleted_readeventresult,
	    'Error') ->
    4;
enum_to_int(readeventcompleted_readeventresult,
	    'StreamDeleted') ->
    3;
enum_to_int(readeventcompleted_readeventresult,
	    'NoStream') ->
    2;
enum_to_int(readeventcompleted_readeventresult,
	    'NotFound') ->
    1;
enum_to_int(readeventcompleted_readeventresult,
	    'Success') ->
    0;
enum_to_int(operationresult, 'AccessDenied') -> 7;
enum_to_int(operationresult, 'InvalidTransaction') -> 6;
enum_to_int(operationresult, 'StreamDeleted') -> 5;
enum_to_int(operationresult, 'WrongExpectedVersion') ->
    4;
enum_to_int(operationresult, 'ForwardTimeout') -> 3;
enum_to_int(operationresult, 'CommitTimeout') -> 2;
enum_to_int(operationresult, 'PrepareTimeout') -> 1;
enum_to_int(operationresult, 'Success') -> 0.

int_to_enum(scavengedatabasecompleted_scavengeresult,
	    2) ->
    'Failed';
int_to_enum(scavengedatabasecompleted_scavengeresult,
	    1) ->
    'InProgress';
int_to_enum(scavengedatabasecompleted_scavengeresult,
	    0) ->
    'Success';
int_to_enum(nothandled_nothandledreason, 2) ->
    'NotMaster';
int_to_enum(nothandled_nothandledreason, 1) ->
    'TooBusy';
int_to_enum(nothandled_nothandledreason, 0) ->
    'NotReady';
int_to_enum(subscriptiondropped_subscriptiondropreason,
	    1) ->
    'AccessDenied';
int_to_enum(subscriptiondropped_subscriptiondropreason,
	    0) ->
    'Unsubscribed';
int_to_enum(readalleventscompleted_readallresult, 3) ->
    'AccessDenied';
int_to_enum(readalleventscompleted_readallresult, 2) ->
    'Error';
int_to_enum(readalleventscompleted_readallresult, 1) ->
    'NotModified';
int_to_enum(readalleventscompleted_readallresult, 0) ->
    'Success';
int_to_enum(readstreameventscompleted_readstreamresult,
	    5) ->
    'AccessDenied';
int_to_enum(readstreameventscompleted_readstreamresult,
	    4) ->
    'Error';
int_to_enum(readstreameventscompleted_readstreamresult,
	    3) ->
    'NotModified';
int_to_enum(readstreameventscompleted_readstreamresult,
	    2) ->
    'StreamDeleted';
int_to_enum(readstreameventscompleted_readstreamresult,
	    1) ->
    'NoStream';
int_to_enum(readstreameventscompleted_readstreamresult,
	    0) ->
    'Success';
int_to_enum(readeventcompleted_readeventresult, 5) ->
    'AccessDenied';
int_to_enum(readeventcompleted_readeventresult, 4) ->
    'Error';
int_to_enum(readeventcompleted_readeventresult, 3) ->
    'StreamDeleted';
int_to_enum(readeventcompleted_readeventresult, 2) ->
    'NoStream';
int_to_enum(readeventcompleted_readeventresult, 1) ->
    'NotFound';
int_to_enum(readeventcompleted_readeventresult, 0) ->
    'Success';
int_to_enum(operationresult, 7) -> 'AccessDenied';
int_to_enum(operationresult, 6) -> 'InvalidTransaction';
int_to_enum(operationresult, 5) -> 'StreamDeleted';
int_to_enum(operationresult, 4) ->
    'WrongExpectedVersion';
int_to_enum(operationresult, 3) -> 'ForwardTimeout';
int_to_enum(operationresult, 2) -> 'CommitTimeout';
int_to_enum(operationresult, 1) -> 'PrepareTimeout';
int_to_enum(operationresult, 0) -> 'Success';
int_to_enum(_, Val) -> Val.

decode_nothandled_masterinfo(Bytes)
    when is_binary(Bytes) ->
    decode(nothandled_masterinfo, Bytes).

decode_scavengedatabasecompleted(Bytes)
    when is_binary(Bytes) ->
    decode(scavengedatabasecompleted, Bytes).

decode_scavengedatabase(Bytes) when is_binary(Bytes) ->
    decode(scavengedatabase, Bytes).

decode_nothandled(Bytes) when is_binary(Bytes) ->
    decode(nothandled, Bytes).

decode_subscriptiondropped(Bytes)
    when is_binary(Bytes) ->
    decode(subscriptiondropped, Bytes).

decode_unsubscribefromstream(Bytes)
    when is_binary(Bytes) ->
    decode(unsubscribefromstream, Bytes).

decode_streameventappeared(Bytes)
    when is_binary(Bytes) ->
    decode(streameventappeared, Bytes).

decode_subscriptionconfirmation(Bytes)
    when is_binary(Bytes) ->
    decode(subscriptionconfirmation, Bytes).

decode_subscribetostream(Bytes) when is_binary(Bytes) ->
    decode(subscribetostream, Bytes).

decode_readalleventscompleted(Bytes)
    when is_binary(Bytes) ->
    decode(readalleventscompleted, Bytes).

decode_readallevents(Bytes) when is_binary(Bytes) ->
    decode(readallevents, Bytes).

decode_readstreameventscompleted(Bytes)
    when is_binary(Bytes) ->
    decode(readstreameventscompleted, Bytes).

decode_readstreamevents(Bytes) when is_binary(Bytes) ->
    decode(readstreamevents, Bytes).

decode_readeventcompleted(Bytes)
    when is_binary(Bytes) ->
    decode(readeventcompleted, Bytes).

decode_readevent(Bytes) when is_binary(Bytes) ->
    decode(readevent, Bytes).

decode_transactioncommitcompleted(Bytes)
    when is_binary(Bytes) ->
    decode(transactioncommitcompleted, Bytes).

decode_transactioncommit(Bytes) when is_binary(Bytes) ->
    decode(transactioncommit, Bytes).

decode_transactionwritecompleted(Bytes)
    when is_binary(Bytes) ->
    decode(transactionwritecompleted, Bytes).

decode_transactionwrite(Bytes) when is_binary(Bytes) ->
    decode(transactionwrite, Bytes).

decode_transactionstartcompleted(Bytes)
    when is_binary(Bytes) ->
    decode(transactionstartcompleted, Bytes).

decode_transactionstart(Bytes) when is_binary(Bytes) ->
    decode(transactionstart, Bytes).

decode_deletestreamcompleted(Bytes)
    when is_binary(Bytes) ->
    decode(deletestreamcompleted, Bytes).

decode_deletestream(Bytes) when is_binary(Bytes) ->
    decode(deletestream, Bytes).

decode_writeeventscompleted(Bytes)
    when is_binary(Bytes) ->
    decode(writeeventscompleted, Bytes).

decode_writeevents(Bytes) when is_binary(Bytes) ->
    decode(writeevents, Bytes).

decode_resolvedevent(Bytes) when is_binary(Bytes) ->
    decode(resolvedevent, Bytes).

decode_resolvedindexedevent(Bytes)
    when is_binary(Bytes) ->
    decode(resolvedindexedevent, Bytes).

decode_eventrecord(Bytes) when is_binary(Bytes) ->
    decode(eventrecord, Bytes).

decode_newevent(Bytes) when is_binary(Bytes) ->
    decode(newevent, Bytes).

delimited_decode_newevent(Bytes) ->
    delimited_decode(newevent, Bytes).

delimited_decode_eventrecord(Bytes) ->
    delimited_decode(eventrecord, Bytes).

delimited_decode_resolvedindexedevent(Bytes) ->
    delimited_decode(resolvedindexedevent, Bytes).

delimited_decode_resolvedevent(Bytes) ->
    delimited_decode(resolvedevent, Bytes).

delimited_decode_writeevents(Bytes) ->
    delimited_decode(writeevents, Bytes).

delimited_decode_writeeventscompleted(Bytes) ->
    delimited_decode(writeeventscompleted, Bytes).

delimited_decode_deletestream(Bytes) ->
    delimited_decode(deletestream, Bytes).

delimited_decode_deletestreamcompleted(Bytes) ->
    delimited_decode(deletestreamcompleted, Bytes).

delimited_decode_transactionstart(Bytes) ->
    delimited_decode(transactionstart, Bytes).

delimited_decode_transactionstartcompleted(Bytes) ->
    delimited_decode(transactionstartcompleted, Bytes).

delimited_decode_transactionwrite(Bytes) ->
    delimited_decode(transactionwrite, Bytes).

delimited_decode_transactionwritecompleted(Bytes) ->
    delimited_decode(transactionwritecompleted, Bytes).

delimited_decode_transactioncommit(Bytes) ->
    delimited_decode(transactioncommit, Bytes).

delimited_decode_transactioncommitcompleted(Bytes) ->
    delimited_decode(transactioncommitcompleted, Bytes).

delimited_decode_readevent(Bytes) ->
    delimited_decode(readevent, Bytes).

delimited_decode_readeventcompleted(Bytes) ->
    delimited_decode(readeventcompleted, Bytes).

delimited_decode_readstreamevents(Bytes) ->
    delimited_decode(readstreamevents, Bytes).

delimited_decode_readstreameventscompleted(Bytes) ->
    delimited_decode(readstreameventscompleted, Bytes).

delimited_decode_readallevents(Bytes) ->
    delimited_decode(readallevents, Bytes).

delimited_decode_readalleventscompleted(Bytes) ->
    delimited_decode(readalleventscompleted, Bytes).

delimited_decode_subscribetostream(Bytes) ->
    delimited_decode(subscribetostream, Bytes).

delimited_decode_subscriptionconfirmation(Bytes) ->
    delimited_decode(subscriptionconfirmation, Bytes).

delimited_decode_streameventappeared(Bytes) ->
    delimited_decode(streameventappeared, Bytes).

delimited_decode_unsubscribefromstream(Bytes) ->
    delimited_decode(unsubscribefromstream, Bytes).

delimited_decode_subscriptiondropped(Bytes) ->
    delimited_decode(subscriptiondropped, Bytes).

delimited_decode_nothandled(Bytes) ->
    delimited_decode(nothandled, Bytes).

delimited_decode_scavengedatabase(Bytes) ->
    delimited_decode(scavengedatabase, Bytes).

delimited_decode_scavengedatabasecompleted(Bytes) ->
    delimited_decode(scavengedatabasecompleted, Bytes).

delimited_decode_nothandled_masterinfo(Bytes) ->
    delimited_decode(nothandled_masterinfo, Bytes).

delimited_decode(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode(Type, Bytes, []).

delimited_decode(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
      {Size, Rest} when size(Rest) < Size ->
	  {lists:reverse(Acc), Bytes};
      {Size, Rest} ->
	  <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
	  Message = decode(Type, MessageBytes),
	  delimited_decode(Type, Rest2, [Message | Acc])
    catch
      _What:_Why -> {lists:reverse(Acc), Bytes}
    end.

decode(enummsg_values, 1) -> value1;
decode(newevent, Bytes) when is_binary(Bytes) ->
    Types = [{6, metadata, bytes, []}, {5, data, bytes, []},
	     {4, metadata_content_type, int32, []},
	     {3, data_content_type, int32, []},
	     {2, event_type, string, []}, {1, event_id, bytes, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(newevent, Decoded);
decode(eventrecord, Bytes) when is_binary(Bytes) ->
    Types = [{8, metadata, bytes, []}, {7, data, bytes, []},
	     {6, metadata_content_type, int32, []},
	     {5, data_content_type, int32, []},
	     {4, event_type, string, []}, {3, event_id, bytes, []},
	     {2, event_number, int32, []},
	     {1, event_stream_id, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(eventrecord, Decoded);
decode(resolvedindexedevent, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, link, eventrecord, [is_record]},
	     {1, event, eventrecord, [is_record]}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(resolvedindexedevent, Decoded);
decode(resolvedevent, Bytes) when is_binary(Bytes) ->
    Types = [{4, prepare_position, int64, []},
	     {3, commit_position, int64, []},
	     {2, link, eventrecord, [is_record]},
	     {1, event, eventrecord, [is_record]}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(resolvedevent, Decoded);
decode(writeevents, Bytes) when is_binary(Bytes) ->
    Types = [{4, require_master, bool, []},
	     {3, events, newevent, [is_record, repeated]},
	     {2, expected_version, int32, []},
	     {1, event_stream_id, string, []}],
    Defaults = [{3, events, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(writeevents, Decoded);
decode(writeeventscompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, first_event_number, int32, []},
	     {2, message, string, []},
	     {1, result, operationresult, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(writeeventscompleted, Decoded);
decode(deletestream, Bytes) when is_binary(Bytes) ->
    Types = [{3, require_master, bool, []},
	     {2, expected_version, int32, []},
	     {1, event_stream_id, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(deletestream, Decoded);
decode(deletestreamcompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, message, string, []},
	     {1, result, operationresult, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(deletestreamcompleted, Decoded);
decode(transactionstart, Bytes) when is_binary(Bytes) ->
    Types = [{3, require_master, bool, []},
	     {2, expected_version, int32, []},
	     {1, event_stream_id, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(transactionstart, Decoded);
decode(transactionstartcompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, message, string, []},
	     {2, result, operationresult, []},
	     {1, transaction_id, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(transactionstartcompleted, Decoded);
decode(transactionwrite, Bytes) when is_binary(Bytes) ->
    Types = [{3, require_master, bool, []},
	     {2, events, newevent, [is_record, repeated]},
	     {1, transaction_id, int64, []}],
    Defaults = [{2, events, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(transactionwrite, Decoded);
decode(transactionwritecompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, message, string, []},
	     {2, result, operationresult, []},
	     {1, transaction_id, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(transactionwritecompleted, Decoded);
decode(transactioncommit, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, require_master, bool, []},
	     {1, transaction_id, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(transactioncommit, Decoded);
decode(transactioncommitcompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, message, string, []},
	     {2, result, operationresult, []},
	     {1, transaction_id, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(transactioncommitcompleted, Decoded);
decode(readevent, Bytes) when is_binary(Bytes) ->
    Types = [{4, require_master, bool, []},
	     {3, resolve_link_tos, bool, []},
	     {2, event_number, int32, []},
	     {1, event_stream_id, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(readevent, Decoded);
decode(readeventcompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{3, error, string, []},
	     {2, event, resolvedindexedevent, [is_record]},
	     {1, result, readeventcompleted_readeventresult, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(readeventcompleted, Decoded);
decode(readstreamevents, Bytes) when is_binary(Bytes) ->
    Types = [{5, require_master, bool, []},
	     {4, resolve_link_tos, bool, []},
	     {3, max_count, int32, []},
	     {2, from_event_number, int32, []},
	     {1, event_stream_id, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(readstreamevents, Decoded);
decode(readstreameventscompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{7, error, string, []},
	     {6, last_commit_position, int64, []},
	     {5, is_end_of_stream, bool, []},
	     {4, last_event_number, int32, []},
	     {3, next_event_number, int32, []},
	     {2, result, readstreameventscompleted_readstreamresult,
	      []},
	     {1, events, resolvedindexedevent,
	      [is_record, repeated]}],
    Defaults = [{1, events, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(readstreameventscompleted, Decoded);
decode(readallevents, Bytes) when is_binary(Bytes) ->
    Types = [{5, require_master, bool, []},
	     {4, resolve_link_tos, bool, []},
	     {3, max_count, int32, []},
	     {2, prepare_position, int64, []},
	     {1, commit_position, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(readallevents, Decoded);
decode(readalleventscompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{7, error, string, []},
	     {6, result, readalleventscompleted_readallresult, []},
	     {5, next_prepare_position, int64, []},
	     {4, next_commit_position, int64, []},
	     {3, events, resolvedevent, [is_record, repeated]},
	     {2, prepare_position, int64, []},
	     {1, commit_position, int64, []}],
    Defaults = [{3, events, []}, {6, result, 'Success'}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(readalleventscompleted, Decoded);
decode(subscribetostream, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, resolve_link_tos, bool, []},
	     {1, event_stream_id, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(subscribetostream, Decoded);
decode(subscriptionconfirmation, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, last_event_number, int32, []},
	     {1, last_commit_position, int64, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(subscriptionconfirmation, Decoded);
decode(streameventappeared, Bytes)
    when is_binary(Bytes) ->
    Types = [{1, event, resolvedevent, [is_record]}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(streameventappeared, Decoded);
decode(unsubscribefromstream, Bytes)
    when is_binary(Bytes) ->
    Types = [],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(unsubscribefromstream, Decoded);
decode(subscriptiondropped, Bytes)
    when is_binary(Bytes) ->
    Types = [{1, reason,
	      subscriptiondropped_subscriptiondropreason, []}],
    Defaults = [{1, reason, 'Unsubscribed'}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(subscriptiondropped, Decoded);
decode(nothandled, Bytes) when is_binary(Bytes) ->
    Types = [{2, additional_info, bytes, []},
	     {1, reason, nothandled_nothandledreason, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(nothandled, Decoded);
decode(scavengedatabase, Bytes) when is_binary(Bytes) ->
    Types = [],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(scavengedatabase, Decoded);
decode(scavengedatabasecompleted, Bytes)
    when is_binary(Bytes) ->
    Types = [{4, total_space_saved, int64, []},
	     {3, total_time_ms, int32, []}, {2, error, string, []},
	     {1, result, scavengedatabasecompleted_scavengeresult,
	      []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(scavengedatabasecompleted, Decoded);
decode(nothandled_masterinfo, Bytes)
    when is_binary(Bytes) ->
    Types = [{6, external_secure_tcp_port, int32, []},
	     {5, external_secure_tcp_address, string, []},
	     {4, external_http_port, int32, []},
	     {3, external_http_address, string, []},
	     {2, external_tcp_port, int32, []},
	     {1, external_tcp_address, string, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(nothandled_masterinfo, Decoded).

decode(<<>>, Types, Acc) ->
    reverse_repeated_fields(Acc, Types);
decode(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
    case lists:keyfind(FNum, 1, Types) of
      {FNum, Name, Type, Opts} ->
	  {Value1, Rest1} = case lists:member(is_record, Opts) of
			      true ->
				  {{FNum, V}, R} = protobuffs:decode(Bytes,
								     bytes),
				  RecVal = decode(Type, V),
				  {RecVal, R};
			      false ->
				  case lists:member(repeated_packed, Opts) of
				    true ->
					{{FNum, V}, R} =
					    protobuffs:decode_packed(Bytes,
								     Type),
					{V, R};
				    false ->
					{{FNum, V}, R} =
					    protobuffs:decode(Bytes, Type),
					{unpack_value(V, Type), R}
				  end
			    end,
	  case lists:member(repeated, Opts) of
	    true ->
		case lists:keytake(FNum, 1, Acc) of
		  {value, {FNum, Name, List}, Acc1} ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1) | List]}
			      | Acc1]);
		  false ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1)]} | Acc])
		end;
	    false ->
		decode(Rest1, Types,
		       [{FNum, Name, int_to_enum(Type, Value1)} | Acc])
	  end;
      false ->
	  case lists:keyfind('$extensions', 2, Acc) of
	    {_, _, Dict} ->
		{{FNum, _V}, R} = protobuffs:decode(Bytes, bytes),
		Diff = size(Bytes) - size(R),
		<<V:Diff/binary, _/binary>> = Bytes,
		NewDict = dict:store(FNum, V, Dict),
		NewAcc = lists:keyreplace('$extensions', 2, Acc,
					  {false, '$extensions', NewDict}),
		decode(R, Types, NewAcc);
	    _ ->
		{ok, Skipped} = protobuffs:skip_next_field(Bytes),
		decode(Skipped, Types, Acc)
	  end
    end.

reverse_repeated_fields(FieldList, Types) ->
    [begin
       case lists:keyfind(FNum, 1, Types) of
	 {FNum, Name, _Type, Opts} ->
	     case lists:member(repeated, Opts) of
	       true -> {FNum, Name, lists:reverse(Value)};
	       _ -> Field
	     end;
	 _ -> Field
       end
     end
     || {FNum, Name, Value} = Field <- FieldList].

unpack_value(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
unpack_value(Value, _) -> Value.

to_record(newevent, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       newevent),
						   Record, Name, Val)
			  end,
			  #newevent{}, DecodedTuples),
    Record1;
to_record(eventrecord, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       eventrecord),
						   Record, Name, Val)
			  end,
			  #eventrecord{}, DecodedTuples),
    Record1;
to_record(resolvedindexedevent, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       resolvedindexedevent),
						   Record, Name, Val)
			  end,
			  #resolvedindexedevent{}, DecodedTuples),
    Record1;
to_record(resolvedevent, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       resolvedevent),
						   Record, Name, Val)
			  end,
			  #resolvedevent{}, DecodedTuples),
    Record1;
to_record(writeevents, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       writeevents),
						   Record, Name, Val)
			  end,
			  #writeevents{}, DecodedTuples),
    Record1;
to_record(writeeventscompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       writeeventscompleted),
						   Record, Name, Val)
			  end,
			  #writeeventscompleted{}, DecodedTuples),
    Record1;
to_record(deletestream, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       deletestream),
						   Record, Name, Val)
			  end,
			  #deletestream{}, DecodedTuples),
    Record1;
to_record(deletestreamcompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       deletestreamcompleted),
						   Record, Name, Val)
			  end,
			  #deletestreamcompleted{}, DecodedTuples),
    Record1;
to_record(transactionstart, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       transactionstart),
						   Record, Name, Val)
			  end,
			  #transactionstart{}, DecodedTuples),
    Record1;
to_record(transactionstartcompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       transactionstartcompleted),
						   Record, Name, Val)
			  end,
			  #transactionstartcompleted{}, DecodedTuples),
    Record1;
to_record(transactionwrite, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       transactionwrite),
						   Record, Name, Val)
			  end,
			  #transactionwrite{}, DecodedTuples),
    Record1;
to_record(transactionwritecompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       transactionwritecompleted),
						   Record, Name, Val)
			  end,
			  #transactionwritecompleted{}, DecodedTuples),
    Record1;
to_record(transactioncommit, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       transactioncommit),
						   Record, Name, Val)
			  end,
			  #transactioncommit{}, DecodedTuples),
    Record1;
to_record(transactioncommitcompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       transactioncommitcompleted),
						   Record, Name, Val)
			  end,
			  #transactioncommitcompleted{}, DecodedTuples),
    Record1;
to_record(readevent, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       readevent),
						   Record, Name, Val)
			  end,
			  #readevent{}, DecodedTuples),
    Record1;
to_record(readeventcompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       readeventcompleted),
						   Record, Name, Val)
			  end,
			  #readeventcompleted{}, DecodedTuples),
    Record1;
to_record(readstreamevents, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       readstreamevents),
						   Record, Name, Val)
			  end,
			  #readstreamevents{}, DecodedTuples),
    Record1;
to_record(readstreameventscompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       readstreameventscompleted),
						   Record, Name, Val)
			  end,
			  #readstreameventscompleted{}, DecodedTuples),
    Record1;
to_record(readallevents, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       readallevents),
						   Record, Name, Val)
			  end,
			  #readallevents{}, DecodedTuples),
    Record1;
to_record(readalleventscompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       readalleventscompleted),
						   Record, Name, Val)
			  end,
			  #readalleventscompleted{}, DecodedTuples),
    Record1;
to_record(subscribetostream, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       subscribetostream),
						   Record, Name, Val)
			  end,
			  #subscribetostream{}, DecodedTuples),
    Record1;
to_record(subscriptionconfirmation, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       subscriptionconfirmation),
						   Record, Name, Val)
			  end,
			  #subscriptionconfirmation{}, DecodedTuples),
    Record1;
to_record(streameventappeared, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       streameventappeared),
						   Record, Name, Val)
			  end,
			  #streameventappeared{}, DecodedTuples),
    Record1;
to_record(unsubscribefromstream, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       unsubscribefromstream),
						   Record, Name, Val)
			  end,
			  #unsubscribefromstream{}, DecodedTuples),
    Record1;
to_record(subscriptiondropped, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       subscriptiondropped),
						   Record, Name, Val)
			  end,
			  #subscriptiondropped{}, DecodedTuples),
    Record1;
to_record(nothandled, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       nothandled),
						   Record, Name, Val)
			  end,
			  #nothandled{}, DecodedTuples),
    Record1;
to_record(scavengedatabase, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       scavengedatabase),
						   Record, Name, Val)
			  end,
			  #scavengedatabase{}, DecodedTuples),
    Record1;
to_record(scavengedatabasecompleted, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       scavengedatabasecompleted),
						   Record, Name, Val)
			  end,
			  #scavengedatabasecompleted{}, DecodedTuples),
    Record1;
to_record(nothandled_masterinfo, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       nothandled_masterinfo),
						   Record, Name, Val)
			  end,
			  #nothandled_masterinfo{}, DecodedTuples),
    Record1.

decode_extensions(Record) -> Record.

decode_extensions(_Types, [], Acc) ->
    dict:from_list(Acc);
decode_extensions(Types, [{Fnum, Bytes} | Tail], Acc) ->
    NewAcc = case lists:keyfind(Fnum, 1, Types) of
	       {Fnum, Name, Type, Opts} ->
		   {Value1, Rest1} = case lists:member(is_record, Opts) of
				       true ->
					   {{FNum, V}, R} =
					       protobuffs:decode(Bytes, bytes),
					   RecVal = decode(Type, V),
					   {RecVal, R};
				       false ->
					   case lists:member(repeated_packed,
							     Opts)
					       of
					     true ->
						 {{FNum, V}, R} =
						     protobuffs:decode_packed(Bytes,
									      Type),
						 {V, R};
					     false ->
						 {{FNum, V}, R} =
						     protobuffs:decode(Bytes,
								       Type),
						 {unpack_value(V, Type), R}
					   end
				     end,
		   case lists:member(repeated, Opts) of
		     true ->
			 case lists:keytake(FNum, 1, Acc) of
			   {value, {FNum, Name, List}, Acc1} ->
			       decode(Rest1, Types,
				      [{FNum, Name,
					lists:reverse([int_to_enum(Type, Value1)
						       | lists:reverse(List)])}
				       | Acc1]);
			   false ->
			       decode(Rest1, Types,
				      [{FNum, Name, [int_to_enum(Type, Value1)]}
				       | Acc])
			 end;
		     false ->
			 [{Fnum,
			   {optional, int_to_enum(Type, Value1), Type, Opts}}
			  | Acc]
		   end;
	       false -> [{Fnum, Bytes} | Acc]
	     end,
    decode_extensions(Types, Tail, NewAcc).

set_record_field(Fields, Record, '$extensions',
		 Value) ->
    Decodable = [],
    NewValue = decode_extensions(element(1, Record),
				 Decodable, dict:to_list(Value)),
    Index = list_index('$extensions', Fields),
    erlang:setelement(Index + 1, Record, NewValue);
set_record_field(Fields, Record, Field, Value) ->
    Index = list_index(Field, Fields),
    erlang:setelement(Index + 1, Record, Value).

list_index(Target, List) -> list_index(Target, List, 1).

list_index(Target, [Target | _], Index) -> Index;
list_index(Target, [_ | Tail], Index) ->
    list_index(Target, Tail, Index + 1);
list_index(_, [], _) -> -1.

extension_size(_) -> 0.

has_extension(_Record, _FieldName) -> false.

get_extension(_Record, _FieldName) -> undefined.

set_extension(Record, _, _) -> {error, Record}.

