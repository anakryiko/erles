-record(event_data, {event_id = erlesque_utils:gen_uuid() :: binary(),
                     event_type = erlang:error({required, event_type}),
                     is_json = false :: boolean,
                     data = erlang:error({required, data}) :: binary(),
                     metadata = <<"">> :: binary()
      }).

-record(event, {stream_id,
                event_number,
                event_id,
                event_type,
                data,
                metadata = <<"">>}).

-record(resolved_event, {event,
                         link,
                         position = unknown}).
