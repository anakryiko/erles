-record(event_data, {event_id = erlesque_utils:create_uuid_v4() :: binary(),
                     event_type = erlang:error({required, event_type}),
                     is_json = false :: boolean,
                     data = erlang:error({required, data}) :: binary(),
                     metadata = <<"">> :: binary()
      }).
