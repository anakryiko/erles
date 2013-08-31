-record(connection_settings, {verboseLogging = false,
                              maxActiveOperations = 5000,
                              maxRetries = 10,
                              maxReconnections = 10,
                              reconnectionDelay = 100,
                              operationTimeout = 7000,
                              defaultCredentials = no_credentials,
                              failNoResponse = false,
                              heartbeatInterval = 750,
                              heartbeatTimeout = 1500,
                              connectionTimeout = 1000,
                              errorOccurred,
                              closed,
                              connected,
                              disconnected,
                              reconnecting}).

-record(event_data, {event_id = erlesque_utils:create_uuid_v4(),
                     event_type,
                     is_json = false,
                     data :: binary(),
                     metadata :: binary()
      }).
