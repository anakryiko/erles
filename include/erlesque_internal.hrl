-record(conn_settings, {destination,
                        conn_timeout,
                        reconn_delay,
                        max_reconns,
                        heartbeat_period,
                        heartbeat_timeout}).

-record(sys_params, {corr_id,
                     esq_pid,
                     conn_pid,
                     reply_pid,
                     op_timeout,
                     op_retries,
                     retry_delay,
                     auth}).
