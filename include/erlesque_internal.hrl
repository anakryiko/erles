-record(conn_settings, {conn_timeout,
                        reconn_delay,
                        max_conn_retries,
                        heartbeat_period,
                        heartbeat_timeout,
                        dns_timeout,
                        gossip_timeout,
                        discover_delay,
                        max_discover_retries}).

-record(sys_params, {corr_id,
                     esq_pid,
                     conn_pid,
                     reply_pid,
                     op_timeout,
                     op_retries,
                     retry_delay,
                     auth}).
