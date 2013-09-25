%% SPECIAL VALUES
-define(EXPVER_ANY, -2).
-define(LAST_EVENT_NUM, -1).
-define(LAST_LOG_POS, -1).

%% COMMON REQUEST DEFAULTS
-define(DEF_AUTH, defauth).
-define(DEF_RESOLVE, true).
-define(DEF_REQ_MASTER, true).

%% CONNECTION DEFAULTS
-define(DEF_CONNECTION_TIMEOUT, 2000).
-define(DEF_RECONNECTION_DELAY, 500).
-define(DEF_MAX_CONNECTION_RETRIES, 10).
-define(DEF_HEARTBEAT_PERIOD, 500).
-define(DEF_HEARTBEAT_TIMEOUT, 1000).

%% CLUSTER DEFAULTS
-define(DEF_DNS_TIMEOUT, 2000).
-define(DEF_GOSSIP_TIMEOUT, 1000).

%% OPERATIONS DEFAULTS
-define(DEF_MAX_SERVER_OPS, 2000).
-define(DEF_OPERATION_TIMEOUT, 7000).
-define(DEF_OPERATION_RETRIES, 10).
-define(DEF_RETRY_DELAY, 500).
-define(DEF_DEFAULT_AUTH, noauth).

%%% SYS EVENT TYPES
-define(ET_STREAMMETADATA, <<"$metadata">>).
-define(ET_SETTINGS, <<"$settings">>).

%% SYS METADATA
-define(META_MAXAGE, <<"$maxAge">>).
-define(META_MAXCOUNT, <<"$maxCount">>).
-define(META_TRUNCATEBEFORE, <<"$tb">>).
-define(META_CACHECONTROL, <<"$cacheControl">>).
-define(META_ACL, <<"$acl">>).
-define(META_ACLREAD, <<"$r">>).
-define(META_ACLWRITE, <<"$w">>).
-define(META_ACLDELETE, <<"$d">>).
-define(META_ACLMETAREAD, <<"$mr">>).
-define(META_ACLMETAWRITE, <<"$mw">>).

%% SYS ROLES
-define(ROLE_ADMINS, <<"$admins">>).
-define(ROLE_ALL, <<"$all">>).

%% SYS DEFAULT ACLS
-define(SYS_USERSTREAMACL, <<"$userStreamAcl">>).
-define(SYS_SYSTEMSTREAMACL, <<"$systemStreamAcl">>).

%% ES CONNECTION SETTINGS
-record(conn_settings, {conn_timeout,
                        reconn_delay,
                        max_conn_retries,
                        heartbeat_period,
                        heartbeat_timeout,
                        dns_timeout,
                        gossip_timeout}).

%% REQUEST COMMON PARAMS
-record(sys_params, {corr_id,
                     esq_pid,
                     conn_pid,
                     reply_pid,
                     op_timeout,
                     op_retries,
                     retry_delay,
                     auth}).
