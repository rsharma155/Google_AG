
# queries.py

# CPU Usage (using ring buffers for historical data, picking the latest)
GET_CPU_USAGE = """
SELECT TOP 1
    [SQLProcessUtilization] = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int'),
    [SystemIdle] = record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int'),
    [OtherProcessUtilization] = 100 - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int') 
    - record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')
FROM (
    SELECT timestamp, CONVERT(xml, record) AS record
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
    AND record LIKE '%<SystemHealth>%'
) AS x
ORDER BY timestamp DESC;
"""

# Memory Usage
GET_MEMORY_USAGE = """
SELECT
    physical_memory_in_use_kb,
    large_page_allocations_kb,
    locked_page_allocations_kb,
    page_fault_count,
    memory_utilization_percentage,
    process_physical_memory_low,
    process_virtual_memory_low
FROM sys.dm_os_process_memory;
"""

# I/O Stats (Latency and Throughput per database file)
GET_IO_STATS = """
SELECT
    DB_NAME(mf.database_id) AS database_name,
    mf.name AS logical_name,
    mf.type_desc,
    vfs.num_of_reads,
    vfs.num_of_bytes_read,
    vfs.io_stall_read_ms,
    vfs.num_of_writes,
    vfs.num_of_bytes_written,
    vfs.io_stall_write_ms,
    vfs.size_on_disk_bytes
FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
JOIN sys.master_files AS mf ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id;
"""

# Wait Stats (System wide)
# Filtering out benign waits is important to avoid noise
GET_WAIT_STATS = """
SELECT
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    max_wait_time_ms,
    signal_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE wait_time_ms > 0
AND wait_type NOT IN (
    'BROKER_EVENTHANDLER', 'BROKER_RECEIVE_WAITFOR', 'BROKER_TASK_STOP', 'BROKER_TO_FLUSH',
    'BROKER_TRANSMITTER', 'CHECKPOINT_QUEUE', 'CHKPT', 'CLR_AUTO_EVENT', 'CLR_MANUAL_EVENT',
    'CLR_SEMAPHORE', 'DBMIRROR_DBM_EVENT', 'DBMIRROR_EVENTS_QUEUE', 'DBMIRROR_WORKER_QUEUE',
    'DBMIRRORING_CMD', 'DIRTY_PAGE_POLL', 'DISPATCHER_QUEUE_SEMAPHORE', 'EXECSYNC', 'FSAGENT',
    'FT_IFTS_SCHEDULER_IDLE_WAIT', 'FT_IFTSHC_MUTEX', 'HADR_CLUSAPI_CALL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
    'HADR_LOGCAPTURE_WAIT', 'HADR_NOTIFICATION_DEQUEUE', 'HADR_TIMER_TASK', 'HADR_WORK_QUEUE',
    'KSOURCE_WAKEUP', 'LAZYWRITER_SLEEP', 'LOGMGR_QUEUE', 'MEMORY_ALLOCATION_EXT', 'ONDEMAND_TASK_QUEUE',
    'PREEMPTIVE_XE_GETTARGETSTATE', 'PWAIT_ALL_COMPONENTS_INITIALIZED', 'PWAIT_DIRECTLOGCONSUMER_GETNEXT',
    'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'QDS_ASYNC_QUEUE', 'QDS_CLEANUP_STALE', 'QDS_SHUTDOWN_QUEUE',
    'REQUEST_FOR_DEADLOCK_SEARCH', 'RESOURCE_QUEUE', 'SERVER_IDLE_CHECK', 'SLEEP_BPOOL_FLUSH',
    'SLEEP_DBSTARTUP', 'SLEEP_DCOMSTARTUP', 'SLEEP_MASTERDBREADY', 'SLEEP_MASTERMDREADY',
    'SLEEP_MASTERUPGRADED', 'SLEEP_MSDBSTARTUP', 'SLEEP_SYSTEMTASK', 'SLEEP_TASK', 'SLEEP_TEMPDBSTARTUP',
    'SNI_HTTP_ACCEPT', 'SP_SERVER_DIAGNOSTICS_SLEEP', 'SQLTRACE_BUFFER_FLUSH', 'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
    'SQLTRACE_WAIT_ENTRIES', 'WAIT_FOR_RESULTS', 'WAITFOR', 'WAITFOR_TASKSHUTDOWN', 'WAIT_XTP_RECOVERY',
    'WAIT_XTP_HOST_WAIT', 'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'WAIT_XTP_CKPT_CLOSE', 'XE_DISPATCHER_JOIN',
    'XE_DISPATCHER_WAIT', 'XE_TIMER_EVENT'
);
"""

# User Error Logs (Recent errors - Sev > 10)
# Note: xp_readerrorlog can be slow if log is huge. We limit to last 30 minutes if possible, but the procedure parameters are (Filenum, LogType, SearchString1, SearchString2, StartTime, EndTime).
# Since calling with dates from python is safer, we will just read the last N rows or use a python filter. 
# However, for a metric collector, counting errors is usually better.
# We will count errors in sys.messages logic or actually just reading ring buffer is safer/lighter for connectivity errors.
# Let's try to get error counts from sys.dm_os_ring_buffers as well for connectivity, but real errors are in logs.
# Warning: xp_readerrorlog is blocking and heavy. 
# ALTERNATIVE: sys.dm_os_ring_buffers for 'RING_BUFFER_EXCEPTION'
GET_RECENT_EXCEPTIONS = """
SELECT TOP 20
    Error = record.value('(./Record/Exception/Error)[1]', 'int'),
    Severity = record.value('(./Record/Exception/Severity)[1]', 'int'),
    State = record.value('(./Record/Exception/State)[1]', 'int'),
    Message = record.value('(./Record/Exception/UserDefinedMsg)[1]', 'varchar(max)'),
    pct = record.value('(./Record/Exception/UserDefinedMsg)[1]', 'varchar(max)'),
    CreationTime = record.value('(./Record/@time)[1]', 'bigint')
FROM (
    SELECT timestamp, CONVERT(xml, record) AS record
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_EXCEPTION'
) AS x
ORDER BY timestamp DESC;
"""

# Active Sessions & Blocking
# Gets sessions with active requests or blocking others
GET_ACTIVE_SESSIONS = """
SELECT
    s.session_id,
    s.login_name,
    s.host_name,
    s.program_name,
    r.status,
    r.command,
    r.cpu_time,
    r.total_elapsed_time,
    r.wait_type,
    r.wait_time,
    r.last_wait_type,
    r.blocking_session_id,
    DB_NAME(r.database_id) AS database_name,
    t.text AS query_text
FROM sys.dm_exec_requests r
JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
WHERE s.is_user_process = 1;
"""

# Problematic Queries (Long running or high CPU in cache)
# Top 10 by CPU
GET_TOP_CPU_QUERIES = """
SELECT TOP 10
    qs.execution_count,
    qs.total_worker_time / 1000 AS total_cpu_ms,
    qs.total_worker_time / ISNULL(NULLIF(qs.execution_count, 0), 1) / 1000 AS avg_cpu_ms,
    qs.total_elapsed_time / 1000 AS total_duration_ms,
    qs.total_elapsed_time / ISNULL(NULLIF(qs.execution_count, 0), 1) / 1000 AS avg_duration_ms,
    SUBSTRING(qt.text, (qs.statement_start_offset/2)+1,
        ((CASE qs.statement_end_offset
            WHEN -1 THEN DATALENGTH(qt.text)
            ELSE qs.statement_end_offset
        END - qs.statement_start_offset)/2) + 1) AS query_text,
    DB_NAME(qp.dbid) AS database_name
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) qp
ORDER BY qs.total_worker_time DESC;
"""

# Failed Jobs (Last 24 hours)
GET_FAILED_JOBS = """
SELECT
    j.name AS job_name,
    h.run_status,
    h.run_date,
    h.run_time,
    h.message
FROM msdb.dbo.sysjobs j
INNER JOIN msdb.dbo.sysjobhistory h ON j.job_id = h.job_id
WHERE h.run_status = 0 -- Failed
AND h.run_date >= CONVERT(varchar(8), GETDATE(), 112) -- Today (simplification)
ORDER BY h.instance_id DESC;
"""

# Database States (Offline, Recovery, etc.)
GET_DB_STATES = """
SELECT name, state_desc, user_access_desc, is_read_only
FROM sys.databases;
"""
