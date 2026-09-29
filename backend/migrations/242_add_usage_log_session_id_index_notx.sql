-- Support session tracing without blocking writes to the usage log.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_usage_logs_session_created_at
    ON usage_logs (session_id, created_at DESC, id DESC)
    WHERE session_id IS NOT NULL;
