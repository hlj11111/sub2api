-- Routing metadata only: no conversation content, credentials or response payloads.
-- Successful ownership has no TTL. Lease expiry only releases in-flight exclusion.
CREATE TABLE openai_session_bindings (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL,
    session_hash VARCHAR(16) NOT NULL,
    account_id BIGINT,
    lease_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    committed_owner TEXT,
    last_success_at TIMESTAMPTZ,
    PRIMARY KEY (user_id, group_id, session_hash)
);
-- group_id = 0 represents an ungrouped request. Keep deleted account IDs as
-- unavailable owners rather than erasing them and guessing another account.
