-- Encrypted visible history for exact compaction recovery; never plaintext.
CREATE TABLE openai_session_checkpoints (
    user_id BIGINT NOT NULL,
    group_id BIGINT NOT NULL,
    session_hash VARCHAR(16) NOT NULL,
    ciphertext BYTEA NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (user_id, group_id, session_hash),
    FOREIGN KEY (user_id, group_id, session_hash)
        REFERENCES openai_session_bindings(user_id, group_id, session_hash) ON DELETE CASCADE,
    CHECK (octet_length(ciphertext) <= 4195328)
);
CREATE INDEX openai_session_checkpoints_expiry_idx ON openai_session_checkpoints(expires_at);
