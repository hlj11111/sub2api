-- A policy row is the restriction marker. Losing its last account must fail closed.
CREATE TABLE user_group_account_policies (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    group_id BIGINT NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, group_id)
);
CREATE TABLE user_group_account_policy_accounts (
    user_id BIGINT NOT NULL,
    group_id BIGINT NOT NULL,
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, group_id, account_id),
    FOREIGN KEY (user_id, group_id) REFERENCES user_group_account_policies(user_id, group_id) ON DELETE CASCADE
);
CREATE INDEX user_group_account_policy_accounts_account_idx ON user_group_account_policy_accounts(account_id);
