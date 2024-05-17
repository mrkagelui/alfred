CREATE TABLE IF NOT EXISTS users
(
    id         uuid        DEFAULT gen_random_uuid()
        CONSTRAINT users_pk
            PRIMARY KEY,
    status     TEXT                      NOT NULL,
    created_at timestamptz DEFAULT NOW() NOT NULL,
    updated_at timestamptz DEFAULT NOW() NOT NULL
);

CREATE INDEX IF NOT EXISTS users_status_index
    ON users (status);
