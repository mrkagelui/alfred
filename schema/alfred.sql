CREATE TABLE IF NOT EXISTS jobs
(
    id         uuid        DEFAULT gen_random_uuid()
        CONSTRAINT jobs_pk
            PRIMARY KEY,
    user_id    uuid                      NOT NULL,
    year       INT                       NOT NULL,
    month      INT                       NOT NULL,
    status     TEXT                      NOT NULL,
    failures   INT                       NOT NULL DEFAULT 0,
    msg_key    uuid,
    last_err   TEXT,
    created_at timestamptz DEFAULT NOW() NOT NULL,
    updated_at timestamptz DEFAULT NOW() NOT NULL,
    CONSTRAINT jobs_info
        UNIQUE (user_id, year, month)
);

CREATE INDEX jobs_year_month_status_index
    on jobs (year, month, status);

CREATE TABLE IF NOT EXISTS results
(
    id         uuid        DEFAULT gen_random_uuid()
        CONSTRAINT results_pk
            PRIMARY KEY,
    user_id    TEXT                      NOT NULL,
    year       INT                       NOT NULL,
    month      INT                       NOT NULL,
    result     TEXT                      NOT NULL,
    created_at timestamptz DEFAULT NOW() NOT NULL,
    updated_at timestamptz DEFAULT NOW() NOT NULL,
    CONSTRAINT results_info
        UNIQUE (user_id, year, month)
);
