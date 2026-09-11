CREATE TABLE sample_cron_run (
    id uuid PRIMARY KEY,
    job text NOT NULL,
    pod text NOT NULL,
    ran_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX sample_cron_run_job_idx ON sample_cron_run (job, ran_at);

CREATE TABLE sample_backfill (
    id uuid PRIMARY KEY,
    mirror text NOT NULL,
    rows_seen bigint NOT NULL,
    at timestamptz NOT NULL DEFAULT now()
);
