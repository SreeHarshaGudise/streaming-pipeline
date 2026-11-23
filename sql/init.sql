-- Windowed counts the dashboard reads. A 10s window can span several
-- micro-batches, so on conflict we ADD to the existing count.
CREATE TABLE IF NOT EXISTS event_agg (
    window_start TIMESTAMPTZ NOT NULL,
    event_type   TEXT        NOT NULL,
    event_count  BIGINT      NOT NULL,
    PRIMARY KEY (window_start, event_type)
);

-- One row per processed micro-batch. Also the idempotency ledger:
-- inserting the same batch_id twice does nothing, so a replayed batch
-- after a crash can't double-count.
CREATE TABLE IF NOT EXISTS batch_metrics (
    batch_id       BIGINT PRIMARY KEY,
    processed_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    valid_count    INT              NOT NULL,
    invalid_count  INT              NOT NULL,
    avg_latency_ms DOUBLE PRECISION,
    max_latency_ms DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_batch_metrics_processed_at ON batch_metrics (processed_at);
