CREATE TABLE IF NOT EXISTS actions (
    id           TEXT PRIMARY KEY,
    recipient    TEXT NOT NULL,
    body         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | SENDING | UNCERTAIN | SENT | FAILED
    attempts     INT  NOT NULL DEFAULT 0,
    attempted_at TIMESTAMPTZ,                       -- when the current/last attempt was claimed
    receipt_id   TEXT,                              -- set when SENT
    fail_reason  TEXT                               -- set when FAILED
);
