CREATE TABLE IF NOT EXISTS actions (
    id           TEXT PRIMARY KEY,
    recipient    TEXT NOT NULL,
    body         TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | SENDING | SENT | FAILED
    attempts     INT  NOT NULL DEFAULT 0,
    attempted_at TIMESTAMPTZ,                       -- when the current/last attempt was claimed
    receipt_id   TEXT,                              -- set when SENT
    fail_reason  TEXT                               -- set when FAILED
);

-- One row shared by all workers: when the next request may go out, and until when the
-- provider has asked everyone to stay quiet (Retry-After).
CREATE TABLE IF NOT EXISTS provider_state (
    id            INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    next_slot     TIMESTAMPTZ NOT NULL DEFAULT now(),
    blocked_until TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_cursor   TEXT        NOT NULL DEFAULT '0'    -- how far we have read the provider's /sent list
);
INSERT INTO provider_state DEFAULT VALUES ON CONFLICT DO NOTHING;
