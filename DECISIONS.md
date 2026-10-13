# DECISIONS

`verify.py` is clean (0 duplicates, 0 missing, 0 violations) on seeds 1, 2, 3, 5 and 11 with three workers, `kill -9` at 60 s and default faults; runs finish between 87 and 106 s.

## State machine

Four states, one request in flight per worker. A timeout or connection reset is not a transition: the worker that hit it walks away and the row stays `SENDING`. A worker killed mid-send leaves exactly the same row. Once a `SENDING` row is older than the 10 s lease, whichever worker notices first runs `settle()`, which asks `/sent` what really happened to every overdue row at once.

| transition | who | guard |
|---|---|---|
| PENDING -> SENDING | `claim()` | `FOR UPDATE SKIP LOCKED` |
| SENDING -> SENT / PENDING / FAILED | owner, on 200 / 429 or 500 / any other code | `AND status = 'SENDING'` (without it, a worker back from a hang overwrote a `SENT` written by another and duplicated) |
| SENDING -> SENT | `settle()`, receipt found in `/sent` | `AND status <> 'SENT'` |
| SENDING -> PENDING | `settle()`, no receipt after a full scan | `status = 'SENDING'` and `attempted_at` older than the lease |

Pacing is one shared row: every request, `/sent` included, books a 100 ms slot via `GREATEST(next_slot, now(), blocked_until) + 0.1 s`, any 429 sets `blocked_until`, and a worker re-checks it after sleeping.

## Timeout after a possible send

I never resend directly. After the 10 s lease, `settle()` pages `/sent` from a shared cursor to the end of the list, marks every row it finds `SENT` with its receipt, and only then returns the overdue rows it did not find to `PENDING`. No duplicate is guaranteed only if an accepted send is visible in `/sent` before that scan, which this provider satisfies strictly (the receipt is recorded before it hangs or drops, and the scan starts at least 10 s later); a `/sent` that lags longer than that, or a request admitted after I gave up, would be resent.

## What I would ask the provider for

Idempotency on `POST /send` keyed by `action_id`: the same id returns the same receipt and sends nothing. That one change deletes the uncertain class. A timeout becomes "retry", and `settle()`, the lease wait and the `/sent` scan all disappear. It is also the fix that scales.

## What verify.py cannot catch

Anything about my own table. It never checks that the 500 seeded actions actually landed in the database, or that what I sent was the seeded data: receipts carry no body, so the right person getting the wrong text passes. It also does not see a row left `SENDING` or wrongly `FAILED` while the provider shows it sent. I check instead: `make seed` reports 500 rows, `SELECT status, count(*)` ends at `SENT 500`, every `receipt_id` in my table exists in `/sent`, and `recipient` and `body` in my table match `seed/actions.json`.

## What breaks first at 50,000 an hour

The rate limit. 50,000 an hour is 14/s and the provider allows 12/s, so no worker design reaches it; it needs a higher limit or more accounts. In my code, the single `provider_state` row goes first: every request updates it, so more workers queue on one lock. I would book slots in batches, add an index on `(status, attempted_at)`, and move `settle()` to one dedicated process.

## Deliberately not built

Threads or async inside a worker. One request in flight per process is what keeps violations at or under three per `Retry-After` window, and three single-threaded processes already finish 500 actions in about 90 s.
