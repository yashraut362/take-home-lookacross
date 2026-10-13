"""Drain PENDING actions and send them through the provider."""
import json
import os
import time
import urllib.error
import urllib.request

import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch")
PROVIDER_URL = os.environ.get("PROVIDER_URL", "http://127.0.0.1:8080")
HTTP_TIMEOUT = 3  # seconds; provider latency is 20-120 ms, a hang would otherwise block for minutes
SEND_INTERVAL = 0.1  # seconds between requests across ALL workers: 10/s, under the provider's 12/s


def wait_for_slot(conn):
    """Reserve the next send slot in the shared row and sleep until it arrives.

    One UPDATE does the booking atomically, so three workers get three different slots.
    GREATEST(...) pushes the slot past any Retry-After block the provider has given us.
    A block can appear while we sleep, so we check again after waking and rebook if needed.
    """
    while True:
        seconds = conn.execute(
            """
            UPDATE provider_state
               SET next_slot = GREATEST(next_slot, now(), blocked_until) + %s * interval '1 second'
         RETURNING EXTRACT(EPOCH FROM next_slot - now())
            """,
            (SEND_INTERVAL,),
        ).fetchone()[0]
        if seconds > 0:
            time.sleep(float(seconds))

        blocked = conn.execute("SELECT blocked_until > now() FROM provider_state").fetchone()[0]
        if not blocked:
            return


def block_everyone(conn, seconds):
    """Provider sent Retry-After: no worker may send until the window closes."""
    conn.execute(
        "UPDATE provider_state SET blocked_until = GREATEST(blocked_until, now() + %s * interval '1 second')",
        (seconds,),
    )


def claim(conn):
    """Take one PENDING row and mark it SENDING. SKIP LOCKED keeps workers off each other's rows."""
    return conn.execute(
        """
        UPDATE actions
           SET status = 'SENDING', attempts = attempts + 1, attempted_at = now()
         WHERE id = (SELECT id FROM actions WHERE status = 'PENDING'
                      LIMIT 1 FOR UPDATE SKIP LOCKED)
     RETURNING id, recipient, body
        """
    ).fetchone()


def send(action_id, recipient, body):
    """POST /send. Returns the receipt_id, or raises."""
    data = json.dumps({"action_id": action_id, "recipient": recipient, "body": body}).encode()
    req = urllib.request.Request(PROVIDER_URL + "/send", data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.load(resp)["receipt_id"]


def main():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        while True:
            wait_for_slot(conn)
            row = claim(conn)
            if row is None:
                print("nothing left to do")
                return
            action_id, recipient, body = row
            try:
                receipt_id = send(action_id, recipient, body)
                conn.execute("UPDATE actions SET status = 'SENT', receipt_id = %s WHERE id = %s", (receipt_id, action_id))
                print(f"{action_id} SENT {receipt_id}")
            except Exception as e:
                if isinstance(e, urllib.error.HTTPError) and e.code in (429, 500):
                    # nothing was sent in either case, so retrying is safe
                    conn.execute("UPDATE actions SET status = 'PENDING' WHERE id = %s", (action_id,))
                    print(f"{action_id} RETRY after {e.code}")
                    if e.code == 429:
                        # provider opened a Retry-After window; any request inside it is a violation
                        block_everyone(conn, int(e.headers["Retry-After"]))
                    continue

                # everything else is not handled yet: record what happened and move on
                reason = f"{type(e).__name__}: {e}"
                conn.execute("UPDATE actions SET status = 'FAILED', fail_reason = %s WHERE id = %s", (reason, action_id))
                print(f"{action_id} FAILED {reason}")


if __name__ == "__main__":
    main()
