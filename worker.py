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
LEASE_SECONDS = 10


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


def lease_expired(conn):
    """True if any SENDING row has outlived its lease: its owner is not going to report back."""
    count = conn.execute(
        "SELECT count(*) FROM actions WHERE status = 'SENDING' AND attempted_at < now() - %s * interval '1 second'",
        (LEASE_SECONDS,),
    ).fetchone()[0]
    return count > 0


def all_done(conn):
    """True once every row is SENT or FAILED."""
    remaining = conn.execute("SELECT count(*) FROM actions WHERE status NOT IN ('SENT', 'FAILED')").fetchone()[0]
    return remaining == 0


def send(action_id, recipient, body):
    """POST /send. Returns the receipt_id, or raises."""
    data = json.dumps({"action_id": action_id, "recipient": recipient, "body": body}).encode()
    req = urllib.request.Request(PROVIDER_URL + "/send", data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return json.load(resp)["receipt_id"]


def settle(conn):
    """Settle SENDING rows whose owner never reported back.

    Read the provider's /sent list from where we left off and mark everything we find as SENT.
    Any row still SENDING past its lease after that was never recorded by the provider, so it
    goes back to PENDING and will be sent again.
    """
    while True:
        wait_for_slot(conn)  # /sent shares the provider's rate limit with /send
        cursor = conn.execute("SELECT sent_cursor FROM provider_state").fetchone()[0]
        try:
            with urllib.request.urlopen(f"{PROVIDER_URL}/sent?since={cursor}", timeout=HTTP_TIMEOUT) as resp:
                page = json.load(resp)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                block_everyone(conn, int(e.headers["Retry-After"]))
            continue  # 429 or 500: ask for the same page again

        for receipt in page["receipts"]:
            conn.execute(
                "UPDATE actions SET status = 'SENT', receipt_id = %s WHERE id = %s AND status <> 'SENT'",
                (receipt["receipt_id"], receipt["action_id"]),
            )
        conn.execute("UPDATE provider_state SET sent_cursor = %s", (page["next"],))
        if not page["receipts"]:
            break  # end of the list for now

    resent = conn.execute(
        "UPDATE actions SET status = 'PENDING' WHERE status = 'SENDING' AND attempted_at < now() - %s * interval '1 second'",
        (LEASE_SECONDS,),
    ).rowcount
    print(f"settled: {resent} row(s) were never sent, back to PENDING")


def main():
    with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
        while True:
            if lease_expired(conn):
                settle(conn)

            wait_for_slot(conn)
            row = claim(conn)
            if row is None:
                if all_done(conn):
                    print("nothing left to do")
                    return
                time.sleep(1)  # other workers are mid-send, or rows are waiting out their lease
                continue

            action_id, recipient, body = row
            try:
                receipt_id = send(action_id, recipient, body)
                conn.execute(
                    "UPDATE actions SET status = 'SENT', receipt_id = %s WHERE id = %s AND status = 'SENDING'",
                    (receipt_id, action_id),
                )
                print(f"{action_id} SENT {receipt_id}")
            except urllib.error.HTTPError as e:
                if e.code in (429, 500):
                    # nothing was sent in either case, so retrying is safe
                    conn.execute("UPDATE actions SET status = 'PENDING' WHERE id = %s AND status = 'SENDING'", (action_id,))
                    print(f"{action_id} RETRY after {e.code}")
                    if e.code == 429:
                        # provider opened a Retry-After window; any request inside it is a violation
                        block_everyone(conn, int(e.headers["Retry-After"]))
                else:
                    # any other HTTP error (400) will not get better with a retry
                    reason = f"HTTP {e.code}"
                    conn.execute(
                        "UPDATE actions SET status = 'FAILED', fail_reason = %s WHERE id = %s AND status = 'SENDING'",
                        (reason, action_id),
                    )
                    print(f"{action_id} FAILED {reason}")
            except OSError as e:
                print(f"{action_id} NO ANSWER ({type(e).__name__}), settle() will check /sent after the lease")


if __name__ == "__main__":
    main()
