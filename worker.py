"""Drain PENDING actions and send them through the provider."""
import json
import os
import urllib.request

import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch")
PROVIDER_URL = os.environ.get("PROVIDER_URL", "http://127.0.0.1:8080")
HTTP_TIMEOUT = 3  # seconds; provider latency is 20-120 ms, a hang would otherwise block for minutes


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
            row = claim(conn)
            if row is None:
                print("nothing left to do")
                return
            action_id, recipient, body = row
            try:
                receipt_id = send(action_id, recipient, body)
                conn.execute("UPDATE actions SET status = 'SENT', receipt_id = %s WHERE id = %s", (receipt_id, action_id))
                print(f"{action_id} SENT {receipt_id}")
            except Exception as e:  # no fault handling yet: record what happened and move on
                reason = f"{type(e).__name__}: {e}"
                conn.execute("UPDATE actions SET status = 'FAILED', fail_reason = %s WHERE id = %s", (reason, action_id))
                print(f"{action_id} FAILED {reason}")


if __name__ == "__main__":
    main()
