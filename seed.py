"""Create the schema and load seed/actions.json. Safe to run more than once."""
import json
import os
from pathlib import Path

import psycopg

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://dispatch:dispatch@127.0.0.1:5432/dispatch")
HERE = Path(__file__).resolve().parent


def main() -> None:
    actions = json.loads((HERE / "seed" / "actions.json").read_text(encoding="utf-8"))
    with psycopg.connect(DATABASE_URL) as conn:
        conn.execute((HERE / "schema.sql").read_text())
        conn.cursor().executemany(
            "INSERT INTO actions (id, recipient, body) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING",
            [(a["id"], a["recipient"], a["body"]) for a in actions],
        )
        total = conn.execute("SELECT count(*) FROM actions").fetchone()[0]
    print(f"seeded {len(actions)} actions, table has {total} rows")


if __name__ == "__main__":
    main()
