import os

import psycopg2


def verify_persistence():
    """Verify that traces are persisting to Postgres."""
    print("--- Postgres Persistence Verification ---")

    # Defaults for local dev based on docker-compose.infra.yml
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5433")  # agent-control-db external port
    db_name = os.environ.get("DB_NAME", "agent_control")
    db_user = os.environ.get("DB_USER", "postgres")
    db_pass = os.environ.get("DB_PASS", "control_password")

    conn = None
    try:
        conn = psycopg2.connect(
            host=db_host, port=db_port, dbname=db_name, user=db_user, password=db_pass
        )
        cur = conn.cursor()

        # Debug: check total count
        cur.execute("SELECT count(*) FROM otel.spans;")
        total_count = cur.fetchone()[0]
        print(f"Total spans in DB: {total_count}")

        # Debug: check ingestion queue
        try:
            cur.execute("SELECT status, count(*) FROM otel.ingestion_queue GROUP BY status;")
            rows = cur.fetchall()
            print(f"Ingestion Queue status: {rows}")

            cur.execute(
                "SELECT error_message, received_at FROM otel.ingestion_queue "
                "WHERE status = 'failed' ORDER BY received_at DESC LIMIT 1;"
            )
            err = cur.fetchone()
            if err:
                print(f"Latest failure: {err}")
        except Exception as e:
            print(f"Ingestion queue query failed: {e}")
            conn.rollback()

        if total_count > 0:
            cur.execute("SELECT max(start_time) FROM otel.spans;")
            last_time = cur.fetchone()[0]
            print(f"Most recent span time: {last_time}")
            cur.execute("SELECT NOW();")
            db_now = cur.fetchone()[0]
            print(f"DB time: {db_now}")

        # Check for spans in the last 15 minutes
        query = """
        SELECT count(*)
        FROM otel.spans
        WHERE start_time > (NOW() - INTERVAL '15 minutes');
        """

        cur.execute(query)
        count = cur.fetchone()[0]

        if count > 0:
            print(f"SUCCESS: {count} traces persisted to Postgres in last 15 minutes.")
            return True
        else:
            print("\nFAILURE: No spans found in Postgres in last 15 minutes.")
            print("Diagnosis:")
            if total_count > 0:
                print(f"- Historical persistence confirmed ({total_count} total spans).")
            else:
                print("- No historical persistence (DB is empty).")

            # Check queue state for hint
            try:
                cur.execute("SELECT count(*) FROM otel.ingestion_queue WHERE status = 'failed'")
                failed = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM otel.ingestion_queue WHERE status = 'pending'")
                pending = cur.fetchone()[0]

                if failed > 0:
                    print(
                        f"- CRITICAL: {failed} items failed in ingestion queue. "
                        "Check otel-worker logs."
                    )
                if pending > 0:
                    print(
                        f"- WARNING: {pending} items pending in queue. "
                        "Ingestion might be lagging."
                    )
            except Exception:
                pass

            print("- Suggestion: Verify 'otel-worker' is running and connected to DB.")
            return False

    except Exception as e:
        print(f"Error connecting to Postgres: {e}")
        return False
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    if verify_persistence():
        exit(0)
    else:
        exit(1)
