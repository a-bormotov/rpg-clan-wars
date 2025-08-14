import os, csv, sys, time
from datetime import datetime

# === DB ===
USE_SSH = os.getenv("USE_SSH", "false").lower() == "true"

def run():
    print("⏱️ Start:", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"))
    # Read SQL
    with open("query.sql", "r", encoding="utf-8") as f:
        query = f.read()

    # Connect (with or without SSH tunnel)
    if USE_SSH:
        from sshtunnel import SSHTunnelForwarder
        import paramiko
        pkey = paramiko.RSAKey.from_private_key_file("ssh_key.pem")
        server = SSHTunnelForwarder(
            (os.environ["SSH_HOST"], int(os.getenv("SSH_PORT", "22"))),
            ssh_username=os.environ["SSH_USER"],
            ssh_pkey=pkey,
            remote_bind_address=(os.environ["DB_HOST"], int(os.getenv("DB_PORT", "5432")))
        )
        server.start()
        host = "127.0.0.1"
        port = server.local_bind_port
    else:
        host = os.environ["DB_HOST"]
        port = int(os.getenv("DB_PORT", "5432"))

    import psycopg2
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"]
    )
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    with open("leaderboard.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    cur.close()
    conn.close()
    if USE_SSH:
        server.stop()

    print(f"✅ Wrote leaderboard.csv with {len(rows)} rows")

if __name__ == "__main__":
    run()
