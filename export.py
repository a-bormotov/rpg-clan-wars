import os, json, csv
import psycopg2

def main():
    # 1) load input.json
    with open("input.json", "r", encoding="utf-8") as f:
        payload = json.load(f)  # Python list/dict
    payload_json = json.dumps(payload, ensure_ascii=False)  # ensure valid JSON string

    # 2) load SQL (expects one %s placeholder)
    with open("query.sql", "r", encoding="utf-8") as f:
        sql = f.read()

    # 3) connect to Postgres via env vars
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
    )
    cur = conn.cursor()

    # 4) execute with JSON parameter
    cur.execute(sql, (payload_json,))
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    # 5) write CSV
    with open("leaderboard.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    cur.close()
    conn.close()
    print(f"✅ leaderboard.csv updated, rows: {len(rows)}")

if __name__ == "__main__":
    main()
