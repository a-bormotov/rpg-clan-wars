import os, csv, psycopg2

def main():
    with open("query.sql", "r", encoding="utf-8") as f:
        sql = f.read()

    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    with open("leaderboard.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)

    cur.close()
    conn.close()
    print(f"✅ leaderboard.csv обновлён, строк: {len(rows)}")

if __name__ == "__main__":
    main()
