import sqlite3
import sys

def fix_db(file_path):
    print(f"Opening {file_path}...")
    conn = sqlite3.connect(file_path)
    
    # Check before
    print("--- BEFORE ---")
    cursor = conn.execute("SELECT * FROM metadata WHERE name = 'scheme'")
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"metadata scheme: {row}")
    else:
        print("metadata scheme: NOT FOUND")

    # Inject
    print("Injecting scheme='tms'...")
    conn.execute("INSERT OR REPLACE INTO metadata (name, value) VALUES ('scheme', 'tms')")
    conn.commit()

    # Check after
    print("--- AFTER ---")
    cursor = conn.execute("SELECT * FROM metadata WHERE name = 'scheme'")
    rows = cursor.fetchall()
    for row in rows:
        print(f"metadata scheme: {row}")

    conn.close()
    print("Done!")

if __name__ == "__main__":
    fix_db("WAC_BRASIL_FULL.mbtiles")
