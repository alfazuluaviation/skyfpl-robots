import sqlite3
import os

db_path = r'c:\Users\josemir\Desktop\skyFPL-native\assets\world_fallback.mbtiles'

if not os.path.exists(db_path):
    print(f"File not found: {db_path}")
    exit(1)

print(f"File size: {os.path.getsize(db_path)} bytes")

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables found:", tables)
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='view';")
    views = cursor.fetchall()
    print("Views found:", views)
    
    conn.close()
except Exception as e:
    print(f"Error inspecting DB: {e}")
