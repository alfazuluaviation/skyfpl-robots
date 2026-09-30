import sqlite3

conn = sqlite3.connect(r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles')
cur = conn.cursor()

# Get metadata
cur.execute("SELECT name, value FROM metadata")
meta = dict(cur.fetchall())
print("Metadata:", meta)

# Sample some tiles at zoom 6
cur.execute("SELECT zoom_level, tile_column, tile_row, length(tile_data) FROM tiles WHERE zoom_level = 6 LIMIT 10")
tiles = cur.fetchall()
for t in tiles:
    print(f"Z={t[0]}, X={t[1]}, Y={t[2]}, Size={t[3]} bytes")
