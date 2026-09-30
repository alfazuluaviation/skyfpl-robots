import os
import json
import sqlite3
import subprocess

# Let's check what tiles exist in enrc_staging_L3_HD.mbtiles
# and let's check what tiles were in the original GDAL translate before purging
mbtiles_path = r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles'

conn = sqlite3.connect(mbtiles_path)
cur = conn.cursor()

# Check all tiles at Zoom 6
cur.execute("SELECT zoom_level, tile_column, tile_row, length(tile_data) FROM tiles WHERE zoom_level in (5, 6, 7) ORDER BY zoom_level, tile_row, tile_column")
rows = cur.fetchall()
print(f"Total tiles in Z5-Z7: {len(rows)}")
for r in rows:
    print(f"Z={r[0]}, X={r[1]}, Y={r[2]}, Size={r[3]}")
