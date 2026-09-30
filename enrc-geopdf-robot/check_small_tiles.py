import sqlite3
from PIL import Image
import io
import numpy as np

conn = sqlite3.connect(r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles')
cur = conn.cursor()

cur.execute("SELECT zoom_level, tile_column, tile_row, length(tile_data), tile_data FROM tiles WHERE length(tile_data) < 3000")
rows = cur.fetchall()
print(f"Total tiles with size < 3000 bytes: {len(rows)}")

for z, x, y, size, data in rows[:15]:
    img = Image.open(io.BytesIO(data))
    arr = np.array(img)
    if arr.ndim == 3 and arr.shape[2] == 4:
        alpha = arr[:, :, 3]
        nz = np.count_nonzero(alpha > 0)
        print(f"Z={z}, X={x}, Y={y}, Size={size}b -> Non-zero alpha: {nz}/{alpha.size} ({nz/alpha.size*100:.2f}%)")
    else:
        print(f"Z={z}, X={x}, Y={y}, Size={size}b -> Mode: {img.mode}")
