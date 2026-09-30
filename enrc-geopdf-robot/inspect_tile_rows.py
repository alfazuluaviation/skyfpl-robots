import sqlite3
from PIL import Image
import io
import numpy as np

conn = sqlite3.connect(r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles')
cur = conn.cursor()
cur.execute("SELECT tile_data FROM tiles WHERE zoom_level = 6 AND tile_column = 24 AND tile_row = 29")
row = cur.fetchone()
img = Image.open(io.BytesIO(row[0]))
img.save(r'c:\Users\josemir\Desktop\skyfpl-robots_temp\enrc-geopdf-robot\tile_z6_24_29.png')
print("Saved tile_z6_24_29.png")

# Let's inspect where alpha becomes 0
arr = np.array(img)
alpha = arr[:, :, 3]
r = arr[:, :, 0]
g = arr[:, :, 1]
b = arr[:, :, 2]

# In TMS, tile row 29 has OSM Y = 34.
# In the PNG image, row 0 is TOP, row 255 is BOTTOM.
for row_idx in range(200, 256):
    row_alpha = alpha[row_idx, :]
    non_zero = np.count_nonzero(row_alpha > 0)
    if non_zero > 0:
        row_r = r[row_idx, row_alpha > 0]
        row_g = g[row_idx, row_alpha > 0]
        row_b = b[row_idx, row_alpha > 0]
        # check how many are black
        black_cnt = np.count_nonzero((row_r < 30) & (row_g < 30) & (row_b < 30))
        print(f"Row {row_idx:3d}: non-zero alpha={non_zero:3d}/256, black pixels={black_cnt}")
