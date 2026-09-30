import sqlite3
from PIL import Image
import io
import numpy as np

conn = sqlite3.connect(r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles')
cur = conn.cursor()

def dump_tile(z, x, y, name):
    cur.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, y))
    row = cur.fetchone()
    if not row:
        print(f"Tile {name} (Z={z}, X={x}, Y={y}) NOT FOUND!")
        return None
    img = Image.open(io.BytesIO(row[0]))
    arr = np.array(img)
    alpha = arr[:, :, 3] if arr.shape[2] == 4 else None
    print(f"\n--- {name} (Z={z}, X={x}, Y={y}) ---")
    print(f"Size: {img.size}, Mode: {img.mode}, Bytes: {len(row[0])}")
    if alpha is not None:
        print(f"Alpha > 0 count: {np.count_nonzero(alpha)} / {alpha.size}")
        # Let's check left edge of the tile (column 0 to 5)
        print("Left edge alpha (col 0..3):")
        for col in range(4):
            print(f"  Col {col}: non-zero alpha={np.count_nonzero(alpha[:, col] > 0)}")
        # Check bottom edge (row 252..255)
        print("Bottom edge alpha (row 252..255):")
        for r_idx in range(252, 256):
            print(f"  Row {r_idx}: non-zero alpha={np.count_nonzero(alpha[r_idx, :] > 0)}")
    return img

img1 = dump_tile(6, 24, 31, "Top-Left (X=24, Y=31)")
img2 = dump_tile(6, 23, 30, "Below Top-Left (X=23, Y=30)")
img3 = dump_tile(6, 24, 30, "Center-Left (X=24, Y=30)")
