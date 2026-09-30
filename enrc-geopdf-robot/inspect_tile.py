import sqlite3
import math
from PIL import Image
import io
import numpy as np

# Coordinate: Lon = -44.0, Lat = -14.5, Zoom = 6
# Slippy map tile calculation:
def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)

# In MBTiles, Y is TMS (flipped): tms_y = (2**zoom - 1) - osm_y
for z in [6, 7]:
    x, osm_y = deg2num(-14.5, -44.0, z)
    tms_y = (2**z - 1) - osm_y
    print(f"Zoom {z}: Lon -44, Lat -14.5 -> X={x}, OSM_Y={osm_y}, TMS_Y={tms_y}")

conn = sqlite3.connect(r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles')
cur = conn.cursor()

# Check what tiles exist around this area
for z in [6]:
    x, osm_y = deg2num(-14.5, -44.0, z)
    tms_y = (2**z - 1) - osm_y
    cur.execute("SELECT length(tile_data), tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?", (z, x, tms_y))
    row = cur.fetchone()
    if row:
        print(f"Found tile Z={z}, X={x}, Y={tms_y}, size={row[0]} bytes")
        img = Image.open(io.BytesIO(row[1]))
        print(f"Image format: {img.format}, mode: {img.mode}, size: {img.size}")
        arr = np.array(img)
        print(f"Array shape: {arr.shape}")
        if arr.shape[2] == 4:
            alpha = arr[:, :, 3]
            print(f"Alpha min: {alpha.min()}, max: {alpha.max()}, non-zero: {np.count_nonzero(alpha)}")
            # Check bottom row of image
            print("Bottom 10 rows alpha:", alpha[-10:, :].max(axis=1))
    else:
        print(f"Tile NOT found for Z={z}, X={x}, Y={tms_y}")
