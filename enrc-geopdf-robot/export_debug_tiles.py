import sqlite3
import math
from PIL import Image
import io
import os

def deg2num(lat_deg, lon_deg, zoom):
    lat_rad = math.radians(lat_deg)
    n = 2.0 ** zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (xtile, ytile)

conn = sqlite3.connect(r'c:\Users\josemir\Desktop\skynav-pro-official\admin\.cache\mbtiles\enrc_staging_L3_HD.mbtiles')
cur = conn.cursor()

os.makedirs(r'c:\Users\josemir\Desktop\skyfpl-robots_temp\enrc-geopdf-robot\debug_tiles', exist_ok=True)

# Screenshot 1: Manga (-14.5, -44.0 to -43.0)
print("=== SCREENSHOT 1 TILES (Zoom 6 and 7) ===")
for z in [6, 7]:
    for lon in [-44.8, -44.0, -43.0]:
        for lat in [-14.6, -14.4]:
            x, osm_y = deg2num(lat, lon, z)
            tms_y = (2**z - 1) - osm_y
            cur.execute("SELECT length(tile_data), tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
            row = cur.fetchone()
            if row:
                fname = f"tile_z{z}_x{x}_ytms{tms_y}_osm{osm_y}.png"
                outpath = os.path.join(r'c:\Users\josemir\Desktop\skyfpl-robots_temp\enrc-geopdf-robot\debug_tiles', fname)
                with open(outpath, "wb") as f:
                    f.write(row[1])
                print(f"Z={z}, Lon={lon}, Lat={lat} -> X={x}, TMS_Y={tms_y}, size={row[0]} -> {fname}")
            else:
                print(f"Z={z}, Lon={lon}, Lat={lat} -> X={x}, TMS_Y={tms_y} -> NOT FOUND / NULL")

# Screenshot 3: Top-left / Barra do Corda / Lago da Pedra (-5.0, -45.0)
print("\n=== SCREENSHOT 3 TILES (Zoom 6 and 7) ===")
for z in [6, 7]:
    for lon in [-45.1, -45.0, -44.9]:
        for lat in [-5.5, -5.0, -4.5]:
            x, osm_y = deg2num(lat, lon, z)
            tms_y = (2**z - 1) - osm_y
            cur.execute("SELECT length(tile_data), tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, tms_y))
            row = cur.fetchone()
            if row:
                fname = f"tile_z{z}_x{x}_ytms{tms_y}_osm{osm_y}.png"
                outpath = os.path.join(r'c:\Users\josemir\Desktop\skyfpl-robots_temp\enrc-geopdf-robot\debug_tiles', fname)
                with open(outpath, "wb") as f:
                    f.write(row[1])
                print(f"Z={z}, Lon={lon}, Lat={lat} -> X={x}, TMS_Y={tms_y}, size={row[0]} -> {fname}")
            else:
                print(f"Z={z}, Lon={lon}, Lat={lat} -> X={x}, TMS_Y={tms_y} -> NOT FOUND / NULL")
