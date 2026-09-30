from osgeo import gdal, ogr
import json

with open(r'c:\Users\josemir\Desktop\skyfpl-robots_temp\enrc-geopdf-robot\enrc_official_polygons.json') as f:
    polys = json.load(f)

l3 = polys['L3']['coordinates'][0]

print("=== ALL L3 VERTICES ===")
for i, (lon, lat) in enumerate(l3):
    print(f"{i:3d}: lon={lon:9.5f}, lat={lat:9.5f}")
