import math

def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)

print("Tile X=23, OSM_Y=32 (TMS 31):")
print("NW:", num2deg(23, 32, 6))
print("SE:", num2deg(24, 33, 6))

print("Tile X=23, OSM_Y=33 (TMS 30):")
print("NW:", num2deg(23, 33, 6))
print("SE:", num2deg(24, 34, 6))

print("Tile X=23, OSM_Y=34 (TMS 29):")
print("NW:", num2deg(23, 34, 6))
print("SE:", num2deg(24, 35, 6))
