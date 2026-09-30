import math

def num2deg(xtile, ytile, zoom):
    n = 2.0 ** zoom
    lon_deg = xtile / n * 360.0 - 180.0
    lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * ytile / n)))
    lat_deg = math.degrees(lat_rad)
    return (lat_deg, lon_deg)

# In zoom 7:
# TMS Y=58 -> OSM Y = (127 - 58) = 69
# TMS Y=57 -> OSM Y = (127 - 57) = 70
print("Zoom 7, TMS Y=58 (OSM 69):")
print("NW:", num2deg(47, 69, 7))
print("SE:", num2deg(48, 70, 7))

print("Zoom 7, TMS Y=57 (OSM 70):")
print("NW:", num2deg(47, 70, 7))
print("SE:", num2deg(48, 71, 7))
