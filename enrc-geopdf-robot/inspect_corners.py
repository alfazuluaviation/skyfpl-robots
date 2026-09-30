from osgeo import gdal
import numpy as np

ds = gdal.Open(r'C:\Users\josemir\Downloads\ENRC_L3.tif')
width = ds.RasterXSize
height = ds.RasterYSize
gt = ds.GetGeoTransform()

print(f"Size: {width}x{height}")
print(f"GeoTransform: {gt}")

# Read alpha band (band 4)
alpha = ds.GetRasterBand(4)
# Let's check non-zero pixels
# To avoid memory explosion, let's sample or check corners
b1 = ds.GetRasterBand(1)
b2 = ds.GetRasterBand(2)
b3 = ds.GetRasterBand(3)

print("Checking bands...")
# Check corners in pixel space
# Top-Left: (0, 0)
# Bottom-Left: (0, height-1)
# Top-Right: (width-1, 0)
# Bottom-Right: (width-1, height-1)

def check_window(x, y, w, h, name):
    a = alpha.ReadAsArray(x, y, w, h)
    r = b1.ReadAsArray(x, y, w, h)
    g = b2.ReadAsArray(x, y, w, h)
    b = b3.ReadAsArray(x, y, w, h)
    print(f"\n--- {name} (x={x}, y={y}, w={w}, h={h}) ---")
    print(f"Alpha > 0 count: {np.count_nonzero(a)} / {w*h}")
    if np.count_nonzero(a) > 0:
        valid_r = r[a > 0]
        valid_g = g[a > 0]
        valid_b = b[a > 0]
        print(f"RGB where alpha>0: R=[{valid_r.min()}, {valid_r.max()}], G=[{valid_g.min()}, {valid_g.max()}], B=[{valid_b.min()}, {valid_b.max()}]")
        black_pixels = np.count_nonzero((r < 10) & (g < 10) & (b < 10) & (a > 0))
        print(f"Black pixels (RGB < 10 with alpha > 0): {black_pixels}")

check_window(0, 0, 500, 500, "Top-Left")
check_window(0, height-500, 500, 500, "Bottom-Left")
check_window(width-500, height-500, 500, 500, "Bottom-Right")
