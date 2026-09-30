from osgeo import gdal, ogr
import json

ds = gdal.Open(r'C:\Users\josemir\Downloads\ENRC_L3.tif')
band = ds.GetRasterBand(4)
drv = ogr.GetDriverByName("Memory")
dst_ds = drv.CreateDataSource("mem")
dst_layer = dst_ds.CreateLayer("poly", srs=ogr.osr.SpatialReference(ds.GetProjection()))
fd = ogr.FieldDefn("DN", ogr.OFTInteger)
dst_layer.CreateField(fd)
gdal.Polygonize(band, band, dst_layer, 0, [])

feat = dst_layer.GetNextFeature()
geom = feat.GetGeometryRef()
print("Original polygon envelope (W, E, S, N):", geom.GetEnvelope())

# Buffer by -0.012
buffered = geom.Buffer(-0.012)
print("Buffered polygon envelope (W, E, S, N):", buffered.GetEnvelope())

# Let's inspect the rings
ring_orig = geom.GetGeometryRef(0)
ring_buf = buffered.GetGeometryRef(0)

print(f"Orig points: {ring_orig.GetPointCount()}, Buf points: {ring_buf.GetPointCount()}")

# Find the 4 extreme points of Orig vs Buf
# NW: min X, max Y
# NE: max X, max Y
# SW: min X, min Y
# SE: max X, min Y
def find_corners(ring, name):
    pts = [ring.GetPoint(i) for i in range(ring.GetPointCount())]
    # NW: min x + max y
    nw = min(pts, key=lambda p: p[0] - p[1])
    ne = max(pts, key=lambda p: p[0] + p[1])
    sw = min(pts, key=lambda p: p[0] + p[1])
    se = max(pts, key=lambda p: p[0] - p[1])
    print(f"\n--- Corners of {name} ---")
    print(f"NW: {nw[0]:.5f}, {nw[1]:.5f}")
    print(f"NE: {ne[0]:.5f}, {ne[1]:.5f}")
    print(f"SW: {sw[0]:.5f}, {sw[1]:.5f}")
    print(f"SE: {se[0]:.5f}, {se[1]:.5f}")

find_corners(ring_orig, "ORIGINAL")
find_corners(ring_buf, "BUFFERED (-0.012)")
