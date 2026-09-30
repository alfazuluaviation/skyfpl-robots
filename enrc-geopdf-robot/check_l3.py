from osgeo import gdal, ogr
import json

ds = gdal.Open(r'C:\Users\josemir\Downloads\ENRC_L3.tif')
print('Driver:', ds.GetDriver().ShortName)
print('Size:', ds.RasterXSize, ds.RasterYSize)
print('GeoTransform:', ds.GetGeoTransform())

with open(r'c:\Users\josemir\Desktop\skyfpl-robots_temp\enrc-geopdf-robot\enrc_official_polygons.json') as f:
    polys = json.load(f)

l3_coords = polys['L3']['coordinates'][0]
print('Total L3 vertices:', len(l3_coords))
for i, pt in enumerate(l3_coords[:15]):
    print(f'pt[{i}]: {pt}')
