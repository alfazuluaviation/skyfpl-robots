
import requests
import math
from PIL import Image
from io import BytesIO
import os

WMS_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms"
TILE_SIZE = 256

def tile_bbox_mercator(x: int, y: int, z: int):
    world_size = 20037508.342789244 * 2
    res = world_size / (2 ** z)
    minx = x * res - 20037508.342789244
    maxx = (x + 1) * res - 20037508.342789244
    maxy = 20037508.342789244 - y * res
    miny = 20037508.342789244 - (y + 1) * res
    return (minx, miny, maxx, maxy)

def get_wms_tile(x, y, z, layers):
    minX, minY, maxX, maxY = tile_bbox_mercator(x, y, z)
    params = {
        "SERVICE": "WMS",
        "VERSION": "1.1.1",
        "REQUEST": "GetMap",
        "LAYERS": layers,
        "STYLES": "",
        "SRS": "EPSG:3857",
        "BBOX": f"{minX},{minY},{maxX},{maxY}",
        "WIDTH": str(TILE_SIZE),
        "HEIGHT": str(TILE_SIZE),
        "FORMAT": "image/png",
        "TRANSPARENT": "TRUE",
    }
    
    resp = requests.get(WMS_URL, params=params, timeout=30)
    print(f"Request for Y={y} ({layers}): Status {resp.status_code}, Length {len(resp.content)}")
    return resp.content

def analyze(path):
    img = Image.open(path).convert("RGBA")
    bbox = img.getbbox()
    print(f"File {path}: BBOX {bbox}")

full_layers = "ICA:ENRC_L9,ICA:ENRC_L8,ICA:ENRC_L7,ICA:ENRC_L4,ICA:ENRC_L6,ICA:ENRC_L5,ICA:ENRC_L2,ICA:ENRC_L1,ICA:ENRC_L3"

for cy in [131, 132, 133]:
    print(f"\n--- Testing Y={cy} ---")
    # Simula o que o robô escolheu ontem (provavelmente L6+L9 para esse ponto)
    data_dynamic = get_wms_tile(88, cy, 8, "ICA:ENRC_L9,ICA:ENRC_L6")
    # Simula a estratégia "Matadora" (Tudo)
    data_full = get_wms_tile(88, cy, 8, full_layers)
    
    with open(f"test_dynamic_{cy}.png", "wb") as f: f.write(data_dynamic)
    with open(f"test_full_{cy}.png", "wb") as f: f.write(data_full)
    
    analyze(f"test_dynamic_{cy}.png")
    analyze(f"test_full_{cy}.png")
