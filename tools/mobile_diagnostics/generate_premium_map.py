import sqlite3
import math
import os
import sys
import requests
import concurrent.futures
from datetime import datetime

# Configurações do DECEA WMS
WMS_URL = "https://geoaisweb.decea.mil.br/geoserver/wms"
LAYER = "ICA:CM_01_MUNDIAL"
FORMAT = "image/png"
ZOOM_START = 0
ZOOM_END = 5

def tile_to_bbox(z, x, y):
    """Calcula o BBOX (LonMin, LatMin, LonMax, LatMax) para um tile XYZ em EPSG:4326."""
    def tile_to_lon(x, z):
        return x / math.pow(2.0, z) * 360.0 - 180.0
    def tile_to_lat(y, z):
        n = math.pi - 2.0 * math.pi * y / math.pow(2.0, z)
        return math.degrees(math.atan(math.sinh(n)))

    lon_min = tile_to_lon(x, z)
    lon_max = tile_to_lon(x + 1, z)
    lat_max = tile_to_lat(y, z)
    lat_min = tile_to_lat(y + 1, z)
    
    return [lon_min, lat_min, lon_max, lat_max]

def download_tile(tile):
    z, x, y = tile
    bbox = tile_to_bbox(z, x, y)
    bbox_str = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    
    params = {
        'SERVICE': 'WMS',
        'VERSION': '1.1.1',
        'REQUEST': 'GetMap',
        'LAYERS': LAYER,
        'STYLES': '',
        'FORMAT': FORMAT,
        'TRANSPARENT': 'FALSE',
        'SRS': 'EPSG:4326',
        'WIDTH': '256',
        'HEIGHT': '256',
        'BBOX': bbox_str
    }
    
    try:
        resp = requests.get(WMS_URL, params=params, timeout=15)
        if resp.status_code == 200 and 'image' in resp.headers.get('Content-Type', ''):
            # Inverter Y para o padrão TMS (MBTiles)
            tms_y = (1 << z) - 1 - y
            return (z, x, tms_y, resp.content)
        else:
            print(f" [!] Erro no tile {z}/{x}/{y}: Status {resp.status_code}")
    except Exception as e:
        print(f" [!] Erro de rede no tile {z}/{x}/{y}: {e}")
    return None

def main(output_file):
    print(f"🌍 Iniciando geração do Mapa Premium: {output_file}")
    
    if os.path.exists(output_file):
        os.remove(output_file)

    conn = sqlite3.connect(output_file)
    c = conn.cursor()

    # Estrutura MBTiles 1.3
    c.execute('CREATE TABLE metadata (name text, value text);')
    c.execute('CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);')
    c.execute('CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);')

    metadata = [
        ('name', 'SkyFPL_Premium_World'),
        ('type', 'baselayer'),
        ('version', '1.0'),
        ('description', 'Mapa Mundi Premium - DECEA Atlas (Z0-Z5)'),
        ('format', 'png'),
        ('bounds', '-180.0,-85.05,180.0,85.05')
    ]
    c.executemany('INSERT INTO metadata VALUES (?,?)', metadata)
    conn.commit()

    # Gerar lista de tiles
    all_tiles = []
    for z in range(ZOOM_START, ZOOM_END + 1):
        num_tiles = 1 << z
        for x in range(num_tiles):
            for y in range(num_tiles):
                all_tiles.append((z, x, y))

    total = len(all_tiles)
    print(f"📦 Total de tiles para baixar: {total} (Zoom {ZOOM_START}-{ZOOM_END})")

    # Download paralelo (20 threads)
    count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(download_tile, t): t for t in all_tiles}
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result:
                c.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
            count += 1
            if count % 100 == 0:
                print(f"进度: {count}/{total} ({(count/total)*100:.1f}%)")
                conn.commit()

    conn.commit()
    conn.close()
    
    size_mb = os.path.getsize(output_file) / (1024 * 1024)
    print(f"✅ Mapa Premium gerado com sucesso! Tamanho: {size_mb:.2f} MB")

if __name__ == "__main__":
    out = "premium_world.mbtiles"
    if len(sys.argv) > 1:
        out = sys.argv[1]
    main(out)
