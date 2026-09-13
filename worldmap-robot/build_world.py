import sqlite3
import math
import sys
import os
import concurrent.futures
import requests
import boto3
import json
import time
from datetime import datetime
import argparse

# Setup de comunicação R2 em tempo real
r2_endpoint = os.environ.get('R2_ENDPOINT')
r2_access_key_id = os.environ.get('R2_ACCESS_KEY_ID')
r2_secret_access_key = os.environ.get('R2_SECRET_ACCESS_KEY')

s3_client = None
if r2_endpoint and r2_access_key_id and r2_secret_access_key:
    s3_client = boto3.client(
        's3', endpoint_url=r2_endpoint,
        aws_access_key_id=r2_access_key_id,
        aws_secret_access_key=r2_secret_access_key,
        region_name='wnam'
    )

def push_progress(current, total, status="in_progress", size_mb=None, size_bytes=None, min_zoom=0, max_zoom=7):
    if not s3_client: return
    try:
        pct = int((current / total) * 100) if total > 0 else 0
        data = {
            "progress": current,
            "total": total,
            "percent": pct,
            "status": status,
            "updated_at": datetime.now().isoformat()
        }
        if size_mb is not None:
            data['size_mb'] = float(size_mb)
        if size_bytes is not None:
            data['size_bytes'] = int(size_bytes)
            
        data['metadata'] = {
            'global_config': {
                'minzoom': min_zoom,
                'maxzoom': max_zoom,
                'size_bytes': size_bytes if size_bytes else 0,
                'updated_at': datetime.now().isoformat()
            }
        }
        s3_client.put_object(
            Bucket="skyfpl-charts",
            Key="worldmap_progress.json",
            Body=json.dumps(data).encode('utf-8'),
            ContentType='application/json',
            CacheControl='max-age=0, no-cache, no-store, must-revalidate'
        )
    except Exception:
        pass

URL_TEMPLATE = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
HEADERS = {
    'User-Agent': 'SkyFPL-WorldMapBot/1.0 (github.com/alfazuluaviation/skyfpl-native; contact@skyfpl.app)'
}


def lat_lng_to_tile(lat, lng, zoom):
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y


def get_tiles(min_zoom, max_zoom):
    tiles = []
    for z in range(min_zoom, max_zoom + 1):
        num_tiles = 1 << z
        # Cobertura GLOBAL — todos os tiles deste nível de zoom
        for x in range(num_tiles):
            for y in range(num_tiles):
                tiles.append((z, x, y))
    return tiles


def download_tile(tile):
    z, x, y = tile
    url = URL_TEMPLATE.replace('{z}', str(z)).replace('{x}', str(x)).replace('{y}', str(y))

    for attempt in range(5):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                # Converte Y XYZ → Y TMS (padrão MBTiles)
                mbtiles_y = (1 << z) - 1 - y
                return (z, x, mbtiles_y, resp.content)
            elif resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
        except Exception as e:
            if attempt == 4:
                print(f"❌ Erro fatal no tile {tile} após 5 tentativas: {e}")
            time.sleep(0.5)
    return None


def main(output_file, min_zoom, max_zoom, max_workers):
    if os.path.exists(output_file):
        os.remove(output_file)

    conn = sqlite3.connect(output_file)
    c = conn.cursor()

    # Estrutura MBTiles 1.3
    c.execute('CREATE TABLE metadata (name text, value text);')
    c.execute('''CREATE TABLE tiles (
                    zoom_level integer,
                    tile_column integer,
                    tile_row integer,
                    tile_data blob);''')
    c.execute('CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);')

    metadata = [
        ('name',        'SkyFPL_World'),
        ('type',        'baselayer'),
        ('version',     '1.0'),
        ('description', f'Mapa Mundial SkyFPL — Z{min_zoom}-Z{max_zoom} Global (OSM)'),
        ('format',      'png'),
        ('minzoom',     str(min_zoom)),
        ('maxzoom',     str(max_zoom)),
        ('bounds',      '-180,-85.05,180,85.05'),
        ('scheme',      'tms'),
    ]
    c.executemany('INSERT INTO metadata VALUES (?,?)', metadata)
    conn.commit()

    tiles = get_tiles(min_zoom, max_zoom)
    total_tiles = len(tiles)
    print(f"[Robô WorldMap] {total_tiles} tiles | Z{min_zoom}-Z{max_zoom} GLOBAL")
    push_progress(0, total_tiles, "in_progress", min_zoom=min_zoom, max_zoom=max_zoom)

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tile = {executor.submit(download_tile, t): t for t in tiles}
        for future in concurrent.futures.as_completed(future_to_tile):
            result = future.result()
            if result:
                z, x, mbtiles_y, tile_data = result
                c.execute(
                    "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                    (z, x, mbtiles_y, tile_data)
                )
            completed += 1
            if completed % 100 == 0:
                print(f"[Progresso] {completed}/{total_tiles} tiles...")
                conn.commit()
                push_progress(completed, total_tiles, "in_progress", min_zoom=min_zoom, max_zoom=max_zoom)

    conn.commit()
    conn.close()

    size_bytes = os.path.getsize(output_file)
    size_mb = size_bytes / (1024 * 1024)
    print(f"📦 Compilação finalizada ({size_mb:.2f} MB). Iniciando Upload R2...")

    push_progress(total_tiles, total_tiles, "uploading", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)

    if s3_client:
        try:
            s3_client.upload_file(output_file, "skyfpl-charts", "SkyFPL_World.mbtiles")
            push_progress(total_tiles, total_tiles, "completed", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)
            print("✅ SkyFPL_World.mbtiles enviado com sucesso para a Cloudflare R2!")
        except Exception as e:
            print(f"❌ Erro no upload: {e}")
            push_progress(total_tiles, total_tiles, "failed", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)
            sys.exit(1)
    else:
        print("⚠️ Upload ignorado: credenciais R2 não configuradas.")
        push_progress(total_tiles, total_tiles, "completed", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="World Map MBTiles Generator")
    parser.add_argument("output_file", help="Caminho do arquivo MBTiles")
    parser.add_argument("--min_zoom", type=int, default=0, help="Zoom mínimo (default: 0)")
    parser.add_argument("--max_zoom", type=int, default=7, help="Zoom máximo (default: 7)")
    parser.add_argument("--workers", type=int, default=50, help="Número de workers paralelos (default: 50)")
    
    args = parser.parse_args()
    main(args.output_file, args.min_zoom, args.max_zoom, args.workers)
