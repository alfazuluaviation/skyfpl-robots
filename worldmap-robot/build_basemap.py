import sqlite3
import math
import sys
import os
import concurrent.futures
import requests
import boto3
import json
import argparse
from datetime import datetime

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

def push_progress(current, total, status="in_progress", size_mb=None, size_bytes=None, min_zoom=0, max_zoom=11):
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
            Key="basemap_progress.json",
            Body=json.dumps(data).encode('utf-8'),
            ContentType='application/json',
            CacheControl='max-age=0, no-cache, no-store, must-revalidate'
        )
    except Exception as e:
        pass # Ignora erros de log para não derrubar o worker principal

# Bounding Box do Brasil
BRAZIL_BOUNDS = {
    'minLat': -34.0,
    'maxLat': 6.0,
    'minLng': -74.0,
    'maxLng': -34.7  # Extremo leste do Brasil (Ponta do Seixas é approx -34.79)
}

# OpenStreetMap Carto (Padrão) - Mapa rodoviário colorido oficial
URL_TEMPLATE = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
HEADERS = {
    'User-Agent': 'SkyFPL-BasemapBot/1.0 (github.com/alfazuluaviation/skyfpl-native; contact@skyfpl.app)'
}

def latLngToTile(lat, lng, zoom):
    n = 2.0 ** zoom
    x = int((lng + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.log(math.tan(lat_rad) + (1 / math.cos(lat_rad))) / math.pi) / 2.0 * n)
    return x, y

def get_tiles(min_zoom, max_zoom):
    tiles = []
    for z in range(min_zoom, max_zoom + 1):
        # Brazil-only for ALL zooms (regional detail)
        min_x, max_y = latLngToTile(BRAZIL_BOUNDS['minLat'], BRAZIL_BOUNDS['minLng'], z)
        max_x, min_y = latLngToTile(BRAZIL_BOUNDS['maxLat'], BRAZIL_BOUNDS['maxLng'], z)
        
        # Clamp to valid range
        num_tiles = 1 << z
        min_x = max(0, min_x)
        max_x = min(num_tiles - 1, max_x)
        min_y = max(0, min_y)
        max_y = min(num_tiles - 1, max_y)
        
        for x in range(min_x, max_x + 1):
            for y in range(min_y, max_y + 1):
                tiles.append((z, x, y))
    return tiles

def download_tile(tile):
    z, x, y = tile
    url = URL_TEMPLATE.replace('{z}', str(z)).replace('{x}', str(x)).replace('{y}', str(y))
    
    for attempt in range(5):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                mbtiles_y = (1 << z) - 1 - y
                return (z, x, mbtiles_y, resp.content)
            elif resp.status_code == 429:
                import time
                time.sleep(2)
        except Exception as e:
            if attempt == 4:
                print(f"❌ Erro fatal no tile {tile} após 5 tentativas: {e}")
            import time
            time.sleep(0.5)
    return None

def main(output_file, min_zoom, max_zoom, max_workers):
    if os.path.exists(output_file):
        os.remove(output_file)

    conn = sqlite3.connect(output_file)
    c = conn.cursor()

    # Criação da estrutura padrão do MBTiles 1.3
    c.execute('CREATE TABLE metadata (name text, value text);')
    c.execute('''CREATE TABLE tiles (
                    zoom_level integer, 
                    tile_column integer, 
                    tile_row integer, 
                    tile_data blob);''')
    c.execute('CREATE UNIQUE INDEX tile_index on tiles (zoom_level, tile_column, tile_row);')

    # Metadados
    metadata = [
        ('name', 'SkyFPL_Base'),
        ('type', 'baselayer'),
        ('version', '1.0'),
        ('description', f'Mapa Base OSM Brasil (Z{min_zoom}-Z{max_zoom}) para SkyFPL'),
        ('format', 'png'),
        ('bounds', f"{BRAZIL_BOUNDS['minLng']},{BRAZIL_BOUNDS['minLat']},{BRAZIL_BOUNDS['maxLng']},{BRAZIL_BOUNDS['maxLat']}"),
        ('scheme', 'tms') # Já corrigido conforme V24.02
    ]
    c.executemany('INSERT INTO metadata VALUES (?,?)', metadata)
    conn.commit()

    tiles = get_tiles(min_zoom, max_zoom)
    total_tiles = len(tiles)
    print(f"[Robo BaseMap] Iniciando download de {total_tiles} tiles do Brasil (Zoom {min_zoom} - {max_zoom})...")

    push_progress(0, total_tiles, "in_progress", min_zoom=min_zoom, max_zoom=max_zoom)

    completed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_tile = {executor.submit(download_tile, t): t for t in tiles}
        
        for future in concurrent.futures.as_completed(future_to_tile):
            result = future.result()
            if result:
                z, x, mbtiles_y, tile_data = result
                c.execute("INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
                               (z, x, mbtiles_y, tile_data))
            completed += 1
            if completed % 1000 == 0:
                print(f"[Progresso] {completed}/{total_tiles} tiles adicionados...")
                conn.commit()
                push_progress(completed, total_tiles, "in_progress", min_zoom=min_zoom, max_zoom=max_zoom)

    conn.commit()
    conn.close()
    
    size_bytes = os.path.getsize(output_file)
    size_mb = size_bytes / (1024 * 1024)
    print(f"📦 Compilação finalizada ({size_mb:.2f} MB). Iniciando Upload para o Cofre R2...")
    
    push_progress(total_tiles, total_tiles, "uploading", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)
    
    if s3_client:
        try:
            s3_client.upload_file(output_file, "skyfpl-charts", "SkyFPL_Base.mbtiles")
            push_progress(total_tiles, total_tiles, "completed", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)
            print(f"✅ MBTiles enviado com sucesso para a Cloudflare!")
        except Exception as e:
            print(f"❌ Erro no upload final: {e}")
            push_progress(total_tiles, total_tiles, "failed", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)
            sys.exit(1)
    else:
        print("⚠️ Ignorando upload: S3_CLIENT não inicializado.")
        push_progress(total_tiles, total_tiles, "completed", size_mb=size_mb, size_bytes=size_bytes, min_zoom=min_zoom, max_zoom=max_zoom)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Base Map MBTiles Generator")
    parser.add_argument("output_file", help="Caminho do arquivo MBTiles")
    parser.add_argument("--min_zoom", type=int, default=0, help="Zoom mínimo (default: 0)")
    parser.add_argument("--max_zoom", type=int, default=11, help="Zoom máximo (default: 11)")
    parser.add_argument("--workers", type=int, default=50, help="Número de workers paralelos (default: 50)")
    
    args = parser.parse_args()
    main(args.output_file, args.min_zoom, args.max_zoom, args.workers)
