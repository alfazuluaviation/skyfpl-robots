import sqlite3
import requests
import os
import math

# Configurações para um fallback "Premium" (Z0-Z4)
# Z4 oferece detalhes de fronteiras e grandes cidades
MAX_ZOOM = 4 
DB_PATH = r'c:\Users\josemir\Desktop\skyFPL-native\assets\world_fallback.mbtiles'

def get_tiles():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('CREATE TABLE metadata (name text, value text)')
    c.execute('CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob)')
    
    metadata = [
        ('name', 'SkyFPL Premium Fallback'),
        ('type', 'baselayer'),
        ('version', '1.1'),
        ('description', 'World map fallback Z0-Z4'),
        ('format', 'png'),
    ]
    c.executemany('INSERT INTO metadata VALUES (?, ?)', metadata)
    
    # Usando o servidor do OSM (Respeitando limites para este pequeno fallback)
    headers = {'User-Agent': 'SkyFPLCachedMapGenerator/1.0'}
    
    count = 0
    for z in range(MAX_ZOOM + 1):
        num_tiles = 2**z
        print(f"Baixando Zoom {z} ({num_tiles}x{num_tiles} tiles)...")
        for x in range(num_tiles):
            for y in range(num_tiles):
                # OSM usa XYZ, MBTiles usa TMS para a tabela tiles
                # tms_y = (2^z - 1) - xyz_y
                tms_y = (2**z - 1) - y
                
                url = f"https://tile.openstreetmap.org/{z}/{x}/{y}.png"
                try:
                    resp = requests.get(url, headers=headers, timeout=5)
                    if resp.status_code == 200:
                        c.execute('INSERT INTO tiles VALUES (?, ?, ?, ?)', (z, x, tms_y, resp.content))
                        count += 1
                except:
                    pass
        conn.commit()
    
    conn.close()
    print(f"Sucesso! {count} tiles salvos em {DB_PATH}")

if __name__ == "__main__":
    get_tiles()
