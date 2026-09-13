
import sqlite3
import os
import json
from PIL import Image
from io import BytesIO

DB_PATH = "ENRCL_FINAL.mbtiles"
OUTPUT_DIR = "audit_tiles"

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Coordenadas fornecidas pelo usuário (assumindo XYZ)
COORDS = [
    (8, 94, 141),
    (9, 187, 277),
    (9, 189, 279),
]

def analyze_tile(z, x, y_xyz):
    results = {
        "coord_xyz": {"z": z, "x": x, "y": y_xyz},
        "tms_y": (1 << z) - 1 - y_xyz,
        "attempts": []
    }
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Tentamos os dois esquemas: TMS e XYZ
    for scheme, y_val in [("TMS", results["tms_y"]), ("XYZ", y_xyz)]:
        cursor.execute("SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?", (z, x, y_val))
        row = cursor.fetchone()
        
        status = {"scheme": scheme, "y_queried": y_val, "exists": False}
        
        if row:
            data = row[0]
            status["exists"] = True
            status["size_bytes"] = len(data)
            status["magic_bytes"] = data[:4].hex()
            
            # Salva para inspeção visual
            filename = f"z{z}_x{x}_y{y_val}_{scheme}.png"
            filepath = os.path.join(OUTPUT_DIR, filename)
            with open(filepath, "wb") as f:
                f.write(data)
            status["saved_as"] = filepath
            
            # Análise de Imagem
            try:
                img = Image.open(BytesIO(data)).convert("RGBA")
                status["width"], status["height"] = img.size
                
                # Conta pixels não transparentes
                bbox = img.getbbox()
                status["is_pure_transparent"] = (bbox is None)
                if bbox:
                   status["bbox"] = bbox
                
            except Exception as e:
                status["image_error"] = str(e)
        
        results["attempts"].append(status)
    
    conn.close()
    return results

def audit():
    print(f"--- Deep Tile Audit: {DB_PATH} ---")
    final_report = []
    
    for z, x, y in COORDS:
        print(f"Auditing Z:{z} X:{x} Y:{y} (XYZ)...")
        res = analyze_tile(z, x, y)
        
        # Auditoria de vizinhos (Zoom acima e abaixo)
        res["neighbors"] = {
            "parent": analyze_tile(z-1, x // 2, y // 2) if z > 5 else None,
            "children": [analyze_tile(z+1, x*2 + dx, y*2 + dy) for dx in [0, 1] for dy in [0, 1]] if z < 11 else []
        }
        
        final_report.append(res)
        
    with open("deep_audit_results.json", "w") as f:
        json.dump(final_report, f, indent=4)
    
    print("\n[OK] Auditoria concluída. Resultados em deep_audit_results.json")

if __name__ == "__main__":
    audit()
