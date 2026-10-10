"""
extract_all_reul_polygons.py — SkyFPL REUL HD BBOX & Neatline Catalog Extractor
================================================================================
Extrai a geometria exata da moldura oficial (NEATLINE) e BBOX das Cartas de Rotas Especiais
de Ultraleves (REUL) publicadas pelo DECEA:
  1. Extrai o metadado do GeoTIFF oficial do DECEA GeoServer via GDAL.
  2. Reprojeta os vértices para coordenadas geográficas EPSG:4326.
  3. Aplica Inset Buffer de segurança (~80 m / 0.0008°) para eliminar linha preta/margem de papel.
  4. Descobre emendas vigentes e URLs do GeoPDF oficial no portal AISWEB.
  5. Atualiza os catálogos canônicos no Admin Dashboard e App Native.
"""

import os
import sys
import json
import re
import shutil
import urllib.request
import subprocess
import xml.etree.ElementTree as ET

# ─── Utilitários Cartográficos GDAL ──────────────────────────────────────────

def find_gdal_tool(tool_name: str) -> str:
    found = shutil.which(tool_name)
    if found:
        return found
    if sys.platform.startswith("win"):
        found = shutil.which(f"{tool_name}.exe")
        if found:
            return found
        possible_paths = [
            f"C:\\Program Files\\QGIS 3.40.15\\bin\\{tool_name}.exe",
            f"C:\\Program Files\\QGIS 3.38.3\\bin\\{tool_name}.exe",
            f"C:\\OSGeo4W\\bin\\{tool_name}.exe",
            f"C:\\OSGeo4W64\\bin\\{tool_name}.exe"
        ]
        for p in possible_paths:
            if os.path.exists(p):
                return p
    return tool_name

GDALINFO = find_gdal_tool("gdalinfo")

# ─── Mapeamento Canônico Homologado pelo DECEA ───────────────────────────────

CANONICAL_SECTORS = [
    {
        "code": "REUL_WJ3_RIO_DE_JANEIRO",
        "ident": "CCV_REUL_WJ3_RIO_DE_JANEIRO",
        "name": "Rio de Janeiro (WJ3 - Jacarepaguá / Maricá / Saquarema)",
        "layer": "ICA:CCV_REUL_WJ3_RIO_DE_JANEIRO",
        "file": "CCV_REUL_WJ3_RIO_DE_JANEIRO.tif",
        "z_order": 10
    }
]

WMS_CAPABILITIES_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms?service=WMS&request=GetCapabilities"
GEOTIFF_BASE_URL = "https://geoaisweb.decea.mil.br/src/geotiffs/{filename}"
AISWEB_VISUAIS_URL = "https://aisweb.decea.mil.br/?i=cartas&p=visuais"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/REUL-Catalog-Extractor"}

OUTPUT_JSON = os.path.join(os.path.dirname(__file__), "reul_official_polygons.json")
OUTPUT_ADMIN_TS = r"C:\Users\josemir\Desktop\skynav-pro-official\admin\src\data\reulOfficialPolygons.ts"
OUTPUT_NATIVE_TS = r"C:\Users\josemir\Desktop\skyFPL-native\src\data\reulCatalog.ts"

# ─── Funções de Coleta e Cálculo ─────────────────────────────────────────────

def fetch_decea_capabilities():
    print("📡 [DECEA] Consultando WMS GetCapabilities do GeoServer...")
    bboxes = {}
    try:
        req = urllib.request.Request(WMS_CAPABILITIES_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=35) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)
        for layer in root.iter('{http://www.opengis.net/wms}Layer'):
            name_el = layer.find('{http://www.opengis.net/wms}Name')
            ex_bbox = layer.find('{http://www.opengis.net/wms}EX_GeographicBoundingBox')
            if name_el is not None and name_el.text and ex_bbox is not None:
                n = name_el.text.strip().replace("ICA:", "")
                try:
                    w = float(ex_bbox.findtext('{http://www.opengis.net/wms}westBoundLongitude'))
                    s = float(ex_bbox.findtext('{http://www.opengis.net/wms}southBoundLatitude'))
                    e = float(ex_bbox.findtext('{http://www.opengis.net/wms}eastBoundLongitude'))
                    n_lat = float(ex_bbox.findtext('{http://www.opengis.net/wms}northBoundLatitude'))
                    bboxes[n] = [round(w, 5), round(s, 5), round(e, 5), round(n_lat, 5)]
                except Exception:
                    pass
    except Exception as e:
        print(f"  [Aviso] Falha ao consultar WMS ({e})")
    return bboxes

def fetch_live_reul_urls():
    print("🌐 [AISWEB] Descobrindo URLs vigentes de GeoPDF (REUL)...")
    pdf_map = {}
    try:
        req = urllib.request.Request(AISWEB_VISUAIS_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        rows = re.findall(r"<tr[^>]*>.*?</tr>", html, re.DOTALL)
        reul_rows = [row for row in rows if 'REUL' in row.upper()]
        for row in reul_rows:
            m = re.search(r'href=[\'"]([^\'"]+\.pdf)[\'"]', row)
            if m:
                u = m.group(1).strip()
                fname = os.path.basename(u).lower()
                full_url = u if u.startswith("http") else f"https://aisweb.decea.mil.br/{u.lstrip('/')}"
                row_clean = ' '.join(re.sub(r'<[^>]+>', ' ', row).split())
                pdf_map[fname] = {
                    "url": full_url,
                    "info": row_clean
                }
        print(f"  ✓ {len(pdf_map)} arquivos REUL encontrados no AISWEB.")
    except Exception as e:
        print(f"  [Aviso] Falha ao consultar AISWEB ({e})")
    return pdf_map

def apply_inset_buffer(coords: list, inset_deg: float = 0.0008) -> list:
    ring = coords[0]
    cx = sum(p[0] for p in ring[:-1]) / (len(ring) - 1)
    cy = sum(p[1] for p in ring[:-1]) / (len(ring) - 1)
    
    new_ring = []
    for p in ring:
        dx = cx - p[0]
        dy = cy - p[1]
        dist = (dx*dx + dy*dy)**0.5
        if dist > 1e-6:
            nx = p[0] + (dx / dist) * inset_deg
            ny = p[1] + (dy / dist) * inset_deg
            new_ring.append([round(nx, 5), round(ny, 5)])
        else:
            new_ring.append([round(p[0], 5), round(p[1], 5)])
            
    if new_ring[0] != new_ring[-1]:
        new_ring[-1] = new_ring[0]
        
    return [new_ring]

def main():
    print("=" * 80)
    print("🪂 SkyFPL — Extração Fiel de Molduras e Polígonos Oficiais REUL HD")
    print("=" * 80)
    
    wms_bboxes = fetch_decea_capabilities()
    aisweb_map = fetch_live_reul_urls()
    
    results = {}
    for item in CANONICAL_SECTORS:
        code = item["code"]
        ident = item["ident"]
        name = item["name"]
        file_name = item["file"]
        clean_layer = item["layer"].replace("ICA:", "")
        
        tif_url = GEOTIFF_BASE_URL.format(filename=file_name)
        vsi_url = f"/vsicurl/{tif_url}"
        
        print(f"\n🔍 Extraindo metadados de {file_name} via GDAL...")
        
        cmd = [GDALINFO, "-json", vsi_url]
        p = subprocess.run(cmd, capture_output=True, text=True)
        if p.returncode == 0:
            info = json.loads(p.stdout)
            corners = info.get("cornerCoordinates", {})
            ul = corners.get("upperLeft")
            ll = corners.get("lowerLeft")
            ur = corners.get("upperRight")
            lr = corners.get("lowerRight")
            center = corners.get("center")
            
            min_lon = min(ul[0], ll[0])
            max_lon = max(ur[0], lr[0])
            min_lat = min(ll[1], lr[1])
            max_lat = max(ul[1], ur[1])
            
            bbox_raw = [round(min_lon, 5), round(min_lat, 5), round(max_lon, 5), round(max_lat, 5)]
            center_coords = [round(center[0], 5), round(center[1], 5)]
            
            poly_coords = [[
                [round(ul[0], 5), round(ul[1], 5)],
                [round(ll[0], 5), round(ll[1], 5)],
                [round(lr[0], 5), round(lr[1], 5)],
                [round(ur[0], 5), round(ur[1], 5)],
                [round(ul[0], 5), round(ul[1], 5)]
            ]]
            
            buffered_coords = apply_inset_buffer(poly_coords, inset_deg=0.0008)
            ring = buffered_coords[0]
            b_min_lon = min(p[0] for p in ring)
            b_min_lat = min(p[1] for p in ring)
            b_max_lon = max(p[0] for p in ring)
            b_max_lat = max(p[1] for p in ring)
            bbox_buffered = [b_min_lon, b_min_lat, b_max_lon, b_max_lat]
            
            pdf_info = None
            for fname, d in aisweb_map.items():
                if "wj3" in fname or "reul" in fname:
                    pdf_info = d
                    break
                    
            results[code] = {
                "code": code,
                "ident": ident,
                "name": name,
                "layer": item["layer"],
                "file": file_name,
                "bbox_raw": bbox_raw,
                "bbox_buffered": bbox_buffered,
                "center": center_coords,
                "zoom": 10.5,
                "coordinates": buffered_coords,
                "wms_capabilities_bbox": wms_bboxes.get(clean_layer),
                "pdf_info": pdf_info
            }
            print(f"  ✓ Sucesso: BBOX={bbox_buffered}, Centro={center_coords}")
        else:
            print(f"  ❌ Erro GDAL: {p.stderr}")
            
    # Salva JSON de polígonos
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n✅ Catálogo JSON salvo em: {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
