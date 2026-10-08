"""
extract_all_rea_polygons.py — SkyFPL REA HD BBOX & Neatline Catalog Extractor (v2)
====================================================================================
Extrai a geometria exata da moldura oficial (NEATLINE) das Cartas de Rotas Especiais
de Aeronaves (REA) publicadas pelo DECEA:
  1. Extrai o metadado NEATLINE embutido no GeoPDF pelo ArcGIS Pro do DECEA.
  2. Reprojeta os vértices da zona UTM nativa para coordenadas geográficas EPSG:4326.
  3. Aplica Inset Buffer de segurança (~80 m) para eliminar qualquer linha preta/margem de papel.
  4. Possui fallback automático para a extensão oficial do GeoTIFF DECEA e BBOX do WMS GeoServer.
  5. Atualiza:
     - rea-geopdf-robot/rea_catalog.json (Catálogo canônico)
     - rea-geopdf-robot/rea_official_polygons.json (Recorte oficial canônico)
     - skynav-pro-official/admin/src/data/reaOfficialPolygons.ts (Consumo do Dashboard)
"""

import os
import sys
import json
import re
import shutil
import urllib.request
import subprocess
import tempfile
import xml.etree.ElementTree as ET

# ─── Utilitários Cartográficos GDAL ──────────────────────────────────────────

def find_gdal_tool(tool_name: str) -> str:
    """Localiza utilitários do GDAL no Windows e Linux."""
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
OGR2OGR = find_gdal_tool("ogr2ogr")

# ─── Mapeamento Canônico dos 26 Setores Homologados pelo DECEA ────────────────

CANONICAL_SECTORS = [
    {"code": "REA_CURITIBA", "name": "Curitiba (TMA-CT)", "layer": "ICA:REA_CURITIBA", "file": "REA_CURITIBA.tif", "center": [-49.125, -25.84], "zoom": 9},
    {"code": "REA_CY_CUIABA", "name": "Cuiabá", "layer": "ICA:CCV_REA_CY_CUIABA", "file": "CCV_REA_CY_CUIABA.tif", "center": [-56.12, -15.62], "zoom": 9},
    {"code": "REA_LONDRINA", "name": "Londrina", "layer": "ICA:REA_LONDRINA", "file": "REA_LONDRINA.tif", "center": [-51.50, -23.33], "zoom": 9},
    {"code": "REA_PI-PARINTINS", "name": "Parintins", "layer": "ICA:CCV_REA_PI-PARINTINS", "file": "CCV_REA_PI-PARINTINS.tif", "center": [-56.74, -2.70], "zoom": 9},
    {"code": "REA_RIBEIRAO_PRETO", "name": "Ribeirão Preto", "layer": "ICA:REA_RIBEIRAO_PRETO", "file": "REA_RIBEIRAO_PRETO.tif", "center": [-47.79, -21.16], "zoom": 9},
    {"code": "REA_WA_TABATINGA", "name": "Tabatinga", "layer": "ICA:CCV_REA_WA_TABATINGA", "file": "CCV_REA_WA_TABATINGA.tif", "center": [-69.87, -4.20], "zoom": 9},
    {"code": "REA_WB_BELEM", "name": "Belém", "layer": "ICA:CCV_REA_WB_BELEM", "file": "CCV_REA_WB_BELEM.tif", "center": [-48.40, -1.37], "zoom": 9},
    {"code": "REA_WF_RECIFE", "name": "Recife", "layer": "ICA:CCV_REA_WF_RECIFE", "file": "CCV_REA_WF_RECIFE.tif", "center": [-35.01, -8.04], "zoom": 9},
    {"code": "REA_WG_CAMPO_GRANDE", "name": "Campo Grande", "layer": "ICA:CCV_REA_WG_CAMPO_GRANDE", "file": "CCV_REA_WG_CAMPO_GRANDE.tif", "center": [-54.66, -20.46], "zoom": 9},
    {"code": "REA_WH_BELO_HORIZONTE", "name": "Belo Horizonte", "layer": "ICA:CCV_REA_WH_BELO_HORIZONTE", "file": "CCV_REA_WH_BELO_HORIZONTE.tif", "center": [-43.86, -19.78], "zoom": 9},
    {"code": "REA_WJ1_RIO_DE_JANEIRO", "name": "Rio de Janeiro", "layer": "ICA:CCV_REA_WJ1_RIO_DE_JANEIRO", "file": "CCV_REA_WJ1_RIO_DE_JANEIRO.tif", "center": [-43.28, -22.91], "zoom": 9},
    {"code": "REA_WK_PORTO_SEGURO", "name": "Porto Seguro", "layer": "ICA:CCV_REA_WK_PORTO_SEGURO", "file": "CCV_REA_WK_PORTO_SEGURO.tif", "center": [-39.14, -16.56], "zoom": 9},
    {"code": "REA_WN2_MANAUS", "name": "Manaus", "layer": "ICA:CCV_REA_WN2_MANAUS", "file": "CCV_REA_WN2_MANAUS.tif", "center": [-60.08, -3.11], "zoom": 9},
    {"code": "REA_WP_PORTO_ALEGRE", "name": "Porto Alegre", "layer": "ICA:CCV_REA_WP_PORTO_ALEGRE", "file": "CCV_REA_WP_PORTO_ALEGRE.tif", "center": [-51.10, -29.75], "zoom": 9},
    {"code": "REA_WR_BRASILIA", "name": "Brasília", "layer": "ICA:CCV_REA_WR_BRASILIA", "file": "CCV_REA_WR_BRASILIA.tif", "center": [-47.91, -15.81], "zoom": 9},
    {"code": "REA_WS_SAO_LUIS", "name": "São Luís", "layer": "ICA:CCV_REA_WS_SAO_LUIS", "file": "CCV_REA_WS_SAO_LUIS.tif", "center": [-44.24, -2.57], "zoom": 9},
    {"code": "REA_WX_SANTAREM", "name": "Santarém", "layer": "ICA:CCV_REA_WX_SANTAREM", "file": "CCV_REA_WX_SANTAREM.tif", "center": [-54.83, -2.50], "zoom": 9},
    {"code": "REA_WZ_FORTALEZA", "name": "Fortaleza", "layer": "ICA:CCV_REA_WZ_FORTALEZA", "file": "CCV_REA_WZ_FORTALEZA.tif", "center": [-38.46, -3.80], "zoom": 9},
    {"code": "REA_XF_FLORIANOPOLIS", "name": "Florianópolis", "layer": "ICA:CCV_REA_XF_FLORIANOPOLIS", "file": "CCV_REA_XF_FLORIANOPOLIS.tif", "center": [-48.87, -27.40], "zoom": 9},
    {"code": "REA_XK_MACAPA", "name": "Macapá", "layer": "ICA:CCV_REA_XK_MACAPA", "file": "CCV_REA_XK_MACAPA.tif", "center": [-51.12, 0.05], "zoom": 9},
    {"code": "REA_XN-ANAPOLIS", "name": "Anápolis", "layer": "ICA:CCV_REA_XN-ANAPOLIS", "file": "CCV_REA_XN-ANAPOLIS.tif", "center": [-49.00, -16.33], "zoom": 9},
    {"code": "REA_XP1_SAO_PAULO", "name": "São Paulo 1 (RMSP)", "layer": "ICA:CCV_REA_XP1_SAO_PAULO", "file": "CCV_REA_XP1_SAO_PAULO.tif", "center": [-46.58, -23.62], "zoom": 9},
    {"code": "REA_XP2_SAO_PAULO", "name": "São Paulo 2", "layer": "ICA:CCV_REA_XP2_SAO_PAULO", "file": "CCV_REA_XP2_SAO_PAULO.tif", "center": [-46.58, -23.62], "zoom": 9},
    {"code": "REA_XR_VITORIA", "name": "Vitória", "layer": "ICA:CCV_REA_XR_VITORIA", "file": "CCV_REA_XR_VITORIA.tif", "center": [-40.29, -20.25], "zoom": 9},
    {"code": "REA_XS_SALVADOR", "name": "Salvador", "layer": "ICA:CCV_REA_XS_SALVADOR", "file": "CCV_REA_XS_SALVADOR.tif", "center": [-38.33, -12.83], "zoom": 9},
    {"code": "REA_XT_NATAL", "name": "Natal", "layer": "ICA:CCV_REA_XT_NATAL", "file": "CCV_REA_XT_NATAL.tif", "center": [-35.29, -5.70], "zoom": 9}
]

WMS_CAPABILITIES_URL = "https://geoaisweb.decea.mil.br/geoserver/ICA/wms?service=WMS&request=GetCapabilities"
GEOTIFF_BASE_URL = "https://geoaisweb.decea.mil.br/src/geotiffs/{filename}"
AISWEB_VISUAIS_URL = "https://aisweb.decea.mil.br/?i=cartas&p=visuais"

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/REA-Catalog-Extractor"}

OUTPUT_CATALOG = os.path.join(os.path.dirname(__file__), "rea_catalog.json")
OUTPUT_POLYGONS = os.path.join(os.path.dirname(__file__), "rea_official_polygons.json")
OUTPUT_TS = r"C:\Users\josemir\Desktop\skynav-pro-official\admin\src\data\reaOfficialPolygons.ts"

# ─── Descoberta de Dados Oficiais ─────────────────────────────────────────────

def fetch_decea_capabilities():
    """Obtém BBOXes oficiais do XML GetCapabilities do DECEA GeoServer como fallback."""
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

def fetch_live_rea_urls():
    """Mapeia dinamicamente as URLs dos PDFs vigentes na página do AISWEB."""
    print("🌐 [AISWEB] Descobrindo URLs vigentes de GeoPDF...")
    pdf_map = {}
    try:
        req = urllib.request.Request(AISWEB_VISUAIS_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        rows = re.findall(r"<tr[^>]*>.*?</tr>", html, re.DOTALL)
        rea_rows = [row for row in rows if 'data-order="REA"' in row]
        for row in rea_rows:
            m = re.search(r'href=[\'"]([^\'"]+\.pdf)[\'"]', row)
            if m:
                u = m.group(1).strip()
                fname = os.path.basename(u).lower()
                full_url = u if u.startswith("http") else f"https://aisweb.decea.mil.br/{u.lstrip('/')}"
                pdf_map[fname] = full_url
    except Exception as e:
        print(f"  [Aviso] Falha ao consultar AISWEB ({e})")
    return pdf_map

def project_neatline_wkt_to_4326(neatline_wkt: str, srs_wkt_or_proj: str) -> list:
    """Reprojeta um polígono WKT (ex: UTM nativa do DECEA) para EPSG:4326 usando ogr2ogr."""
    raw_pts = neatline_wkt.replace("POLYGON ((", "").replace("))", "").split(",")
    pts = [[float(c) for c in pt.strip().split()[:2]] for pt in raw_pts]
    
    in_geojson = {
        "type": "FeatureCollection",
        "features": [{
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [pts]},
            "properties": {}
        }]
    }
    
    with tempfile.NamedTemporaryFile("w", suffix=".geojson", delete=False, encoding="utf-8") as f_in:
        in_path = f_in.name
        json.dump(in_geojson, f_in)
        
    out_path = os.path.join(tempfile.gettempdir(), f"neat_out_{os.getpid()}_{int(tempfile._get_candidate_names().__next__(), 36)}.geojson")
    if os.path.exists(out_path):
        os.remove(out_path)
        
    cmd = [OGR2OGR, "-f", "GeoJSON", out_path, in_path, "-s_srs", srs_wkt_or_proj, "-t_srs", "EPSG:4326"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    
    coords = None
    if res.returncode == 0 and os.path.exists(out_path):
        with open(out_path, "r", encoding="utf-8") as f_out:
            data = json.load(f_out)
            coords = data["features"][0]["geometry"]["coordinates"]
            
    if os.path.exists(in_path):
        os.remove(in_path)
    if os.path.exists(out_path):
        os.remove(out_path)
        
    return coords

def apply_inset_buffer(coords: list, inset_deg: float = 0.0008) -> list:
    """
    Aplica recuo interno de segurança no anel poligonal (~80 metros)
    para garantir que a linha preta da moldura externa do DECEA nunca apareça.
    """
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

def bbox_to_polygon_coordinates(bbox: list):
    """Fallback para polígono retangular simples a partir de BBOX."""
    minx, miny, maxx, maxy = bbox
    return [[
        [minx, miny],
        [maxx, miny],
        [maxx, maxy],
        [minx, maxy],
        [minx, miny]
    ]]

# ─── Processamento Principal ──────────────────────────────────────────────────

def main():
    print("=" * 80)
    print("🚀 SkyFPL — Extração Fiel de Molduras e Polígonos Oficiais REA HD (v2)")
    print("=" * 80)
    
    live_pdf_map = fetch_live_rea_urls()
    decea_wms_bboxes = fetch_decea_capabilities()
    
    catalog = {}
    polygons = {}
    
    print("\n🔍 Processando os 26 setores homologados:")
    print("-" * 80)
    print(f"{'CÓDIGO':<24} | {'FONTE DE CORTE':<16} | {'VÉRTICES':<8} | {'BBOX CANÔNICO'}")
    print("-" * 80)
    
    success_neatline = 0
    
    for item in CANONICAL_SECTORS:
        code = item["code"]
        name = item["name"]
        layer = item["layer"]
        clean_layer = layer.replace("ICA:", "")
        file_name = item["file"]
        center = item["center"]
        zoom = item["zoom"]
        c_lower = code.lower()
        
        # 1. Resolver URL do GeoPDF
        pdf_url = None
        for fname, u in live_pdf_map.items():
            if ("cuiaba" in c_lower and "cuiaba" in fname) or \
               ("curitiba" in c_lower and "curitiba" in fname) or \
               ("ribeirao" in c_lower and "ribeirao" in fname) or \
               ("anapolis" in c_lower and "anapolis" in fname) or \
               ("londrina" in c_lower and "londrina" in fname) or \
               ("xp1" in c_lower and "xp1" in fname) or \
               ("xp2" in c_lower and "xp2" in fname) or \
               ("wj1" in c_lower and "wj1" in fname):
                pdf_url = u
                break
            else:
                suffix = code.replace("REA_", "").lower().replace("-", "_")
                tokens = [t for t in suffix.split("_") if len(t) > 2]
                if tokens and all(t in fname for t in tokens):
                    pdf_url = u
                    break

        tif_url = GEOTIFF_BASE_URL.format(filename=file_name)
        
        extracted_coords = None
        method = "FALLBACK_WMS"
        
        # 2. Prioridade 1: Extrair NEATLINE oficial do GeoPDF do AISWEB
        if pdf_url:
            try:
                cmd = [GDALINFO, "-json", f"/vsicurl/{pdf_url}"]
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
                if res.returncode == 0:
                    raw_out = res.stdout.lstrip('\ufeff')
                    pdata = json.loads(raw_out)
                    neat = pdata.get("metadata", {}).get("", {}).get("NEATLINE")
                    wkt_srs = pdata.get("coordinateSystem", {}).get("wkt")
                    if neat and wkt_srs:
                        raw_coords = project_neatline_wkt_to_4326(neat, wkt_srs)
                        if raw_coords:
                            extracted_coords = apply_inset_buffer(raw_coords, inset_deg=0.0008)
                            method = "GEOPDF_NEATLINE"
                            success_neatline += 1
            except Exception:
                pass

        # 3. Prioridade 2: wgs84Extent do GeoTIFF oficial da CDN do DECEA
        if not extracted_coords and tif_url:
            try:
                cmd = [GDALINFO, "-json", f"/vsicurl/{tif_url}"]
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
                if res.returncode == 0:
                    raw_out = res.stdout.lstrip('\ufeff')
                    tdata = json.loads(raw_out)
                    extent = tdata.get("wgs84Extent")
                    if extent and extent.get("coordinates"):
                        extracted_coords = apply_inset_buffer(extent["coordinates"], inset_deg=0.0008)
                        method = "GEOTIFF_EXTENT"
                        success_neatline += 1
            except Exception:
                pass
                
        # 4. Fallback WMS se ambos falharem
        if not extracted_coords:
            fallback_bbox = decea_wms_bboxes.get(clean_layer, [-47.89662, -24.50335, -44.39567, -22.2852])
            extracted_coords = bbox_to_polygon_coordinates(fallback_bbox)
            method = "FALLBACK_WMS"

        ring = extracted_coords[0]
        lons = [p[0] for p in ring]
        lats = [p[1] for p in ring]
        final_bbox = [round(min(lons), 5), round(min(lats), 5), round(max(lons), 5), round(max(lats), 5)]
        v_count = len(ring)
        
        print(f"{code:<24} | {method:<16} | {v_count:<8} | {final_bbox}")

        # Monta catálogo
        catalog[code] = {
            "code": code,
            "name": name,
            "layer": layer,
            "file": file_name,
            "pdf_url": pdf_url or "",
            "geotiff_url": tif_url,
            "bbox": final_bbox,
            "center": center,
            "zoom": zoom,
            "z_order": 1
        }
        
        # Monta geometria oficial de recorte
        polygons[code] = {
            "code": code,
            "ident": clean_layer,
            "bbox": final_bbox,
            "coordinates": extracted_coords
        }

    print("-" * 80)
    print(f"✅ Extração de molduras concluída: {success_neatline}/26 com corte exato via NEATLINE/GeoTIFF.")
    
    # ─── 5. Gravar rea_catalog.json ───────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_CATALOG), exist_ok=True)
    with open(OUTPUT_CATALOG, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
    print(f"\n📁 Catálogo gravado com sucesso: {OUTPUT_CATALOG}")

    # ─── 6. Gravar rea_official_polygons.json ─────────────────────────────────
    with open(OUTPUT_POLYGONS, "w", encoding="utf-8") as f:
        json.dump(polygons, f, indent=2, ensure_ascii=False)
    print(f"📁 Polígonos de corte gravados: {OUTPUT_POLYGONS}")

    # ─── 7. Gravar reaOfficialPolygons.ts para o Admin Dashboard ──────────────
    os.makedirs(os.path.dirname(OUTPUT_TS), exist_ok=True)
    ts_content = """// ─── Polígonos e BBOXes Oficiais DECEA ICA das Cartas REA (Neatline v2) ───
// Extraídos diretamente da moldura interna (NEATLINE) e reprojetados de UTM para WGS84.
// Elimina sobras de borda e espaços vazios no mapa e no Dashboard Admin.

export interface ReaSectorPolygon {
  code: string;
  ident: string;
  bbox: [number, number, number, number];
  coordinates: number[][][];
}

export const REA_OFFICIAL_POLYGONS: Record<string, ReaSectorPolygon> = """ + json.dumps(polygons, indent=2, ensure_ascii=False) + ";\n"

    with open(OUTPUT_TS, "w", encoding="utf-8") as f:
        f.write(ts_content)
    print(f"📁 TypeScript exportado para o Dashboard: {OUTPUT_TS}")

    print("\n" + "=" * 80)
    print("🎉 ATUALIZAÇÃO CONCLUÍDA COM SUCESSO!")
    print("=" * 80)

if __name__ == "__main__":
    main()
