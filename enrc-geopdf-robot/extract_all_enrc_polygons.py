"""
extract_all_enrc_polygons.py — SkyFPL Conic Neatline Extractor (DECEA Official GeoTIFFs)
========================================================================================
Baixa diretamente os GeoTIFFs oficiais do DECEA (GeoAISWEB) para as cartas ENRC LOW (L1 a L9),
inspeciona o canal Alfa (Banda 4), extrai o contorno curvo de alta precisão (projeção Lambert)
e gera a base canônica de corte para o robô e para o Dashboard Admin.
"""

import os
import sys
import json
import time
import urllib.request
import tempfile
from osgeo import gdal, ogr

gdal.UseExceptions()

CODES = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9"]
BASE_URL = "https://geoaisweb.decea.mil.br/src/geotiffs/ENRC_{code}.tif"
OUTPUT_JSON = os.path.join(os.path.dirname(__file__), "enrc_official_polygons.json")
OUTPUT_TS = r"C:\Users\josemir\Desktop\skynav-pro-official\admin\src\data\enrcOfficialPolygons.ts"

def download_file(url: str, dest_path: str):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) SkyFPL/Robot-HD"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        with open(dest_path, "wb") as f:
            while True:
                chunk = resp.read(256 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = int(downloaded / total * 100)
                    print(f"\r  Progresso: {downloaded / (1024*1024):.1f} MB / {total / (1024*1024):.1f} MB ({pct}%)", end="", flush=True)
        print()

def extract_polygon_from_tif(tif_path: str, code: str) -> dict:
    print(f"\n[{code}] Abrindo GeoTIFF: {os.path.basename(tif_path)}...")
    ds = gdal.Open(tif_path)
    w, h = ds.RasterXSize, ds.RasterYSize
    print(f"  Dimensões: {w}x{h} pixels, Bandas: {ds.RasterCount}")

    # Verificar se possui banda Alfa (Banda 4)
    if ds.RasterCount < 4:
        raise ValueError(f"O arquivo {tif_path} não possui 4 bandas (necessário RGBA)")

    b4 = ds.GetRasterBand(4)
    
    # Criar camada em memória para vetorização
    mem_drv = ogr.GetDriverByName("MEM")
    mem_ds = mem_drv.CreateDataSource("mem_ds")
    layer = mem_ds.CreateLayer("neatline", None, ogr.wkbPolygon)
    layer.CreateField(ogr.FieldDefn("val", ogr.OFTInteger))

    print(f"  Executando gdal.Polygonize na Banda 4...")
    t0 = time.time()
    gdal.Polygonize(b4, b4, layer, 0, [])
    print(f"  Vetorização concluída em {time.time()-t0:.1f}s. Encontradas {layer.GetFeatureCount()} feições.")

    # Localizar a feição de valor 255 com maior área (o corpo principal da carta)
    best_geom = None
    max_area = 0.0
    for feat in layer:
        val = feat.GetField("val")
        if val == 255:
            geom = feat.GetGeometryRef()
            if geom:
                area = geom.GetArea()
                if area > max_area:
                    max_area = area
                    best_geom = geom.Clone()

    if not best_geom:
        raise ValueError(f"Nenhum polígono opaco (Alfa=255) encontrado em {code}")

    print(f"  Área do polígono principal: {max_area:.4f} graus²")

    # Testar tolerância de simplificação para obter curva suave sem dentes
    # Tolerância de 0.001 graus corresponde a aprox. 110 metros na latitude média do Brasil
    tol = 0.001
    simp = best_geom.SimplifyPreserveTopology(tol)
    ring = simp.GetGeometryRef(0)
    pt_count = ring.GetPointCount()

    if pt_count < 20:
        tol = 0.0006
        simp = best_geom.SimplifyPreserveTopology(tol)
        ring = simp.GetGeometryRef(0)
        pt_count = ring.GetPointCount()
    elif pt_count > 120:
        tol = 0.0012
        simp = best_geom.SimplifyPreserveTopology(tol)
        ring = simp.GetGeometryRef(0)
        pt_count = ring.GetPointCount()

    print(f"  Polígono simplificado com tolerância {tol}°: {pt_count} vértices")

    pts = []
    lons = []
    lats = []
    for i in range(pt_count):
        x = round(ring.GetX(i), 5)
        y = round(ring.GetY(i), 5)
        pts.append([x, y])
        lons.append(x)
        lats.append(y)

    # Garantir anel fechado
    if pts[0] != pts[-1]:
        pts.append(pts[0])

    bbox = [
        round(min(lons), 5),
        round(min(lats), 5),
        round(max(lons), 5),
        round(max(lats), 5)
    ]
    print(f"  BBOX Calculado: {bbox}")

    return {
        "code": code,
        "ident": f"ENRC_{code}",
        "bbox": bbox,
        "coordinates": [pts]
    }

def main():
    print("=" * 70)
    print("SkyFPL — Extração Automática de Polígonos Cônicos DECEA (ENRC L1 a L9)")
    print("=" * 70)

    results = {}
    temp_dir = tempfile.mkdtemp(prefix="decea_geotiffs_")

    try:
        for code in CODES:
            # 1. Verificar se já existe em Downloads
            local_candidate = os.path.join(r"C:\Users\josemir\Downloads", f"ENRC_{code}.tif")
            downloaded_temp = None

            if os.path.exists(local_candidate):
                tif_file = local_candidate
                print(f"\n[INFO] Usando GeoTIFF local já existente: {local_candidate}")
            else:
                url = BASE_URL.format(code=code)
                dest = os.path.join(temp_dir, f"ENRC_{code}.tif")
                print(f"\n[DOWNLOAD] Baixando {code} do DECEA: {url}...")
                t_dl = time.time()
                try:
                    download_file(url, dest)
                    print(f"  Download de {code} concluído em {time.time()-t_dl:.1f}s ({os.path.getsize(dest)/(1024*1024):.1f} MB)")
                    tif_file = dest
                    downloaded_temp = dest
                except Exception as e:
                    print(f"  [ERRO] Falha ao baixar {url}: {e}")
                    continue

            # 2. Extrair polígono
            try:
                poly_data = extract_polygon_from_tif(tif_file, code)
                results[code] = poly_data
            except Exception as e:
                print(f"  [ERRO] Falha ao extrair polígono de {code}: {e}")
            finally:
                # Se foi arquivo temporário baixado, remover para poupar disco
                if downloaded_temp and os.path.exists(downloaded_temp):
                    try:
                        os.remove(downloaded_temp)
                        print(f"  Arquivo temporário removido ({code}).")
                    except Exception:
                        pass

        # 3. Salvar no JSON do robô
        print("\n" + "=" * 70)
        print(f"Gravando {len(results)} polígonos em {OUTPUT_JSON}...")
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        print("  Arquivo JSON atualizado com sucesso!")

        # 4. Salvar no TypeScript do Dashboard Admin
        print(f"Atualizando arquivo TypeScript em {OUTPUT_TS}...")
        ts_content = """// ─── Polígonos Vetoriais Oficiais DECEA ICA (Extraídos do GeoServer ICA:ENRC_L) ───
// Extraídos diretamente da máscara de transparência das cartas oficiais DECEA (GeoTIFFs oficiais).
// Elimina 100% de margens de papel do GeoPDF, dentes de 90° e preserva a curvatura cônica de Lambert.

export interface EnrcChartPolygon {
  code: string;
  ident: string;
  bbox: [number, number, number, number];
  coordinates: number[][][];
}

export const ENRC_OFFICIAL_POLYGONS: Record<string, EnrcChartPolygon> = """ + json.dumps(results, indent=2) + ";\n"

        with open(OUTPUT_TS, "w", encoding="utf-8") as f:
            f.write(ts_content)
        print("  Arquivo TypeScript atualizado com sucesso!")

        print("\n" + "=" * 70)
        print("RESUMO DA EXTRAÇÃO:")
        print(f"{'CÓDIGO':<8} | {'VÉRTICES':<10} | {'BBOX (W, S, E, N)'}")
        print("-" * 70)
        for code in CODES:
            if code in results:
                v_count = len(results[code]["coordinates"][0])
                bbox_str = str(results[code]["bbox"])
                print(f"{code:<8} | {v_count:<10} | {bbox_str}")
            else:
                print(f"{code:<8} | {'FALHA':<10} | -")
        print("=" * 70)

    finally:
        # Limpar diretório temporário se sobrou algo
        if os.path.exists(temp_dir):
            try:
                import shutil
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass

if __name__ == "__main__":
    main()
