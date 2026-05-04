#!/usr/bin/env python3
"""
SkyFPL - Super Robô de Cartas (Versão 14.0 - Unified Precision Pipeline)
============================================================
Indexação, Conversão (250 DPI), Extração GeoPDF (ICA 96-1) e Upload R2.
"""

import os
import sys
import json
import time
import argparse
import logging
import requests
import boto3
import signal
import threading
import re
import socket
import fitz  # PyMuPDF
from io import BytesIO
from PIL import Image
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

# Nuclear Timeout
socket.setdefaulttimeout(30)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('SuperRobo')

# ─── Configurações ────────────────────────────────────────────────────────────
SUPABASE_URL              = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
TABLE_URL                 = f"{SUPABASE_URL}/rest/v1/charts_procedural"
R2_BUCKET                 = "skyfpl-charts"
R2_ACCESS_KEY_ID          = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY      = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT               = os.environ.get('R2_ENDPOINT')

HEADERS_REST = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'apikey': SUPABASE_SERVICE_ROLE_KEY,
    'Prefer': 'resolution=merge-duplicates',
}

# ─── Gerenciamento de Telemetria ──────────────────────────────────────────────
telemetry_lock = threading.Lock()
telemetry = {
    'status': 'initializing',
    'current_icao': '',
    'progress': 0,
    'total_airports': 0,
    'total_offered': 0,
    'total_charts': 0,
    'mirrored_charts': 0,
    'failed_charts': 0,
    'mirrored_bytes': 0,
    'logs': [],
    'failed_airports': [],
    'last_processed_charts': []
}

def upload_telemetry(s3, snapshot):
    if not s3: return
    try:
        snapshot['updated_at'] = time.time()
        r2_key = "procedural/telemetry.json"
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=json.dumps(snapshot, ensure_ascii=False).encode('utf-8'),
            ContentType='application/json'
        )
    except Exception as e:
        log.error(f"❌ Erro telemetria: {e}")

def add_telemetry_log(message):
    log.info(message)
    with telemetry_lock:
        telemetry['logs'].insert(0, f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        if len(telemetry['logs']) > 20: telemetry['logs'] = telemetry['logs'][:20]

# ─── Motor de Geografia e Imagem (ICA 96-1) ──────────────────────────────────

import math
import re

def solve_cramer_3x3(points):
    n = len(points)
    if n < 3: return None
    sumX = sum(p['x'] for p in points)
    sumY = sum(p['y'] for p in points)
    sumV = sum(p['v'] for p in points)
    sumXX = sum(p['x']*p['x'] for p in points)
    sumYY = sum(p['y']*p['y'] for p in points)
    sumXY = sum(p['x']*p['y'] for p in points)
    sumXV = sum(p['x']*p['v'] for p in points)
    sumYV = sum(p['y']*p['v'] for p in points)
    
    det = n*(sumXX*sumYY - sumXY*sumXY) - sumX*(sumX*sumYY - sumY*sumXY) + sumY*(sumX*sumXY - sumY*sumXX)
    if abs(det) < 1e-12: return None
    
    a = (sumV*(sumXX*sumYY - sumXY*sumXY) - sumX*(sumXV*sumYY - sumYV*sumXY) + sumY*(sumXV*sumXY - sumYV*sumXX)) / det
    b = (n*(sumXV*sumYY - sumYV*sumXY) - sumV*(sumX*sumYY - sumY*sumXY) + sumY*(sumX*sumYV - sumY*sumXV)) / det
    c = (n*(sumXX*sumYV - sumXY*sumXV) - sumX*(sumX*sumYV - sumY*sumXV) + sumV*(sumX*sumXY - sumY*sumXX)) / det
    return {'a': a, 'b': b, 'c': c}

def solve_affine_4point(pdf_corners, geo_corners):
    lat_points = []
    lng_points = []
    valid = 0
    for i in range(4):
        if abs(pdf_corners[i*2]) < 0.001 and abs(pdf_corners[i*2+1]) < 0.001: continue
        if abs(geo_corners[i*2]) < 0.001 and abs(geo_corners[i*2+1]) < 0.001: continue
        lat_points.append({'x': pdf_corners[i*2], 'y': pdf_corners[i*2+1], 'v': geo_corners[i*2]})
        lng_points.append({'x': pdf_corners[i*2], 'y': pdf_corners[i*2+1], 'v': geo_corners[i*2+1]})
        valid += 1
        
    if valid < 3: return None
    lat_params = solve_cramer_3x3(lat_points)
    lng_params = solve_cramer_3x3(lng_points)
    if not lat_params or not lng_params: return None
    
    def solver(px, py):
        lat = lat_params['a']*px + lat_params['b']*py + lat_params['c']
        lng = lng_params['a']*px + lng_params['b']*py + lng_params['c']
        return [lat, lng]
    return solver

def from_meters(x, y):
    lng = (x / 20037508.34) * 180.0
    lat = (y / 20037508.34) * 180.0
    lat = (180.0 / math.pi) * (2 * math.atan(math.exp((lat * math.pi) / 180.0)) - math.pi / 2)
    return [lat, lng]

def extract_georef(doc, page, pdf_bytes):
    """
    Motor Tático Sentinel Bytescan: Lê o PDF cru buscando os arrays do DECEA
    e resolve a matriz de distorção LCC na nuvem para entregar cantos limpos ao App.
    """
    try:
        buf = pdf_bytes.decode('latin1', errors='ignore')
        gpts_match = re.search(r'/GPTS\s*\[([^\]]+)\]', buf)
        lpts_match = re.search(r'/LPTS\s*\[([^\]]+)\]', buf)
        
        if not gpts_match or not lpts_match:
            vps = page.get_viewports()
            if vps: return {"type": "GeoPDF_VP", "calibration": vps[0]}
            return None
            
        gpts_raw = gpts_match.group(1).replace(',', ' ').split()
        lpts_raw = lpts_match.group(1).replace(',', ' ').split()
        
        gpts = [float(x) for x in gpts_raw if x.strip()]
        lpts = [float(x) for x in lpts_raw if x.strip()]
        if len(gpts) < 8 or len(lpts) < 8: return None
        
        scale_divisor = 1.0
        if 500 < abs(gpts[0]) < 100000:
            scale_divisor = 1000.0
            
        processed_gpts = [v / scale_divisor for v in gpts]
        
        if any(abs(v) > 500 for v in processed_gpts):
            for i in range(0, len(processed_gpts), 2):
                lat, lng = from_meters(processed_gpts[i+1], processed_gpts[i])
                processed_gpts[i], processed_gpts[i+1] = lat, lng
                
        # Boxes to try
        boxes = [page.rect, page.cropbox, page.mediabox]
        unique_boxes = []
        for b in boxes:
            if not any(abs(b.x0 - ub.x0) < 1 and abs(b.y0 - ub.y0) < 1 and abs(b.width - ub.width) < 1 for ub in unique_boxes):
                unique_boxes.append(b)
                
        best_residual = float('inf')
        final_solver = None
        
        for box in unique_boxes:
            bx, by = box.x0, box.y0
            bw_orig, bh_orig = box.width, box.height
            if bw_orig <= 0 or bh_orig <= 0: continue
            
            for sx in range(90, 111):
                scale_x = sx / 100.0
                for sy in range(90, 111):
                    scale_y = sy / 100.0
                    bw, bh = bw_orig * scale_x, bh_orig * scale_y
                    
                    trial_scaled = [(v * bw + bx) if i % 2 == 0 else (v * bh + by) for i, v in enumerate(lpts)]
                    pdf_corners = trial_scaled[:8]
                    
                    tp_solver = solve_affine_4point(pdf_corners, processed_gpts[:8])
                    if tp_solver:
                        res = 0
                        for j in range(0, min(8, len(processed_gpts)), 2):
                            lat, lng = tp_solver(trial_scaled[j], trial_scaled[j+1])
                            res += abs(lat - processed_gpts[j]) + abs(lng - processed_gpts[j+1])
                        
                        if res < best_residual:
                            best_residual = res
                            final_solver = tp_solver
        
        if not final_solver:
            # Fallback direto (Mapeamento explícito)
            pdf_corners = lpts[:8]
            geo_corners = [
                processed_gpts[6], processed_gpts[7], # NW -> TL
                processed_gpts[4], processed_gpts[5], # NE -> TR
                processed_gpts[2], processed_gpts[3], # SE -> BR
                processed_gpts[0], processed_gpts[1]  # SW -> BL
            ]
            final_solver = solve_affine_4point(pdf_corners, geo_corners)
            if not final_solver: return None
            
        bw, bh = page.rect.width, page.rect.height
        
        tl_lat, tl_lon = final_solver(0, 0)
        tr_lat, tr_lon = final_solver(bw, 0)
        br_lat, br_lon = final_solver(bw, bh)
        bl_lat, bl_lon = final_solver(0, bh)
        
        return {
            "type": "Sentinel_Bytescan",
            "calibration": {
                "measure": {
                    "gpts": [
                        bl_lat, bl_lon,
                        br_lat, br_lon,
                        tr_lat, tr_lon,
                        tl_lat, tl_lon
                    ]
                }
            }
        }
    except Exception as e:
        log.debug(f"GeoRef Skip: {e}")
        return None

def process_pdf_to_jpg(pdf_content):
    """Converte PDF para JPEG 250 DPI e extrai calibração geodésica."""
    try:
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        if doc.page_count == 0: return None, None
        
        page = doc[0]
        # 🛰️ V14.2: Ativação do Motor de Precisão Sentinel Bytescan
        geo_data = extract_georef(doc, page, pdf_content)
        
        zoom = 250 / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
        
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        buffer = BytesIO()
        img.save(buffer, format="JPEG", quality=90, optimize=True, progressive=True, subsampling=0)
        
        meta = {
            "w": pix.width, 
            "h": pix.height, 
            "dpi": 250, 
            "geo": geo_data,
            "processed_at": datetime.now(timezone.utc).isoformat(),
            "version": "14.2-sentinel-bytescan"
        }
        doc.close()
        return buffer.getvalue(), meta
    except Exception as e:
        log.error(f"Erro processamento PDF: {e}")
        return None, None

# ─── Infraestrutura R2 ───────────────────────────────────────────────────────

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        log.error("❌ ERRO: Credenciais do Cloudflare R2 não encontradas!")
        return None
        
    config = Config(
        connect_timeout=15, 
        read_timeout=30, 
        retries={'max_attempts': 3}, 
        max_pool_connections=50
    )
    
    return boto3.client(
        's3', 
        endpoint_url=R2_ENDPOINT, 
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY, 
        region_name='auto', 
        config=config
    )

def upload_to_r2(s3, key, body, content_type):
    try:
        s3.put_object(Bucket=R2_BUCKET, Key=key, Body=body, ContentType=content_type)
        # URL de publicação (pode variar conforme o domínio customizado do Cloudflare)
        return f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{key}"
    except Exception as e:
        log.error(f"Erro upload R2 ({key}): {e}")
        return None

# ─── Lógica de Processamento ─────────────────────────────────────────────────

def process_single_chart(s3, icao, chart, airac, dry_run):
    name = chart.get('nome', 'CARTA')
    tipo = chart.get('tipo', 'UNKN')
    url_decea = chart.get('link', '')
    
    if not url_decea or dry_run: return 0
    
    clean_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_').upper()
    base_path = f"procedural/charts/{airac}/{icao}"
    
    MAX_RETRIES = 3
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url_decea, timeout=30)
            if not resp.ok:
                if attempt < MAX_RETRIES:
                    time.sleep(2 * attempt)
                    continue
                with telemetry_lock:
                    telemetry['failed_charts'] += 1
                    telemetry['failed_airports'].insert(0, {
                        'icao': icao, 
                        'name': name,
                        'error': f"DECEA Offline ({resp.status_code})", 
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                return 0
                
            pdf_bytes = resp.content
            jpg_bytes, meta = process_pdf_to_jpg(pdf_bytes)
            
            if not jpg_bytes:
                if attempt < MAX_RETRIES:
                    time.sleep(1)
                    continue
                with telemetry_lock:
                    telemetry['failed_charts'] += 1
                    telemetry['failed_airports'].insert(0, {
                        'icao': icao, 
                        'name': name,
                        'error': "Erro Conversão JPEG", 
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                return 0

            url_pdf = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.pdf", pdf_bytes, 'application/pdf')
            url_jpg = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.jpg", jpg_bytes, 'image/jpeg') if jpg_bytes else None
            
            if not url_pdf or not url_jpg:
                 if attempt < MAX_RETRIES:
                    time.sleep(5)
                    continue
                 with telemetry_lock:
                    telemetry['failed_charts'] += 1
                    telemetry['failed_airports'].insert(0, {
                        'icao': icao, 
                        'name': name,
                        'error': "Erro Upload R2", 
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                 return 0
            
            record = {
                'icao': icao, 
                'tipo': tipo, 
                'nome_procedimento': name, 
                'url_decea': url_decea,
                'url_r2': url_pdf, 
                'url_r2_jpg': url_jpg, 
                'airac_cycle': airac,
                'data_carta': chart.get('dt', ''), 
                'metadata_geo': meta, 
                'source': 'skyfpl-robo-v14.1'
            }
            
            # Sincronização com o Banco de Dados Supabase
            requests.post(f"{TABLE_URL}?on_conflict=icao,tipo,nome_procedimento", json=[record], headers=HEADERS_REST, timeout=20)
            
            with telemetry_lock:
                telemetry['mirrored_charts'] += 1
                telemetry['mirrored_bytes'] += len(pdf_bytes) + len(jpg_bytes)
                telemetry['last_processed_charts'].insert(0, {
                    'icao': icao, 
                    'name': name, 
                    'url': url_jpg, 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
                if len(telemetry['last_processed_charts']) > 20: telemetry['last_processed_charts'].pop()
            
            add_telemetry_log(f"✅ {icao}: {tipo} - {name} processada")
            return 1
            
        except Exception as e:
            if attempt < MAX_RETRIES:
                log.warning(f"⚠️ Tentativa {attempt} falhou para {icao} - {name}: {e}")
                time.sleep(2 * attempt)
                continue
            err_msg = str(e)
            log.error(f"❌ Falha Definitiva {icao} - {name}: {err_msg}")
            with telemetry_lock:
                telemetry['failed_charts'] += 1
                telemetry['failed_airports'].insert(0, {
                    'icao': icao, 
                    'name': name,
                    'error': f"Erro: {err_msg}", 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
            return 0

def fetch_charts_for_icao(icao):
    """Consulta a API de borda para descobrir novas cartas do DECEA."""
    url = f"{SUPABASE_URL}/functions/v1/fetch-charts"
    try:
        r = requests.post(url, json={'icaoCode': icao}, headers=HEADERS_REST, timeout=30)
        return r.json().get('charts', []) if r.ok else []
    except Exception as e:
        log.debug(f"Fetch Fail {icao}: {e}")
        return []

def export_master_json(s3, airac):
    """Gera o índice mestre de todas as cartas processadas para o App."""
    all_records = []
    offset = 0
    while True:
        r = requests.get(f"{TABLE_URL}?select=*&limit=1000&offset={offset}", headers=HEADERS_REST, timeout=60)
        data = r.json()
        if not data: break
        all_records.extend(data)
        offset += 1000
        if len(data) < 1000: break
    
    payload = {
        'metadata': {
            'generated_at': datetime.now(timezone.utc).isoformat(), 
            'airac_cycle': airac, 
            'total': len(all_records)
        }, 
        'data': all_records
    }
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    s3.put_object(Bucket=R2_BUCKET, Key='latest_proc_charts.json', Body=content, ContentType='application/json')
    return len(content)

# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--icao', help='ICAO ou lista CSV')
    parser.add_argument('--dry-run', default='False')
    parser.add_argument('--airac', help='Ciclo AIRAC')
    parser.add_argument('--workers', type=int, default=10)
    args = parser.parse_args()
    
    dry_run = str(args.dry_run).lower() == 'true'
    s3 = init_s3()
    airac = args.airac or datetime.now(timezone.utc).strftime('%y%m')
    
    # Reinicializa a telemetria global para esta execução
    with telemetry_lock:
        telemetry.update({
            'status': 'Iniciando SkyFPL Robô v14.0...',
            'current_icao': '',
            'progress': 0,
            'total_airports': 0,
            'total_offered': 0,
            'total_charts': 0,
            'mirrored_charts': 0,
            'failed_charts': 0,
            'mirrored_bytes': 0,
            'logs': [],
            'failed_airports': [],
            'last_processed_charts': []
        })
    
    # Upload imediato da telemetria para feedback no Dashboard
    upload_telemetry(s3, telemetry)
    
    def handle_stop(s, f):
        telemetry['status'] = 'stopped'
        upload_telemetry(s3, telemetry)
        sys.exit(0)
    signal.signal(signal.SIGTERM, handle_stop)
    
    add_telemetry_log(f"🚀 SkyFPL Robô v14.0 Iniciado | 250 DPI | AIRAC {airac}")
    
    icao_list = [c.strip().upper() for c in args.icao.split(',')] if args.icao else []
    if not icao_list:
        add_telemetry_log("🌍 Baixando malha aérea brasileira para filtragem...")
        try:
            r = requests.get('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json', timeout=30)
            nav_data = r.json().get('data', [])
            # Filtro: Apenas aeródromos e helipontos
            icao_list = sorted({p['icao'] for p in nav_data if p.get('icao') and p.get('type') in ['airport', 'heliport']})
            add_telemetry_log(f"✅ Malha filtrada: {len(icao_list)} aeródromos identificados.")
        except Exception as e:
            add_telemetry_log(f"❌ Erro ao baixar malha: {e}")
            sys.exit(1)
    
    telemetry['total_airports'] = len(icao_list)
    telemetry['status'] = 'in_progress'
    
    stop_heartbeat = threading.Event()
    def hb():
        while not stop_heartbeat.is_set():
            upload_telemetry(s3, telemetry)
            time.sleep(10)
    threading.Thread(target=hb, daemon=True).start()
    
    add_telemetry_log(f"🌍 Iniciando Descoberta Paralela para {len(icao_list)} aeródromos...")
    all_tasks = []
    
    # 🚀 DISCOBERTA PARALELA
    with ThreadPoolExecutor(max_workers=args.workers * 2) as discovery_exe:
        discovery_futures = {discovery_exe.submit(fetch_charts_for_icao, icao): icao for icao in icao_list}
        processed_discovery = 0
        total_to_discover = len(icao_list)
        
        for future in as_completed(discovery_futures):
            icao = discovery_futures[future]
            processed_discovery += 1
            charts = future.result()
            
            if charts:
                with telemetry_lock:
                    telemetry['total_offered'] += len(charts)
                for c in charts:
                    all_tasks.append((icao, c))
            
            if processed_discovery % 50 == 0 or processed_discovery == total_to_discover:
                with telemetry_lock:
                    telemetry['status'] = f"Descobrindo: {processed_discovery}/{total_to_discover}..."
                    telemetry['progress'] = int((processed_discovery / total_to_discover) * 20)
                upload_telemetry(s3, telemetry)
    
    add_telemetry_log(f"✅ Descoberta concluída: {len(all_tasks)} cartas encontradas.")
    
    # 🚀 PROCESSAMENTO PARALELO (Otimizado)
    processed_count = 0
    total_tasks = len(all_tasks)
    
    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        futures = {exe.submit(process_single_chart, s3, t[0], t[1], airac, dry_run): t[0] for t in all_tasks}
        for future in as_completed(futures):
            icao_task = futures[future]
            processed_count += 1
            with telemetry_lock:
                telemetry['current_icao'] = icao_task
                telemetry['total_charts'] = processed_count
                # O progresso vai de 20% a 100%
                telemetry['progress'] = 20 + int((processed_count / total_tasks) * 80)
        
    stop_heartbeat.set()
    
    # ─── RECONCILIAÇÃO FINAL ───────────────────────────────
    total_offered = telemetry['total_offered']
    total_success = telemetry['mirrored_charts']
    total_failed  = telemetry['failed_charts']
    
    add_telemetry_log(f"📊 Resumo: {total_success} sucessos, {total_failed} falhas.")
    
    add_telemetry_log("📦 Gerando Índice Mestre...")
    if not dry_run:
        size = export_master_json(s3, airac)
        add_telemetry_log(f"✅ Master JSON gerado ({size} bytes).")
    
    telemetry['status'] = 'completed'
    telemetry['progress'] = 100
    add_telemetry_log(f"✅ Robô v14.0 finalizado com sucesso!")
    upload_telemetry(s3, telemetry)

if __name__ == "__main__":
    main()
