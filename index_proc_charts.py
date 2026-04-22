#!/usr/bin/env python3
"""
SkyFPL - Super Robô de Cartas (Versão 13.0 - Unified Pipeline)
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
<<<<<<< HEAD
=======
    'failed_charts': 0,
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
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

def extract_georef(doc, page):
    """Extrai detecção de Viewport para calibração futura."""
    try:
        keys = doc.xref_get_keys(page.xref)
        if "VP" in keys: return {"type": "GeoPDF_VP", "status": "detected"}
        if "WGS 84" in page.get_text().upper(): return {"type": "TEXT_HINT", "status": "detected"}
    except: pass
    return None

def process_pdf_to_jpg(pdf_content):
    """Converte PDF para JPEG 250 DPI de alta fidelidade."""
    try:
        doc = fitz.open(stream=pdf_content, filetype="pdf")
        page = doc[0]
        geo_data = extract_georef(doc, page)
        
        zoom = 250 / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, colorspace=fitz.csRGB)
        
        img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
        buffer = BytesIO()
        # subsampling=0 (4:4:4) garante nitidez em textos finos (ICA 96-1)
        img.save(buffer, format="JPEG", quality=90, optimize=True, progressive=True, subsampling=0)
        
        meta = {"w": pix.width, "h": pix.height, "dpi": 250, "geo": geo_data}
        doc.close()
        return buffer.getvalue(), meta
    except Exception as e:
        log.error(f"Erro processamento: {e}")
        return None, None

# ─── Infraestrutura R2 ───────────────────────────────────────────────────────

def init_s3():
<<<<<<< HEAD
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]): return None
    config = Config(connect_timeout=10, read_timeout=20, retries={'max_attempts': 2})
=======
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        log.error("❌ ERRO: Credenciais do Cloudflare R2 não encontradas nos Secrets do GitHub!")
        return None
    config = Config(connect_timeout=15, read_timeout=30, retries={'max_attempts': 3}, max_pool_connections=50)
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
    return boto3.client('s3', endpoint_url=R2_ENDPOINT, aws_access_key_id=R2_ACCESS_KEY_ID,
                        aws_secret_access_key=R2_SECRET_ACCESS_KEY, region_name='auto', config=config)

def upload_to_r2(s3, key, body, content_type):
    try:
        s3.put_object(Bucket=R2_BUCKET, Key=key, Body=body, ContentType=content_type)
        return f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{key}"
    except: return None

# ─── Lógica de Processamento ─────────────────────────────────────────────────

def process_single_chart(s3, icao, chart, airac, dry_run):
    name = chart.get('nome', 'CARTA')
    tipo = chart.get('tipo', 'UNKN')
    url_decea = chart.get('link', '')
    if not url_decea or dry_run: return 0
    
    clean_name = re.sub(r'[^\w\s-]', '', name).strip().replace(' ', '_').upper()
    base_path = f"procedural/charts/{airac}/{icao}"
    
    try:
        resp = requests.get(url_decea, timeout=30)
<<<<<<< HEAD
        if not resp.ok: return 0
        pdf_bytes = resp.content
        
        jpg_bytes, meta = process_pdf_to_jpg(pdf_bytes)
        
        url_pdf = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.pdf", pdf_bytes, 'application/pdf')
        url_jpg = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.jpg", jpg_bytes, 'image/jpeg') if jpg_bytes else None
        
=======
        if not resp.ok:
            with telemetry_lock:
                telemetry['failed_charts'] += 1
                telemetry['failed_airports'].insert(0, {
                    'icao': icao, 
                    'name': name,
                    'error': f"DECEA Offline/Erro: {resp.status_code}", 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
            return 0
            
        pdf_bytes = resp.content
        jpg_bytes, meta = process_pdf_to_jpg(pdf_bytes)
        
        if not jpg_bytes:
             with telemetry_lock:
                telemetry['failed_charts'] += 1
                telemetry['failed_airports'].insert(0, {
                    'icao': icao, 
                    'name': name,
                    'error': "Erro Conversão JPEG (PDF Inválido?)", 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
             return 0

        url_pdf = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.pdf", pdf_bytes, 'application/pdf')
        url_jpg = upload_to_r2(s3, f"{base_path}/{tipo}_{clean_name}.jpg", jpg_bytes, 'image/jpeg') if jpg_bytes else None
        
        if not url_pdf or not url_jpg:
             with telemetry_lock:
                telemetry['failed_charts'] += 1
                telemetry['failed_airports'].insert(0, {
                    'icao': icao, 
                    'name': name,
                    'error': "Erro Upload R2 (Cloudflare Timeout)", 
                    'at': datetime.now().strftime('%H:%M:%S')
                })
             return 0
        
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
        record = {
            'icao': icao, 'tipo': tipo, 'nome_procedimento': name, 'url_decea': url_decea,
            'url_r2': url_pdf, 'url_r2_jpg': url_jpg, 'airac_cycle': airac,
            'data_carta': chart.get('dt', ''), 'metadata_geo': meta, 'source': 'super-robo-v13'
        }
        
        requests.post(f"{TABLE_URL}?on_conflict=icao,tipo,nome_procedimento", json=[record], headers=HEADERS_REST, timeout=20)
        
        with telemetry_lock:
            telemetry['mirrored_charts'] += 1
            telemetry['mirrored_bytes'] += len(pdf_bytes) + (len(jpg_bytes) if jpg_bytes else 0)
            telemetry['last_processed_charts'].insert(0, {'icao': icao, 'name': name, 'url': url_jpg or url_pdf, 'at': datetime.now().strftime('%H:%M:%S')})
<<<<<<< HEAD
            if len(telemetry['last_processed_charts']) > 5: telemetry['last_processed_charts'].pop()
            
        return 1
    except Exception as e:
        log.error(f"Erro {icao} - {name}: {e}")
=======
            if len(telemetry['last_processed_charts']) > 20: telemetry['last_processed_charts'].pop()
        
        add_telemetry_log(f"✅ {icao}: {tipo} - {name} processada")
        return 1
    except Exception as e:
        err_msg = str(e)
        log.error(f"❌ Erro {icao} - {name}: {err_msg}")
        with telemetry_lock:
            telemetry['failed_charts'] += 1
            telemetry['failed_airports'].insert(0, {
                'icao': icao, 
                'name': name,
                'error': f"Falha Crítica: {err_msg}", 
                'at': datetime.now().strftime('%H:%M:%S')
            })
            if len(telemetry['failed_airports']) > 30: telemetry['failed_airports'].pop()
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
        return 0

def fetch_charts_for_icao(icao):
    url = f"{SUPABASE_URL}/functions/v1/fetch-charts"
    try:
        r = requests.post(url, json={'icaoCode': icao}, headers=HEADERS_REST, timeout=30)
        return r.json().get('charts', []) if r.ok else []
    except: return []

def export_master_json(s3, airac):
    all_records = []
    offset = 0
    while True:
        r = requests.get(f"{TABLE_URL}?select=*&limit=1000&offset={offset}", headers=HEADERS_REST, timeout=60)
        data = r.json()
        if not data: break
        all_records.extend(data)
        offset += 1000
        if len(data) < 1000: break
    
    payload = {'metadata': {'generated_at': datetime.now(timezone.utc).isoformat(), 'airac_cycle': airac, 'total': len(all_records)}, 'data': all_records}
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
    
<<<<<<< HEAD
=======
    # Reinicializa a telemetria global para esta execução
    with telemetry_lock:
        telemetry.update({
            'status': 'Iniciando Super Robô v13.0...',
            'current_icao': '',
            'progress': 0,
            'total_airports': 0,
            'total_offered': 0,
            'total_charts': 0,
            'processed_airports': 0,
            'master_file_size': 0,
            'mirrored_charts': 0,
            'failed_charts': 0,
            'mirrored_bytes': 0,
            'logs': [],
            'failed_airports': [],
            'last_processed_charts': []
        })
    
    # FORÇA UPLOAD IMEDIATO (Feedback instantâneo para o Dashboard)
    upload_telemetry(s3, telemetry)
    
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
    def handle_stop(s, f):
        telemetry['status'] = 'stopped'
        upload_telemetry(s3, telemetry)
        sys.exit(0)
    signal.signal(signal.SIGTERM, handle_stop)
    
    add_telemetry_log(f"🚀 Super Robô v13.0 Iniciado | 250 DPI | AIRAC {airac}")
    
    icao_list = [c.strip().upper() for c in args.icao.split(',')] if args.icao else []
    if not icao_list:
<<<<<<< HEAD
        r = requests.get('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json')
        icao_list = sorted({p['icao'] for p in r.json().get('data', []) if p.get('icao')})
=======
        add_telemetry_log("🌍 Baixando malha aérea brasileira para filtragem...")
        r = requests.get('https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json')
        nav_data = r.json().get('data', [])
        # 🛡️ FILTRO TÁTICO: Apenas aeroportos e helipontos (evita 10k waypoints inúteis)
        icao_list = sorted({p['icao'] for p in nav_data if p.get('icao') and p.get('type') in ['airport', 'heliport']})
        add_telemetry_log(f"✅ Malha filtrada: {len(icao_list)} aeródromos identificados (de {len(nav_data)} pontos totais).")
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
    
    telemetry['total_airports'] = len(icao_list)
    telemetry['status'] = 'in_progress'
    
    stop_heartbeat = threading.Event()
    def hb():
        while not stop_heartbeat.is_set():
<<<<<<< HEAD
=======
            # Força upload para o dashboard ver o status de consulta
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
            upload_telemetry(s3, telemetry)
            time.sleep(10)
    threading.Thread(target=hb, daemon=True).start()
    
<<<<<<< HEAD
    all_tasks = []
    for icao in icao_list:
        charts = fetch_charts_for_icao(icao)
        telemetry['total_offered'] += len(charts)
        for c in charts: all_tasks.append((icao, c))
    
    add_telemetry_log(f"📦 Processando {len(all_tasks)} cartas com {args.workers} workers...")
    
    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        futures = [exe.submit(process_single_chart, s3, t[0], t[1], airac, dry_run) for t in all_tasks]
        for f in as_completed(futures): pass
        
    stop_heartbeat.set()
    add_telemetry_log("📦 Finalizando Master JSON...")
    if not dry_run: export_master_json(s3, airac)
=======
    add_telemetry_log(f"🌍 Iniciando Descoberta Paralela para {len(icao_list)} aeródromos...")
    all_tasks = []
    
    # 🚀 TURBO DISCOVERY: Busca listas de cartas em paralelo
    with ThreadPoolExecutor(max_workers=args.workers * 3) as discovery_exe:
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
            
            # Atualiza status visual de progresso da descoberta
            if processed_discovery % 50 == 0 or processed_discovery == total_to_discover:
                with telemetry_lock:
                    telemetry['status'] = f"Descobrindo: {processed_discovery}/{total_to_discover} aeródromos..."
                    telemetry['progress'] = int((processed_discovery / total_to_discover) * 20) # Os primeiros 20% são descoberta
                upload_telemetry(s3, telemetry)
    
    add_telemetry_log(f"✅ Descoberta concluída: {len(all_tasks)} cartas encontradas em {len(icao_list)} aeródromos.")
    
    processed_airports_set = set()
    with ThreadPoolExecutor(max_workers=args.workers) as exe:
        futures = {exe.submit(process_single_chart, s3, t[0], t[1], airac, dry_run): t[0] for t in all_tasks}
        for future in as_completed(futures):
            icao_task = futures[future]
            with telemetry_lock:
                telemetry['current_icao'] = icao_task
                telemetry['total_charts'] += 1
                processed_airports_set.add(icao_task)
                telemetry['progress'] = len(processed_airports_set)
        
    stop_heartbeat.set()
    
    # ─── RECONCILIAÇÃO FINAL (Tolerância Zero) ───────────────────────────────
    total_offered = telemetry['total_offered']
    total_success = telemetry['mirrored_charts']
    total_failed  = telemetry['failed_charts']
    diff = total_offered - (total_success + total_failed)
    
    if diff == 0 and total_failed == 0:
        add_telemetry_log(f"💎 INTEGRIDADE 100%: Todas as {total_offered} cartas foram processadas com sucesso.")
    elif diff == 0 and total_failed > 0:
        add_telemetry_log(f"⚠️ ATENÇÃO: {total_success} sucessos, {total_failed} falhas registradas. Total reconciliado.")
    else:
        add_telemetry_log(f"🚨 ALERTA CRÍTICO: Diferença de {diff} cartas não contabilizadas!")
    
    add_telemetry_log("📦 Finalizando Master JSON...")
    if not dry_run:
        size = export_master_json(s3, airac)
        with telemetry_lock:
            telemetry['master_file_size'] = size
>>>>>>> e533f6fddebc0a0f28366771d863e766bc7a2fad
    
    telemetry['status'] = 'completed'
    add_telemetry_log(f"✅ Concluído! {telemetry['mirrored_charts']} cartas processadas.")
    upload_telemetry(s3, telemetry)

if __name__ == "__main__":
    main()
