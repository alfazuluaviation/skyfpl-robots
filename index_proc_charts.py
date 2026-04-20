#!/usr/bin/env python3
"""
SkyFPL - Robô de Indexação de Cartas (Versão 12.0 - Auditoria & Estabilidade)
========================================================================
Estratégia: Motor V10 + Telemetria com Lista de Auditoria para Dashboard.
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
import random
import socket
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests

# NUCLEAR TIMEOUT: Força qualquer operação de rede do sistema a expirar em 30s
socket.setdefaulttimeout(30)

# NUCLEAR TIMEOUT: Força qualquer operação de rede do sistema a expirar em 30s
socket.setdefaulttimeout(30)

# Sessão persistente para telemetria (muito mais rápido)
telemetry_session = requests.Session()

# ─── Configuração de Log Local ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('RoboCartas')

# ─── Variáveis de Ambiente ────────────────────────────────────────────────────
SUPABASE_URL              = os.environ.get('SUPABASE_URL', '').rstrip('/')
SUPABASE_SERVICE_ROLE_KEY = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')
EDGE_FUNCTION_URL         = f"{SUPABASE_URL}/functions/v1/fetch-charts"
TABLE_URL                 = f"{SUPABASE_URL}/rest/v1/charts_procedural"

# R2 Cloudflare (Telemetria)
R2_ACCESS_KEY_ID     = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT          = os.environ.get('R2_ENDPOINT')
R2_BUCKET            = "skyfpl-charts"

HEADERS_EDGE = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'apikey': SUPABASE_SERVICE_ROLE_KEY,
}
HEADERS_REST = {
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'apikey': SUPABASE_SERVICE_ROLE_KEY,
    'Prefer': 'resolution=merge-duplicates',
}
HEADERS_STORAGE = {
    'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
    'x-upsert': 'true',
    'Content-Type': 'application/json'
}

# ─── Gerenciamento de Telemetria ──────────────────────────────────────────────

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        log.error("❌ Credenciais R2 incompletas nos Secrets!")
        return None
    
    # Configuração de timeout ultra-agressiva para o S3
    config = Config(
        connect_timeout=15,
        read_timeout=30,
        retries={'max_attempts': 3}
    )
    
    return boto3.client(
        's3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto',
        config=config
    )

def upload_telemetry(snapshot):
    """Sobe o snapshot de telemetria para o Supabase Storage."""
    try:
        snapshot['updated_at'] = time.time()
        # Endereço do Bucket 'robots-telemetry' no Supabase
        url = f"{SUPABASE_URL}/storage/v1/object/robots-telemetry/procedural/telemetry.json"
        upload_url = f"{SUPABASE_URL}/storage/v1/object/robots-telemetry/procedural/telemetry.json"
        json_data = json.dumps(snapshot, ensure_ascii=False).encode('utf-8')
        
        telemetry_session.put(
            upload_url,
            data=json_data,
            headers=HEADERS_STORAGE,
            timeout=10
        )
    except Exception as e:
        log.error(f"Erro ao enviar telemetria: {e}")

def add_telemetry_log(telemetry, message):
    log.info(message)
    with telemetry_lock:
        telemetry['logs'].insert(0, f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        if len(telemetry['logs']) > 20:
            telemetry['logs'] = telemetry['logs'][:20]

telemetry_lock = threading.Lock()

# ─── Utilitários ─────────────────────────────────────────────────────────────

def sanitize_filename(text: str) -> str:
    """Limpa o nome da carta para um formato seguro de arquivo."""
    # Remove acentos e caracteres especiais, converte espaços para underscore
    text = text.replace(' & ', '_AND_').replace(' | ', '_OR_')
    text = re.sub(r'[^\w\s-]', '', text).strip()
    text = re.sub(r'[-\s]+', '_', text)
    return text.upper()

def mirror_pdf_to_r2(s3, icao: str, tipo: str, name: str, url: str, airac: str) -> tuple[str, int]:
    """Baixa o PDF do DECEA e sobe para o R2 no diretório do ciclo AIRAC."""
    if not url or not s3: return '', 0
    
    clean_name = sanitize_filename(name)
    r2_key = f"procedural/charts/{airac}/{icao}/{tipo}_{clean_name}.pdf"
    
    try:
        # Download do original (DECEA)
        # Polidez de rede: Pequeno delay aleatório
        import random
        time.sleep(random.uniform(0.3, 1.2))
        
        resp = requests.get(url, timeout=30, stream=True)
        if not resp.ok:
            log.error(f"[{icao}] Erro download PDF: {url} -> {resp.status_code}")
            return '', 0
        
        # Lendo em chunks para evitar travar em arquivos imensos
        content = b""
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk: content += chunk
        
        size = len(content)
        log.info(f"[{icao}] Download concluído: {name} ({size/1024:.1f} KB)")
        
        # Upload para o R2 com logs de auditoria
        log.info(f"   [R2] Enviando arquivo para CDN: {r2_key}")
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=content,
            ContentType='application/pdf'
        )
        log.info(f"   [R2] Sucesso no espelhamento: {r2_key}")
        url_r2 = f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{r2_key}"
        return url_r2, size
    except Exception as e:
        log.error(f"[{icao}] Falha no espelhamento {name}: {e}")
        return '', 0

# ─── Funções Principais ───────────────────────────────────────────────────────

def fetch_all_icao_codes() -> list[str]:
    R2_NAVDATA_URL = 'https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/latest_navdata.json'
    resp = requests.get(R2_NAVDATA_URL, timeout=30)
    resp.raise_for_status()
    payload = resp.json()
    points = payload.get('data', [])
    icao_set = set()
    for p in points:
        ptype = p.get('type', '')
        icao  = (p.get('icao') or '').strip().upper()
        if ptype in ('airport', 'heliport', 'ICA:airport', 'ICA:heliport') and len(icao) == 4:
            icao_set.add(icao)
    return sorted(icao_set)

def fetch_charts_for_icao(icao: str, retries=3) -> list[dict]:
    try:
        resp = requests.post(
            EDGE_FUNCTION_URL,
            json={'icaoCode': icao},
            headers=HEADERS_EDGE,
            timeout=30,
        )
        
        # Erro 429 (Rate Limit)
        if resp.status_code == 429:
            log.warning(f"[{icao}] Rate limit (429) — aguardando 60s...")
            time.sleep(60)
            return fetch_charts_for_icao(icao, retries)
            
        # Erro 503 (Boot Error/Overload) - Tentamos novamente com backoff
        if resp.status_code == 503 and retries > 0:
            wait_time = (4 - retries) * 5
            log.warning(f"[{icao}] Erro 503 (Boot) — Tentando novamente em {wait_time}s... ({retries} restantes)")
            time.sleep(wait_time)
            return fetch_charts_for_icao(icao, retries - 1)

        if not resp.ok:
            log.error(f"[{icao}] Erro API {resp.status_code}: {resp.text[:200]}")
            return []
            
        data = resp.json()
        return data.get('charts', []) if data.get('success') else []
        
    except Exception as e:
        if retries > 0:
            log.warning(f"[{icao}] Falha de conexão: {e}. Retentando... ({retries})")
            time.sleep(5)
            return fetch_charts_for_icao(icao, retries - 1)
        log.error(f"[{icao}] Falha crítica no fetch após retentativas: {e}")
        return []

def upsert_charts(s3, icao: str, charts: list[dict], airac: str, dry_run: bool, telemetry: dict) -> tuple[str, int]:
    if not charts or dry_run: return len(charts)
    records = []
    mirrored_count = 0
    
    for c in charts:
        carta_nome = c.get('nome', 'CARTA')
        add_telemetry_log(telemetry, f"☁️ [{icao}] Espelhando: {carta_nome} ({c.get('tipo', 'UNKN')})...")
        
        # Espelhamento REAL (Download DECEA -> Upload R2)
        url_r2, size = mirror_pdf_to_r2(s3, icao, c.get('tipo', 'UNKN'), carta_nome, c.get('link', ''), airac)
        
        if url_r2:
            with telemetry_lock:
                telemetry['mirrored_charts'] = telemetry.get('mirrored_charts', 0) + 1
                telemetry['mirrored_bytes'] = telemetry.get('mirrored_bytes', 0) + size
                
                # Log de progresso a cada 10 cartas
                current_count = telemetry['mirrored_charts']
                if current_count % 10 == 0:
                    log.info(f"   [PROGRESSO] {current_count} cartas já espelhadas no total...")
            mirrored_count += 1
        else:
            with telemetry_lock:
                telemetry['failed_airports'].append({
                    'icao': icao,
                    'error': f"Falha no download DECEA: {carta_nome}",
                    'at': datetime.now().strftime('%H:%M:%S')
                })
            
        records.append({
            'icao': icao,
            'tipo': c.get('tipo', 'UNKN'),
            'nome_procedimento': c.get('nome', 'Carta Sem Nome'),
            'url_decea': c.get('link', ''),
            'url_r2': url_r2,
            'airac_cycle': airac,
            'data_carta': c.get('dt', ''),
            'source': 'api'
        })
    try:
        upsert_url = f"{TABLE_URL}?on_conflict=icao,tipo,nome_procedimento"
        resp = requests.post(upsert_url, json=records, headers=HEADERS_REST, timeout=30)
        if not resp.ok:
            error_msg = f"[{icao}] Falha DB: {resp.status_code} - {resp.text[:80]}"
            log.error(error_msg)
            with telemetry_lock:
                add_telemetry_log(telemetry, error_msg)
                telemetry['failed_airports'].append({
                    'icao': icao,
                    'error': f"DB Erro {resp.status_code}",
                    'at': datetime.now().strftime('%H:%M:%S')
                })
        # Retorna a URL e o tamanho da última carta processada (ou vazio se falhar)
        if records:
            return records[0]['url_r2'], mirrored_count
        return '', 0
    except Exception as e:
        error_msg = f"[{icao}] Erro ao upsert: {e}"
        log.error(error_msg)
        with telemetry_lock:
            add_telemetry_log(telemetry, error_msg)
        return '', 0

def export_master_json(s3, airac_cycle: str):
    all_records = []
    page_size = 1000
    offset = 0
    while True:
        resp = requests.get(f"{TABLE_URL}?select=*&limit={page_size}&offset={offset}", headers=HEADERS_REST, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        if not data: break
        all_records.extend(data)
        offset += page_size
        if len(data) < page_size: break
    
    log.info(f"📊 Exportação: {len(all_records)} registros recuperados para o Master JSON.")
    payload = {
        'metadata': {'generated_at': datetime.now(timezone.utc).isoformat(), 'airac_cycle': airac_cycle, 'total_charts': len(all_records)},
        'data': all_records
    }
    
    content = json.dumps(payload, ensure_ascii=False, indent=2)
    filename = 'latest_proc_charts.json'
    
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
        
    # Upload Master JSON para a CDN R2 (Raiz, para compatibilidade com o App Tablet)
    if s3:
        try:
            log.info(f"📤 Subindo Master JSON para o R2 ({len(content)} bytes)...")
            s3.put_object(
                Bucket=R2_BUCKET,
                Key=filename,
                Body=content,
                ContentType='application/json'
            )
            return len(content.encode('utf-8'))
        except Exception as e:
            log.error(f"❌ Falha ao subir Master JSON: {e}")
            return 0
    return 0

# ─── Execução ─────────────────────────────────────────────────────────────────

def main():
    # ... (código de parsing permanece o mesmo até a criação do pool)
    parser = argparse.ArgumentParser()
    parser.add_argument('--icao', help='ICAO específico (ex: SBSV) ou lista CSV (ex: SBSV,SBGR)')
    parser.add_argument('--dry-run', default='False', help='Simulação (True/False)')
    parser.add_argument('--airac', help='Ciclo AIRAC (ex: 2404)')
    parser.add_argument('--workers', type=int, default=10, help='Número de threads paralelas')
    args = parser.parse_args()

    # Conversão de string para booleano (GitHub envia como string)
    dry_run = str(args.dry_run).lower() == 'true'

    s3 = init_s3()
    airac_cycle = args.airac or datetime.now(timezone.utc).strftime('%y%m')
    
    telemetry = {
        'status': 'initializing',
        'current_icao': '',
        'progress': 0,
        'total_airports': 0,
        'total_offered': 0,
        'total_charts': 0,
        'mirrored_charts': 0,
        'mirrored_bytes': 0,
        'logs': [],
        'failed_airports': [],
        'last_processed_charts': []
    }

    # Handler para interrupção graciosa (Cancelamento no GitHub)
    def handle_stop(signum, frame):
        log.warning("🛑 Sinal de interrupção recebido. Finalizando telemetria...")
        telemetry['status'] = 'stopped'
        add_telemetry_log(telemetry, "🛑 Robô interrompido pelo usuário.")
        upload_telemetry(telemetry)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)
    add_telemetry_log(telemetry, f"🤖 Robô Iniciado | Ciclo {airac_cycle} | DryRun: {dry_run}")
    upload_telemetry(telemetry)

    # Suporte a lista de ICAOs separados por vírgula (ex: SBGR,SBBR,SBSP)
    if args.icao:
        raw_codes = [c.strip().upper() for c in args.icao.split(',') if c.strip()]
        icao_list = [c for c in raw_codes if len(c) == 4]  # Valida formato ICAO
        if not icao_list:
            log.error(f"Nenhum ICAO válido encontrado em: '{args.icao}'. Formato esperado: SBGR ou SBGR,SBBR,SBSP")
            sys.exit(1)
        log.info(f"🎯 Modo Seletivo: {len(icao_list)} aeródromo(s) — {', '.join(icao_list)}")
    else:
        icao_list = fetch_all_icao_codes()
    telemetry['total_airports'] = len(icao_list)
    telemetry['status'] = 'in_progress'
    
    # Thread de Heartbeat: Sobe a telemetria periodicamente sem travar os workers
    stop_heartbeat = threading.Event()
    def heartbeat_loop():
        while not stop_heartbeat.is_set():
            upload_telemetry(telemetry)
            time.sleep(10)

    heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    # ESTRATÉGIA V10: Fila Única Global (Polidez de Rede)
    all_chart_tasks = []
    
    # 1. Coleta metadados de todos os aeródromos (Rápido)
    add_telemetry_log(telemetry, f"📡 Coletando lista de cartas para {len(icao_list)} aeródromo(s)...")
    for icao in icao_list:
        try:
            charts = fetch_charts_for_icao(icao)
            count = len(charts)
            telemetry['total_offered'] += count
            add_telemetry_log(telemetry, f"📡 {icao}: {count} cartas encontradas no AISWEB.")
            for chart in charts:
                all_chart_tasks.append((icao, chart))
        except Exception as e:
            add_telemetry_log(telemetry, f"❌ Erro ao buscar cartas de {icao}: {str(e)}")

    # 2. Processamento em Massa com Workers Globais
    total_to_process = len(all_chart_tasks)
    add_telemetry_log(telemetry, f"🚀 Iniciando processamento de {total_to_process} cartas com {args.workers} workers globais...")
    
    processed_count = 0
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Mapeia cada carta para uma tarefa de processamento individual
        def process_single_chart(item):
            icao_code, chart_data = item
            url_r2, size = upsert_charts(s3, icao_code, [chart_data], airac_cycle, dry_run, telemetry)
            
            # Adiciona à lista de auditoria (últimas 5)
            if url_r2:
                with telemetry_lock:
                    telemetry['last_processed_charts'].insert(0, {
                        'icao': icao_code,
                        'name': chart_data.get('nome_procedimento'),
                        'url': url_r2,
                        'at': datetime.now().strftime('%H:%M:%S')
                    })
                    if len(telemetry['last_processed_charts']) > 5:
                        telemetry['last_processed_charts'].pop()
            return 1 if url_r2 else 0

        # Envia para execução paralela
        results = list(executor.map(process_single_chart, all_chart_tasks))
        processed_count = sum(results)

    telemetry['progress'] = len(icao_list) 
    stop_heartbeat.set()
    
    add_telemetry_log(telemetry, "📦 Gerando Master JSON...")
    upload_telemetry(telemetry)
    if not dry_run: 
        file_size = export_master_json(s3, airac_cycle)
        telemetry['master_file_size'] = file_size
    
    telemetry['status'] = 'completed'
    add_telemetry_log(telemetry, f"✅ Operação concluída! Oferecidas: {telemetry.get('total_offered', 0)} | Processadas: {processed_count}")
    upload_telemetry(telemetry)
    
    sys.exit(0)

if __name__ == '__main__':
    main()
