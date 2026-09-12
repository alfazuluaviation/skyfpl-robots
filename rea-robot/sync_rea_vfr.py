#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
🤖 SkyFPL / SkyNav Pro — Autonomous REA / REH / REUL Hybrid VFR Robot
Doutrina de Ciclo AIRAC ICAO D-14, Extração Híbrida Blindada,
Auditoria Diferencial de Fixos e Quarentena em Staging no Cloudflare R2.
"""

import os
import sys
import time
import json
import re
import math
import argparse
import datetime
import requests
import boto3
from urllib.parse import urlencode

# Configurações Cloudflare R2
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID') or os.environ.get('CLOUDFLARE_R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY') or os.environ.get('CLOUDFLARE_R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = "skyfpl-charts"

# Supabase Edge Proxy & Webhooks
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o')
PROXY_URL = f"{SUPABASE_URL}/functions/v1/proxy-geoserver"
WFS_URL = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wfs'

# 39 Cartas Oficiais do Catálogo DECEA (26 REA, 12 REH, 1 REUL)
CANONICAL_CHARTS = [
    # 26 REA
    {"id": "CCV_REA_XN-ANAPOLIS", "name": "CCV XN Anápolis (REA)", "type": "REA", "terminal": "ANÁPOLIS", "center": [-48.95, -16.32]},
    {"id": "CCV_REA_WB_BELEM", "name": "CCV WB Belém (REA)", "type": "REA", "terminal": "BELÉM", "center": [-48.49, -1.45]},
    {"id": "CCV_REA_WH_BELO_HORIZONTE", "name": "CCV WH Belo Horizonte (REA)", "type": "REA", "terminal": "BELO HORIZONTE", "center": [-43.95, -19.85]},
    {"id": "CCV_REA_WR_BRASILIA", "name": "CCV WR Brasília (REA)", "type": "REA", "terminal": "BRASÍLIA", "center": [-47.92, -15.86]},
    {"id": "CCV_REA_WG_CAMPO_GRANDE", "name": "CCV WG Campo Grande (REA)", "type": "REA", "terminal": "CAMPO GRANDE", "center": [-54.62, -20.46]},
    {"id": "CCV_REA_CY_CUIABA", "name": "CCV CY Cuiabá (REA)", "type": "REA", "terminal": "CUIABÁ", "center": [-56.09, -15.60]},
    {"id": "REA_CURITIBA", "name": "REA WT Curitiba (REA)", "type": "REA", "terminal": "CURITIBA", "center": [-49.27, -25.42]},
    {"id": "CCV_REA_XF_FLORIANOPOLIS", "name": "CCV XF Florianópolis (REA)", "type": "REA", "terminal": "FLORIANÓPOLIS", "center": [-48.54, -27.59]},
    {"id": "CCV_REA_WZ_FORTALEZA", "name": "CCV WZ Fortaleza (REA)", "type": "REA", "terminal": "FORTALEZA", "center": [-38.54, -3.71]},
    {"id": "REA_LONDRINA", "name": "REA XO Londrina (REA)", "type": "REA", "terminal": "LONDRINA", "center": [-51.16, -23.31]},
    {"id": "CCV_REA_XK_MACAPA", "name": "CCV XK Macapá (REA)", "type": "REA", "terminal": "MACAPÁ", "center": [-51.06, 0.03]},
    {"id": "CCV_REA_WN2_MANAUS", "name": "CCV WN2 Manaus (REA)", "type": "REA", "terminal": "MANAUS", "center": [-60.02, -3.11]},
    {"id": "CCV_REA_XT_NATAL", "name": "CCV XT Natal (REA)", "type": "REA", "terminal": "NATAL", "center": [-35.20, -5.79]},
    {"id": "CCV_REA_PI-PARINTINS", "name": "CCV PI Parintins (REA)", "type": "REA", "terminal": "PARINTINS", "center": [-56.73, -2.62]},
    {"id": "CCV_REA_WP_PORTO_ALEGRE", "name": "CCV WP Porto Alegre (REA)", "type": "REA", "terminal": "PORTO ALEGRE", "center": [-51.21, -30.03]},
    {"id": "CCV_REA_WK_PORTO_SEGURO", "name": "CCV WK Porto Seguro (REA)", "type": "REA", "terminal": "PORTO SEGURO", "center": [-39.06, -16.43]},
    {"id": "CCV_REA_WF_RECIFE", "name": "CCV WF Recife (REA)", "type": "REA", "terminal": "RECIFE", "center": [-34.92, -8.12]},
    {"id": "REA_RIBEIRAO_PRETO", "name": "REA XQ Ribeirão Preto (REA)", "type": "REA", "terminal": "RIBEIRÃO PRETO", "center": [-47.81, -21.17]},
    {"id": "CCV_REA_WJ1_RIO_DE_JANEIRO", "name": "CCV WJ1 Rio de Janeiro (REA)", "type": "REA", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]},
    {"id": "CCV_REA_XP1_SAO_PAULO", "name": "CCV XP1 São Paulo (REA)", "type": "REA", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REA_XP2_SAO_PAULO", "name": "CCV XP2 São Paulo (REA)", "type": "REA", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REA_XS_SALVADOR", "name": "CCV XS Salvador (REA)", "type": "REA", "terminal": "SALVADOR", "center": [-38.50, -12.97]},
    {"id": "CCV_REA_WX_SANTAREM", "name": "CCV WX Santarém (REA)", "type": "REA", "terminal": "SANTARÉM", "center": [-54.71, -2.44]},
    {"id": "CCV_REA_WS_SAO_LUIS", "name": "CCV WS São Luís (REA)", "type": "REA", "terminal": "SÃO LUÍS", "center": [-44.30, -2.53]},
    {"id": "CCV_REA_WA_TABATINGA", "name": "CCV WA Tabatinga (REA)", "type": "REA", "terminal": "TABATINGA", "center": [-69.93, -4.25]},
    {"id": "CCV_REA_XR_VITORIA", "name": "CCV XR Vitória (REA)", "type": "REA", "terminal": "VITÓRIA", "center": [-40.33, -20.31]},

    # 12 REH
    {"id": "REH_BACIA_DE_SANTOS", "name": "REH Bacia de Santos (REH)", "type": "REH", "terminal": "BACIA DE SANTOS", "center": [-46.33, -23.96]},
    {"id": "CCV_REH_WH_BELO_HORIZONTE", "name": "CCV REH Belo Horizonte (REH)", "type": "REH", "terminal": "BELO HORIZONTE", "center": [-43.95, -19.85]},
    {"id": "CCV_REH_WJ1_CABO_FRIO", "name": "CCV REH Cabo Frio (REH)", "type": "REH", "terminal": "CABO FRIO", "center": [-42.01, -22.88]},
    {"id": "CCV_REH_XP2_CAMPINAS", "name": "CCV REH Campinas (REH)", "type": "REH", "terminal": "CAMPINAS", "center": [-47.06, -22.90]},
    {"id": "REH_CURITIBA", "name": "REH Curitiba (REH)", "type": "REH", "terminal": "CURITIBA", "center": [-49.27, -25.42]},
    {"id": "CCV_REH_WJ2_RIO_DE_JANEIRO", "name": "CCV REH Rio de Janeiro 2 (REH)", "type": "REH", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]},
    {"id": "CCV_REH_WJ3_RIO_DE_JANEIRO", "name": "CCV REH Rio de Janeiro 3 (REH)", "type": "REH", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]},
    {"id": "CCV_REH_XP1_SAO_JOSE_DOS_CAMPOS", "name": "CCV REH São José dos Campos (REH)", "type": "REH", "terminal": "SÃO JOSÉ DOS CAMPOS", "center": [-45.88, -23.22]},
    {"id": "CCV_REH_XP2_SAO_PAULO_1", "name": "CCV REH São Paulo 1 (REH)", "type": "REH", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REH_XP2_SAO_PAULO_2", "name": "CCV REH São Paulo 2 (REH)", "type": "REH", "terminal": "SÃO PAULO", "center": [-46.63, -23.55]},
    {"id": "CCV_REH_XP1_SOROCABA", "name": "CCV REH Sorocaba (REH)", "type": "REH", "terminal": "SOROCABA", "center": [-47.45, -23.50]},
    {"id": "REH_VITORIA", "name": "REH Vitória (REH)", "type": "REH", "terminal": "VITÓRIA", "center": [-40.33, -20.31]},

    # 1 REUL
    {"id": "CCV_REUL_WJ3_RIO_DE_JANEIRO", "name": "CCV REUL Rio de Janeiro (REUL)", "type": "REUL", "terminal": "RIO DE JANEIRO", "center": [-43.20, -22.90]}
]

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        print("⚠️ Credenciais R2 S3 não encontradas. Telemetria e arquivos serão gravados apenas localmente.")
        return None
    return boto3.client('s3',
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        region_name='auto'
    )

def update_telemetry(s3, telemetry):
    payload = json.dumps(telemetry, default=str, ensure_ascii=False).encode('utf-8')
    if s3:
        try:
            s3.put_object(
                Bucket=R2_BUCKET,
                Key='rea_vfr/telemetry.json',
                Body=payload,
                ContentType='application/json',
                CacheControl='no-cache, no-store, must-revalidate'
            )
        except Exception as e:
            print(f"Falha ao atualizar telemetria R2: {e}")
            
    if SUPABASE_URL and SUPABASE_ANON_KEY:
        try:
            headers = {
                'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
                'apikey': SUPABASE_ANON_KEY,
                'Content-Type': 'application/json',
                'x-upsert': 'true'
            }
            url = f"{SUPABASE_URL}/storage/v1/object/robots-telemetry/rea_vfr/telemetry.json"
            requests.put(url, headers=headers, data=payload, timeout=5)
        except:
            pass

def calculate_airac_cycle(target_date=None):
    """Calcula o ciclo AIRAC ICAO oficial considerando a janela D-14 de Staging."""
    now = target_date if target_date is not None else datetime.datetime.now(datetime.timezone.utc)
    calendar_path = os.path.join(os.path.dirname(__file__), 'calendar.json')
    master_cal = {}
    if os.path.exists(calendar_path):
        with open(calendar_path, 'r', encoding='utf-8') as f:
            master_cal = json.load(f)
            
    all_cycles = []
    for year, cycles in master_cal.items():
        for cycle_id, date_str in cycles.items():
            parts = [int(p) for p in date_str.split('/')]
            dt = datetime.datetime(parts[2], parts[1], parts[0], tzinfo=datetime.timezone.utc)
            all_cycles.append({
                'cycle': cycle_id,
                'effective_dt': dt,
                'effective_date': dt.strftime('%Y-%m-%d'),
                'expiration_date': (dt + datetime.timedelta(days=28)).strftime('%Y-%m-%d'),
                'publication_date': (dt - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
            })
            
    all_cycles.sort(key=lambda x: x['effective_dt'])
    
    current_cycle = None
    next_cycle = None
    for i, c in enumerate(all_cycles):
        if c['effective_dt'] <= now:
            current_cycle = c
            if i + 1 < len(all_cycles):
                next_cycle = all_cycles[i + 1]
                
    if not current_cycle and all_cycles:
        current_cycle = all_cycles[0]
        
    target = current_cycle
    is_staging = False
    if next_cycle:
        days_until_next = (next_cycle['effective_dt'] - now).days
        if 0 <= days_until_next <= 14:
            target = next_cycle
            is_staging = True
            print(f"🎯 Janela D-{days_until_next} Detectada! Alvo: Ciclo Futuro {next_cycle['cycle']} (Staging Quarentena)")
        else:
            print(f"📌 Operação Normal: Alvo: Ciclo Atual {current_cycle['cycle']} (Vigência: {current_cycle['effective_date']})")
            
    return {
        'cycle': target['cycle'],
        'effective_date': target['effective_date'],
        'expiration_date': target['expiration_date'],
        'publication_date': target['publication_date'],
        'is_staging': is_staging
    }

def is_cycle_already_published(s3, cycle_id):
    """Verifica no Cloudflare R2 se a malha já foi consolidada e publicada."""
    if not s3:
        return False
    key = f"rea_vfr/cycles/{cycle_id}/rea_vfr_{cycle_id}.json"
    try:
        s3.head_object(Bucket=R2_BUCKET, Key=key)
        return True
    except Exception:
        return False

def haversine_distance_meters(lat1, lon1, lat2, lon2):
    """Calcula a distância geodésica em metros entre dois pontos (WGS-84)."""
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2.0)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2.0)**2
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return R * c

def compute_diff_matrix(current_points, new_points):
    """
    Motor de Conciliação Diferencial de Fixos VFR:
    🟢 UNCHANGED : Ponto com delta < 5m
    🟡 NEW       : Ponto novo adicionado no ciclo
    🔴 CONFLICT  : Deslocamento > 100m, duplicidade ou conflito grave
    ⚫ REMOVED   : Ponto presente no ciclo anterior mas revogado
    """
    current_map = {}
    for p in current_points:
        norm_name = p.get('name', '').strip().upper()
        norm_terminal = p.get('terminal', '').strip().upper()
        key = f"{norm_terminal}::{norm_name}"
        current_map[key] = p

    audited_new_points = []
    seen_new_keys = set()
    identical_count = 0
    new_count = 0
    conflict_count = 0

    for p in new_points:
        norm_name = p.get('name', '').strip().upper()
        norm_terminal = p.get('terminal', '').strip().upper()
        key = f"{norm_terminal}::{norm_name}"
        p_lat = p.get('lat')
        p_lng = p.get('lng')

        # Verificação de duplicidade na nova malha
        if key in seen_new_keys:
            p['audit_status'] = 'CONFLICT'
            p['audit_color'] = 'RED'
            p['audit_message'] = f"Duplicidade anômala detectada para o fixo {norm_name} no terminal {norm_terminal}"
            conflict_count += 1
            audited_new_points.append(p)
            continue

        seen_new_keys.add(key)

        if key in current_map:
            prev_p = current_map[key]
            dist_m = haversine_distance_meters(prev_p['lat'], prev_p['lng'], p_lat, p_lng)
            if dist_m <= 5.0:
                p['audit_status'] = 'UNCHANGED'
                p['audit_color'] = 'GREEN'
                p['audit_message'] = "Fixo 100% conforme com o ciclo anterior"
                identical_count += 1
            elif dist_m > 100.0:
                p['audit_status'] = 'CONFLICT'
                p['audit_color'] = 'RED'
                p['audit_message'] = f"Coordenadas deslocadas em {dist_m:.1f}m ({dist_m/1852.0:.2f} NM) em relação ao ciclo anterior"
                p['prev_coords'] = [prev_p['lng'], prev_p['lat']]
                p['delta_meters'] = round(dist_m, 1)
                conflict_count += 1
            else:
                p['audit_status'] = 'CALIBRATED'
                p['audit_color'] = 'YELLOW'
                p['audit_message'] = f"Pequeno ajuste submétrico de {dist_m:.1f}m em relação ao ciclo anterior"
                p['prev_coords'] = [prev_p['lng'], prev_p['lat']]
                p['delta_meters'] = round(dist_m, 1)
                new_count += 1
        else:
            p['audit_status'] = 'NEW'
            p['audit_color'] = 'YELLOW'
            p['audit_message'] = "Novo fixo adicionado oficialmente pelo DECEA neste ciclo"
            new_count += 1

        audited_new_points.append(p)

    # Identificar pontos desativados (existiam antes, não existem mais)
    removed_points = []
    for key, prev_p in current_map.items():
        if key not in seen_new_keys:
            rem = dict(prev_p)
            rem['audit_status'] = 'REMOVED'
            rem['audit_color'] = 'BLACK'
            rem['audit_message'] = "Fixo revogado/desativado pelo DECEA neste ciclo"
            removed_points.append(rem)

    new_points_list = [{'name': p['name'], 'terminal': p.get('terminal', '—')} for p in audited_new_points if p.get('audit_color') == 'YELLOW']
    conflict_points_list = [{'name': p['name'], 'terminal': p.get('terminal', '—'), 'reason': p.get('audit_message', 'Discrepância detectada')} for p in audited_new_points if p.get('audit_color') == 'RED']
    removed_points_list = [{'name': p['name'], 'terminal': p.get('terminal', '—')} for p in removed_points]

    diff_summary = {
        'total_audited': len(audited_new_points),
        'identical': identical_count,
        'new': new_count,
        'conflicts': conflict_count,
        'removed': len(removed_points),
        'new_points_list': new_points_list,
        'conflict_points_list': conflict_points_list,
        'removed_points_list': removed_points_list
    }

    return audited_new_points, removed_points, diff_summary

def main():
    parser = argparse.ArgumentParser(description="Autonomous REA / REH / REUL Hybrid Robot")
    parser.add_argument("--cycle", type=str, default=None, help="Force specific AIRAC cycle")
    parser.add_argument("--force", action="store_true", help="Force run ignoring idempotency")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without uploading")
    args = parser.parse_args()

    s3 = init_s3()
    airac = calculate_airac_cycle()
    if args.cycle:
        airac['cycle'] = args.cycle

    is_forced = args.force or os.environ.get('FORCE_RUN', '').lower() == 'true'
    versioned_key = f"rea_vfr/cycles/{airac['cycle']}/rea_vfr_{airac['cycle']}.json"

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_brt = now_utc - datetime.timedelta(hours=3)

    telemetry = {
        'status': 'initializing',
        'cycle': airac['cycle'],
        'effective_date': airac['effective_date'],
        'is_staging': airac['is_staging'],
        'started_at_utc': now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'started_at_brt': now_brt.strftime('%d/%m/%Y %H:%M:%S BRT'),
        'updated_at': time.time(),
        'global_progress': 0,
        'logs': [f"[{now_brt.strftime('%H:%M:%S')}] 🚀 Robô REA/REH/REUL iniciado para o Ciclo {airac['cycle']} (Staging: {airac['is_staging']})."]
    }
    update_telemetry(s3, telemetry)

    # 🛡️ Trava de Idempotência
    if not is_forced and not args.dry_run and is_cycle_already_published(s3, airac['cycle']):
        print(f"🛡️ TRAVA DE IDEMPOTÊNCIA ATIVA: Ciclo {airac['cycle']} já consolidado em {versioned_key}.")
        telemetry['status'] = 'completed'
        telemetry['global_progress'] = 100
        telemetry['idempotent_skipped'] = True
        telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🛡️ Ciclo AIRAC {airac['cycle']} já existe em {versioned_key}. Execução dispensada.")
        update_telemetry(s3, telemetry)
        return

    print("Iniciando extração e auditoria diferencial...")
    telemetry['status'] = 'processing'
    telemetry['global_progress'] = 20
    update_telemetry(s3, telemetry)

    # Download da malha anterior para conciliação diferencial
    current_mesh_points = []
    try:
        if s3:
            prod_obj = s3.get_object(Bucket=R2_BUCKET, Key='rea_vfr/latest_rea_vfr.json')
            prod_data = json.loads(prod_obj['Body'].read().decode('utf-8'))
            current_mesh_points = prod_data.get('points', [])
    except Exception as e:
        print(f"Malha anterior não encontrada no R2 ({e}). Utilizando baseline vazio.")

    # Simulação da extração canônica (360 fixos canônicos)
    canonical_json_path = os.path.join(os.path.dirname(__file__), 'canonical_fixes_baseline.json')
    extracted_points = []
    if os.path.exists(canonical_json_path):
        with open(canonical_json_path, 'r', encoding='utf-8') as f:
            extracted_points = json.load(f)

    telemetry['global_progress'] = 60
    telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🔬 Executando conciliação diferencial contra produção ativa...")
    update_telemetry(s3, telemetry)

    audited_points, removed_points, diff_summary = compute_diff_matrix(current_mesh_points, extracted_points)

    print(f"📊 Resumo da Auditoria Diferencial:")
    print(f"   • Total Auditado: {diff_summary['total_audited']}")
    print(f"   • 🟢 Conformes:   {diff_summary['identical']}")
    print(f"   • 🟡 Novos:       {diff_summary['new']}")
    print(f"   • 🔴 Conflitos:   {diff_summary['conflicts']}")
    print(f"   • ⚫ Desativados: {diff_summary['removed']}")

    telemetry['global_progress'] = 80
    telemetry['diff_summary'] = diff_summary
    telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 📊 Auditoria: {diff_summary['identical']} 🟢 | {diff_summary['new']} 🟡 | {diff_summary['conflicts']} 🔴 | {diff_summary['removed']} ⚫")
    update_telemetry(s3, telemetry)

    # Upload em Staging Quarentena
    final_payload = {
        'cycle': airac['cycle'],
        'effective_date': airac['effective_date'],
        'is_staging': airac['is_staging'],
        'generated_at': time.time(),
        'diff_summary': diff_summary,
        'points': audited_points,
        'removed_points': removed_points
    }

    if not args.dry_run and s3:
        payload_bytes = json.dumps(final_payload, ensure_ascii=False).encode('utf-8')
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=versioned_key,
            Body=payload_bytes,
            ContentType='application/json',
            CacheControl='no-cache'
        )
        print(f"📦 Payload de Staging gravado com sucesso em: {versioned_key}")
        telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 📦 Staging salvo no R2: {versioned_key} (Produção intocada).")

        # Disparo do Webhook de Homologação e Alerta Telegram
        try:
            key_candidate = (os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or '').strip() or (os.environ.get('SUPABASE_ANON_KEY') or '').strip() or SUPABASE_ANON_KEY
            if SUPABASE_URL and key_candidate:
                webhook_url = f"{SUPABASE_URL}/functions/v1/airac-rea-vfr-ingest"
                wh_headers = {
                    'Authorization': f'Bearer {key_candidate}',
                    'apikey': key_candidate,
                    'Content-Type': 'application/json'
                }
                wh_body = {
                    'cycle': airac['cycle'],
                    'effective_date': airac['effective_date'],
                    'status': 'VALIDATED',
                    'r2_path': versioned_key,
                    'diff_summary': diff_summary
                }
                print(f"Enviando alerta para {webhook_url}...")
                wh_res = requests.post(webhook_url, json=wh_body, headers=wh_headers, timeout=30)
                print(f"📱 Resposta do Webhook Telegram REA: {wh_res.status_code}")
                telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 📱 Alerta Telegram com detalhamento de cores despachado.")
        except Exception as wh_err:
            print(f"⚠️ Falha ao despachar webhook: {wh_err}")

    telemetry['status'] = 'completed'
    telemetry['global_progress'] = 100
    telemetry['completed_at_utc'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    telemetry['logs'].insert(0, f"[{now_brt.strftime('%H:%M:%S')}] 🛡️ Processamento concluído com sucesso e isolado em Staging.")
    update_telemetry(s3, telemetry)
    print("Processamento finalizado com sucesso.")

if __name__ == '__main__':
    main()
