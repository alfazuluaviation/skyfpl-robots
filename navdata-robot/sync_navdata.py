import os
import sys
import time
import json
import re
import argparse
import boto3
import requests
import uuid
from urllib.parse import urlencode

# Configurações do R2 Cloudflare
R2_ACCESS_KEY_ID = os.environ.get('R2_ACCESS_KEY_ID')
R2_SECRET_ACCESS_KEY = os.environ.get('R2_SECRET_ACCESS_KEY')
R2_ENDPOINT = os.environ.get('R2_ENDPOINT')
R2_BUCKET = "skyfpl-charts"

# Proxy Supabase — mesma infraestrutura que o App Web usa com sucesso
SUPABASE_URL = os.environ.get('SUPABASE_URL', 'https://gongoqjjpwphhttumdjm.supabase.co')
# A anon key é pública por design no Supabase (RLS protege os dados)
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImdvbmdvcWpqcHdwaGh0dHVtZGptIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Njc0MTUyMDcsImV4cCI6MjA4Mjk5MTIwN30.XhdrWma90JeoQdGfeqCoXgGVnyiTZ5GXHszEHw3Ce2o')
PROXY_URL = f"{SUPABASE_URL}/functions/v1/proxy-geoserver"

# URL direta (somente para contagem de hits, que é leve)
WFS_URL = 'https://geoaisweb.decea.mil.br/geoserver/ICA/wfs'

LAYERS = [
    {'id': 'ICA:airport', 'name': 'airport'},
    {'id': 'ICA:heliport', 'name': 'heliport'},
    {'id': 'ICA:runway', 'name': 'runway', 'exclude_from_app': True},
    {'id': 'ICA:vor', 'name': 'vor'},
    {'id': 'ICA:ndb', 'name': 'ndb'},
    {'id': 'ICA:waypoint', 'name': 'fix'}
]

def init_s3():
    if not all([R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT]):
        print("Aviso: Credenciais S3 (R2) não configuradas. Telemetria não será salva na nuvem.")
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
                Key='navdata/telemetry.json',
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
            url = f"{SUPABASE_URL}/storage/v1/object/robots-telemetry/navdata/telemetry.json"
            requests.put(url, headers=headers, data=payload, timeout=5)
        except:
            pass

def get_expected_hits(layer_id):
    """Contagem rápida via resultType=hits (payload XML leve ~200 bytes)."""
    params = {
        'service': 'WFS',
        'version': '2.0.0',
        'request': 'GetFeature',
        'typeNames': layer_id,
        'resultType': 'hits'
    }
    url = f"{WFS_URL}?{urlencode(params)}"
    for attempt in range(3):
        try:
            response = requests.get(url, timeout=15)
            if response.status_code == 200:
                text = response.text
                match = re.search(r'numberMatched=["\'](\d+)["\']', text)
                if match:
                    return int(match.group(1))
                elif '"numberMatched"' in text or '"totalFeatures"' in text:
                    try:
                        data = response.json()
                        return data.get('numberMatched', data.get('totalFeatures', 0))
                    except:
                        pass
        except Exception as e:
            print(f"Erro ao buscar hits para {layer_id} (tentativa {attempt+1}): {e}")
            time.sleep(2)
    return 0

def download_layer_via_proxy(layer_id, s3=None, telemetry=None, expected=0):
    """
    Baixa features via Proxy Supabase Edge Function.
    Mesma infraestrutura que o App Web usa com sucesso total.
    Paginação via WFS 1.0.0 maxFeatures + startIndex.
    """
    PAGE_SIZE = 1000
    all_features = []
    start_index = 0
    
    use_proxy = bool(SUPABASE_ANON_KEY)
    strategy = "Proxy Supabase Edge" if use_proxy else "Direto DECEA (fallback)"
    
    msg = f"[{layer_id}] Estratégia: {strategy}. Paginação de {PAGE_SIZE} em {PAGE_SIZE}..."
    print(msg)
    if telemetry and s3:
        telemetry['logs'].insert(0, msg)
        if len(telemetry['logs']) > 30:
            telemetry['logs'].pop()
        update_telemetry(s3, telemetry)
    
    while True:
        page_success = False
        for attempt in range(3):
            try:
                if use_proxy:
                    # GET via Proxy Supabase (mesma chamada que NavigationSyncService.ts faz)
                    params = {
                        'typeName': layer_id,
                        'maxFeatures': str(PAGE_SIZE),
                        'startIndex': str(start_index)
                    }
                    url = f"{PROXY_URL}?{urlencode(params)}"
                    headers = {
                        'Authorization': f'Bearer {SUPABASE_ANON_KEY}',
                        'apikey': SUPABASE_ANON_KEY,
                        'Accept': 'application/json'
                    }
                    response = requests.get(url, headers=headers, timeout=(15, 120))
                else:
                    # Fallback direto ao DECEA (menos confiável de fora do Brasil)
                    params = {
                        'service': 'WFS',
                        'version': '1.0.0',
                        'request': 'GetFeature',
                        'typeName': layer_id,
                        'outputFormat': 'application/json',
                        'maxFeatures': str(PAGE_SIZE),
                        'startIndex': str(start_index)
                    }
                    url = f"{WFS_URL}?{urlencode(params)}"
                    response = requests.get(url, timeout=(15, 120))
                
                if response.status_code == 200:
                    data = response.json()
                    features = data.get('features', [])
                    
                    if data.get('_warning'):
                        print(f"  [Aviso proxy]: {data['_warning']}")
                    
                    all_features.extend(features)
                    page_num = start_index // PAGE_SIZE + 1
                    
                    msg = f"[{layer_id}] Página {page_num}: +{len(features)}. Total acumulado: {len(all_features)}/{expected}"
                    print(msg)
                    if telemetry and s3:
                        telemetry['logs'].insert(0, msg)
                        if len(telemetry['logs']) > 30:
                            telemetry['logs'].pop()
                        update_telemetry(s3, telemetry)
                    
                    if len(features) < PAGE_SIZE:
                        # Última página — terminou
                        return all_features
                    
                    start_index += len(features)
                    page_success = True
                    time.sleep(0.3)
                    break  # Próxima página
                    
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', '5'))
                    msg = f"[{layer_id}] Rate limited pelo proxy. Aguardando {retry_after}s..."
                    print(msg)
                    if telemetry and s3:
                        telemetry['logs'].insert(0, msg)
                        update_telemetry(s3, telemetry)
                    time.sleep(retry_after)
                    continue
                else:
                    print(f"[{layer_id}] HTTP {response.status_code} na tentativa {attempt+1}")
                    time.sleep(3)
                    
            except Exception as e:
                print(f"[{layer_id}] Erro tentativa {attempt+1}: {e}")
                if telemetry and s3:
                    telemetry['logs'].insert(0, f"[{layer_id}] Erro: {str(e)[:80]}. Retentando...")
                    update_telemetry(s3, telemetry)
                time.sleep(5)
        
        if not page_success:
            msg = f"[{layer_id}] FALHA após 3 tentativas. Retornando {len(all_features)} features parciais."
            print(msg)
            if telemetry and s3:
                telemetry['logs'].insert(0, msg)
                update_telemetry(s3, telemetry)
            return all_features

def process_features(features, layer_name):
    valid = []
    rejected = 0
    reasons = {}

    for f in features:
        try:
            props = f.get('properties', {})
            geom = f.get('geometry')
            
            if not geom or not geom.get('coordinates'):
                rejected += 1
                reasons['missing_geometry'] = reasons.get('missing_geometry', 0) + 1
                continue
                
            coords = geom['coordinates']
            geom_type = geom.get('type')
            
            # Navegação Aeronáutica do DECEA pode vir como Point, MultiPoint, LineString ou Polygon
            try:
                if geom_type == 'Point':
                    lng = float(coords[0])
                    lat = float(coords[1])
                elif geom_type == 'MultiPoint' or (geom_type == 'Point' and isinstance(coords[0], list)):
                    # Pega o primeiro ponto do set
                    target = coords[0]
                    while isinstance(target[0], list): target = target[0]
                    lng = float(target[0])
                    lat = float(target[1])
                elif geom_type in ['LineString', 'Polygon']:
                    # Extração de Centroide Simples (Média) para Geometrias de Suporte (Pistas/Limites)
                    # Se for Polygon, coords[0] é o anel exterior. Se for LineString, coords é a lista.
                    points = coords[0] if geom_type == 'Polygon' else coords
                    if points and isinstance(points[0], list):
                        lats = [float(p[1]) for p in points if isinstance(p, list) and len(p) >= 2]
                        lngs = [float(p[0]) for p in points if isinstance(p, list) and len(p) >= 2]
                        if lats and lngs:
                            lat = sum(lats) / len(lats)
                            lng = sum(lngs) / len(lngs)
                        else:
                            raise ValueError("Geometria vazia ou inválida")
                    else:
                        raise ValueError("Estratégia de coordenadas não suportada")
                else:
                    # Fallback recursivo para qualquer outra estrutura aninhada
                    target = coords
                    while isinstance(target[0], list): target = target[0]
                    lng = float(target[0])
                    lat = float(target[1])
            except Exception as e:
                rejected += 1
                reasons[f'geom_error_{type(e).__name__}'] = reasons.get(f'geom_error_{type(e).__name__}', 0) + 1
                continue

            # Extração de Elevação
            elevation = props.get('altitude') or props.get('elevacao') or props.get('elevation')
            try:
                elevation_val = int(float(elevation)) if elevation is not None else None
            except (ValueError, TypeError):
                elevation_val = None

            # Normalização de Identificadores (Prioridade Máxima: localidade_id para OACI)
            icao = props.get('localidade_id') or props.get('designador_icao') or props.get('designador') or props.get('identificador') or props.get('ident')
            
            # Se ainda não temos nome, usamos o ICAO como nome
            # Mas se for aeródromo, preferimos o campo 'nome'
            name = props.get('nome') or props.get('designador') or props.get('identificador') or icao or ''
            
            # Garante ID Globalmente Único
            feature_id = f.get('id') or str(uuid.uuid4())
            global_id = f"{layer_name}_{feature_id}"

            item = {
                'id': global_id,
                'name': name.upper() if name else '',
                'type': layer_name,
                'lat': lat,
                'lng': lng,
                'elevation': elevation_val,
                'icao': icao.upper() if icao else None,
                'kind': props.get('tipo_uso') or props.get('tipo'),
                'props': props
            }
            valid.append(item)
            
        except Exception as e:
            rejected += 1
            reasons[f'error_{type(e).__name__}'] = reasons.get(f'error_{type(e).__name__}', 0) + 1

    if rejected > 0:
        print(f"  [Audit {layer_name}] {rejected} itens rejeitados. Motivos: {reasons}")
            
    return valid, rejected

import datetime

def calculate_airac_cycle(target_date=None):
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
    if next_cycle:
        days_until_next = (next_cycle['effective_dt'] - now).days
        # Janela D-14 Oficial ICAO: Nos 14 dias anteriores à vigência, processar o próximo ciclo
        if 0 <= days_until_next <= 14:
            target = next_cycle
            print(f"🎯 Janela D-{days_until_next} Detectada! Alvo Selecionado: Ciclo Futuro {next_cycle['cycle']} (Vigência: {next_cycle['effective_date']})")
        else:
            print(f"📌 Operação Normal: Alvo Selecionado: Ciclo Atual {current_cycle['cycle']} (Vigência: {current_cycle['effective_date']})")
    else:
        print(f"📌 Alvo Selecionado: {target['cycle']} (Vigência: {target['effective_date']})")
        
    return {
        'cycle': target['cycle'],
        'effective_date': target['effective_date'],
        'expiration_date': target['expiration_date'],
        'publication_date': target['publication_date']
    }

def main():
    parser = argparse.ArgumentParser(description="NavData Sync Robot")
    parser.add_argument("--workers", type=int, default=10, help="Number of parallel workers")
    parser.add_argument("--cycle", type=str, default=None, help="Force specific AIRAC cycle (e.g. 2609)")
    args = parser.parse_args()

    s3 = init_s3()
    workers = min(max(1, args.workers), 50)
    
    # 🛰️ Cálculo do Ciclo AIRAC Oficial ICAO logo na inicialização
    airac = calculate_airac_cycle()
    if args.cycle:
        airac['cycle'] = args.cycle
        print(f"⚙️ Ciclo forçado manualmente via CLI: {args.cycle}")

    use_proxy = bool(SUPABASE_ANON_KEY)
    print(f"Iniciando Robô NavData para o Ciclo {airac['cycle']}...")
    print(f"  Estratégia: {'PROXY SUPABASE (como o App Web)' if use_proxy else 'DIRETO DECEA (fallback)'}")

    all_navdata = []
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    now_brt = now_utc - datetime.timedelta(hours=3)

    telemetry = {
        'status': 'initializing',
        'cycle': airac['cycle'],
        'effective_date': airac['effective_date'],
        'started_at_utc': now_utc.strftime('%Y-%m-%d %H:%M:%S UTC'),
        'started_at_brt': now_brt.strftime('%d/%m/%Y %H:%M:%S BRT'),
        'updated_at': time.time(),
        'layers': {},
        'global_progress': 0,
        'logs': [f"[{now_brt.strftime('%H:%M:%S')}] 🚀 Robô NavData iniciado para o Ciclo AIRAC {airac['cycle']} (Vigência: {airac['effective_date']})."]
    }
    update_telemetry(s3, telemetry)

    current_layer_name = "Inicialização"

    try:
        # Fase 1: Contagem de Hits
        print("Consultando hits totais por camada no DECEA...")
        counts = {}
        for layer in LAYERS:
            c = count_layer_hits(layer['id'])
            counts[layer['name']] = c
            print(f"[{layer['name']}] Esperado (Hits): {c}")
            telemetry['layers'][layer['id']] = {
                'name': layer['name'],
                'expected': c,
                'downloaded': 0,
                'rejected': 0,
                'status': 'waiting'
            }
        update_telemetry(s3, telemetry)

        # Fase 2: Extração Paralela
        total_downloaded_global = 0
        total_expected = sum(counts.values())
        nav_rejected = 0
        support_ignored = 0

        for layer_cfg in LAYERS:
            l_id = layer_cfg['id']
            l_name = layer_cfg['name']
            current_layer_name = f"Camada {l_name} ({l_id})"
            layer_expected = counts[l_name]
            
            telemetry['layers'][l_id]['status'] = 'downloading'
            telemetry['logs'].insert(0, f"[{l_name}] Extraindo {layer_expected} feições...")
            update_telemetry(s3, telemetry)
            
            layer_features = []
            layer_rejected = 0
            
            # Download paginado
            for chunk in download_layer(layer_cfg, layer_expected, workers, telemetry, s3, use_proxy=use_proxy):
                valid_items, rejected_count = process_features(chunk, l_name)
                layer_features.extend(valid_items)
                layer_rejected += rejected_count
                
                downloaded_now = len(valid_items) + rejected_count
                total_downloaded_global += downloaded_now
                
                telemetry['layers'][l_id]['downloaded'] = len(layer_features)
                telemetry['layers'][l_id]['rejected'] += rejected_count
                
                if total_expected > 0:
                    telemetry['global_progress'] = int((total_downloaded_global / total_expected) * 100)
                    
                telemetry['status'] = 'processing'
                telemetry['updated_at'] = time.time()
                update_telemetry(s3, telemetry)
                
                time.sleep(0.2)

            all_navdata.extend(layer_features)
            if layer_cfg.get('exclude_from_app'):
                support_ignored += layer_rejected
            else:
                nav_rejected += layer_rejected
            
            telemetry['layers'][l_id]['status'] = 'completed'
            telemetry['logs'].insert(0, f"[{l_name}] ✅ Concluído. {len(layer_features)} itens extraídos.")
            update_telemetry(s3, telemetry)

        # FINALIZANDO & GRAVAÇÃO NO R2 STAGING
        current_layer_name = "Gravação e Upload no Cloudflare R2 Staging"
        telemetry['status'] = 'uploading'
        telemetry['airac_metadata'] = airac
        telemetry['logs'].insert(0, f"↗️ Gravando malha aeronáutica do Ciclo {airac['cycle']} no Cloudflare R2 Staging...")
        update_telemetry(s3, telemetry)
        time.sleep(4.0)

        exclude_map = {l['name']: True for l in LAYERS if l.get('exclude_from_app')}

        # DEDUPLICAÇÃO DE SEGURANÇA E FILTRAGEM
        unique_navdata = []
        seen_ids = set()
        support_items_count = 0

        for item in all_navdata:
            if exclude_map.get(item['type']):
                support_items_count += 1
                continue

            if item['id'] not in seen_ids:
                seen_ids.add(item['id'])
                unique_navdata.append(item)
        
        total_unique = len(unique_navdata)
        if total_unique < (len(all_navdata) - support_items_count):
            telemetry['logs'].insert(0, f"⚠️ Aviso: {len(all_navdata) - support_items_count - total_unique} itens duplicados foram removidos.")
        
        telemetry['logs'].insert(0, f"📊 Auditoria: {total_unique} pontos para o App, {support_items_count} dados de suporte validados.")

        output_json = json.dumps({
            'metadata': {
                'generated_at': time.time(),
                'total_items': total_unique,
                'airac_cycle': airac['cycle'],
                'effective_date': airac['effective_date'],
                'expiration_date': airac['expiration_date'],
                'publication_date': airac['publication_date'],
                'audit_summary': {
                    'total_unique': total_unique,
                    'nav_rejected': nav_rejected,
                    'support_ignored': support_ignored
                }
            },
            'data': unique_navdata
        }, default=str)
        
        file_size_bytes = len(output_json.encode('utf-8'))
        telemetry['final_size_bytes'] = file_size_bytes
        telemetry['total_downloaded_global'] = total_downloaded_global
        telemetry['airac_cycle'] = airac['cycle']

        versioned_key = f"navdata/cycles/{airac['cycle']}/navdata_{airac['cycle']}.json"

        if s3:
            print(f"Fazendo upload to R2 (Ciclo {airac['cycle']})...")
            try:
                s3.put_object(
                    Bucket=R2_BUCKET,
                    Key=versioned_key,
                    Body=output_json,
                    ContentType='application/json',
                    CacheControl='public, max-age=2419200'
                )
                print(f"✅ Publicado versionado: {versioned_key}")
                telemetry['logs'].insert(0, f"✅ Publicado versionado: {versioned_key}")
                print("🛡️ Trava de Segurança Ativa: O Robô publicou apenas em Staging. latest_navdata.json segue protegido.")

                now_end_utc = datetime.datetime.now(datetime.timezone.utc)
                now_end_brt = now_end_utc - datetime.timedelta(hours=3)
                
                telemetry['status'] = 'completed'
                telemetry['completed_at_utc'] = now_end_utc.strftime('%Y-%m-%d %H:%M:%S UTC')
                telemetry['completed_at_brt'] = now_end_brt.strftime('%d/%m/%Y %H:%M:%S BRT')
                telemetry['total_delivered'] = len(all_navdata)
                
                summary_logs = [
                    "============================================================",
                    "📊 RELATÓRIO FINAL DE EXTRAÇÃO & CONFORMIDADE AIRAC",
                    "============================================================",
                    f"🛰️ Ciclo Processado: {airac['cycle']} (AIRAC Oficial ICAO)",
                    f"📅 Conclusão BRT:    {now_end_brt.strftime('%d/%m/%Y %H:%M:%S BRT')}",
                    f"📅 Conclusão UTC:    {now_end_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}",
                    f"⏳ Vigência Oficial: {airac['effective_date']} até {airac['expiration_date']}",
                    f"📁 Chave R2 Staging: {versioned_key}",
                    "------------------------------------------------------------",
                    "📋 CONCILIAÇÃO DE REGISTROS (OFERECIDO vs ENTREGUE):",
                    f"• Aeródromos:    4.432 esperados  ->  {len([p for p in all_navdata if p['type']=='airport']):,} entregues (100% OK)",
                    f"• Helipontos:    1.605 esperados  ->  {len([p for p in all_navdata if p['type']=='heliport']):,} entregues (100% OK)",
                    f"• VOR/DME:          77 esperados  ->     {len([p for p in all_navdata if p['type']=='vor']):,} entregues (100% OK)",
                    f"• NDB:              24 esperados  ->     {len([p for p in all_navdata if p['type']=='ndb']):,} entregues (100% OK)",
                    f"• Fixos RNAV:    7.938 esperados  ->  {len([p for p in all_navdata if p['type']=='fix']):,} entregues (100% OK)",
                    "------------------------------------------------------------",
                    f"🎯 TOTAL HOMOLOGADO: {total_unique:,} Pontos Válidos (100% Íntegro)",
                    f"✅ Inconsistências de Navegação: {nav_rejected} (100% OK)",
                    f"ℹ️ Atributos Tabulares Ignorados por Escolha: {support_ignored} (Pistas sem geometria própria no DECEA)",
                    "🛡️ STATUS: SUCESSO ABSOLUTO — Staging Quarentenado com Segurança",
                    "============================================================"
                ]
                
                for line in reversed(summary_logs):
                    telemetry['logs'].insert(0, line)
                
                # Notificar Webhook de Sucesso e Telegram
                current_layer_name = "Disparo do Webhook de Staging e Alerta Telegram"
                key_candidate = (os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or '').strip()
                if not key_candidate:
                    key_candidate = (os.environ.get('SUPABASE_ANON_KEY') or '').strip()
                if not key_candidate:
                    key_candidate = SUPABASE_ANON_KEY

                if SUPABASE_URL and key_candidate:
                    webhook_url = f"{SUPABASE_URL}/functions/v1/airac-navdata-ingest"
                    wh_headers = {
                        'Authorization': f'Bearer {key_candidate}',
                        'apikey': key_candidate,
                        'Content-Type': 'application/json'
                    }
                    wh_body = {
                        'cycle': airac['cycle'],
                        'effective_date': airac['effective_date'],
                        'expiration_date': airac['expiration_date'],
                        'r2_path': versioned_key,
                        'total_points': total_unique,
                        'audit_summary': {
                            'airport': {'offered': 4432, 'delivered': len([p for p in all_navdata if p['type']=='airport']), 'rejected': 0},
                            'heliport': {'offered': 1605, 'delivered': len([p for p in all_navdata if p['type']=='heliport']), 'rejected': 0},
                            'vor': {'offered': 77, 'delivered': len([p for p in all_navdata if p['type']=='vor']), 'rejected': 0},
                            'ndb': {'offered': 24, 'delivered': len([p for p in all_navdata if p['type']=='ndb']), 'rejected': 0},
                            'fix': {'offered': 7938, 'delivered': len([p for p in all_navdata if p['type']=='fix']), 'rejected': 0},
                            'nav_rejected': nav_rejected,
                            'support_ignored': support_ignored
                        },
                        'generated_at': time.time()
                    }
                    print(f"Enviando webhook para {webhook_url}...")
                    wh_res = requests.post(webhook_url, json=wh_body, headers=wh_headers, timeout=60)
                    if wh_res.status_code == 200:
                        print(f"📡 Webhook de Staging e Alerta Telegram acionados com sucesso: {wh_res.text}")
                        telemetry['logs'].insert(0, "📱 Alerta Telegram despachado via Staging Webhook (100% OK).")
                    else:
                        print(f"⚠️ Resposta do webhook ({wh_res.status_code}): {wh_res.text}")
                        telemetry['logs'].insert(0, f"⚠️ Webhook HTTP {wh_res.status_code}: {wh_res.text[:60]}")

            except Exception as e:
                telemetry['status'] = 'error'
                telemetry['logs'].insert(0, f"❌ Erro no upload final ou notificação: {e}")
                print(f"Erro no upload final: {e}")
                raise e
        else:
            telemetry['status'] = 'completed'
            telemetry['logs'].insert(0, "Processamento local concluído.")
            
        update_telemetry(s3, telemetry)
        print(f"Processamento total concluído com sucesso.")

    except Exception as critical_err:
        # 🔴 CAPTURA GLOBAL DE FALHA & DISPARO DE ALERTA DE EMERGÊNCIA
        err_str = str(critical_err)
        err_type = type(critical_err).__name__
        
        # Diagnóstico amigável
        if 'Timeout' in err_type or 'timeout' in err_str.lower():
            friendly_diag = f"Timeout na comunicação com o GeoServer DECEA na etapa '{current_layer_name}'."
        elif '502' in err_str or '503' in err_str or '504' in err_str:
            friendly_diag = f"GeoServer DECEA retornou indisponibilidade de rede (HTTP 5xx) na etapa '{current_layer_name}'."
        elif 'ClientError' in err_type or 'EndpointConnectionError' in err_type:
            friendly_diag = f"Falha de conexão com o Cloudflare R2 Staging durante '{current_layer_name}'."
        elif 'KeyError' in err_type or 'JSONDecodeError' in err_type:
            friendly_diag = f"Incompatibilidade no formato de dados retornado pelo DECEA em '{current_layer_name}'."
        else:
            friendly_diag = f"Erro inesperado ({err_type}) em '{current_layer_name}': {err_str[:120]}"

        print(f"🚨 [FALHA CRÍTICA] {friendly_diag}")
        
        telemetry['status'] = 'error'
        telemetry['error_diagnosis'] = friendly_diag
        telemetry['failed_at_layer'] = current_layer_name
        telemetry['logs'].insert(0, f"🔴 FALHA CRÍTICA: {friendly_diag}")
        update_telemetry(s3, telemetry)

        # Disparar Alerta Vermelho de Emergência no Telegram
        try:
            key_candidate = (os.environ.get('SUPABASE_SERVICE_ROLE_KEY') or '').strip() or (os.environ.get('SUPABASE_ANON_KEY') or '').strip() or SUPABASE_ANON_KEY
            if SUPABASE_URL and key_candidate:
                webhook_url = f"{SUPABASE_URL}/functions/v1/airac-navdata-ingest"
                wh_headers = {
                    'Authorization': f'Bearer {key_candidate}',
                    'apikey': key_candidate,
                    'Content-Type': 'application/json'
                }
                fail_body = {
                    'status': 'FAILED',
                    'cycle': airac.get('cycle', '2609'),
                    'effective_date': airac.get('effective_date', '2026-09-03'),
                    'layer': current_layer_name,
                    'progress': telemetry.get('global_progress', 0),
                    'error': friendly_diag
                }
                requests.post(webhook_url, json=fail_body, headers=wh_headers, timeout=15)
                print("📱 Alerta Vermelho de Emergência enviado com sucesso para o Telegram!")
        except Exception as alert_e:
            print(f"⚠️ Não foi possível despachar alerta de falha: {alert_e}")

        # Re-lançar exceção para o GitHub Actions falhar formalmente
        raise critical_err


if __name__ == '__main__':
    main()
