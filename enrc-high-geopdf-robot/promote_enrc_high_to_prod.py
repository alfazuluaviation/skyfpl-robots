#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção e Transferência ENRC HIGH HD para Produção (R2)
=============================================================================
Objetivo:
1. Transferência real (Move: Server-Side Copy + Delete da Origem) de todas as
   folhas ENRC H presentes em 'enrc_high/staging/' para 'enrc_high/' oficial.
2. Copia 'enrc_high/staging/{code}_HD.mbtiles' ➔ 'enrc_high/{code}.mbtiles' (SQLite MBTiles).
3. Copia 'enrc_high/staging/{code}.pmtiles' ➔ 'enrc_high/{code}.pmtiles' (PMTiles Web).
4. Remove os arquivos de staging imediatamente após a cópia confirmada,
   evitando duplicação de armazenamento e mantendo a quarentena limpa.
5. Atualiza o índice oficial 'enrch_progress.json' e remove as cartas promovidas
   de 'enrch_hd_progress.json'.
=============================================================================
"""

import os
import sys
import json
import requests
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta

ALL_ENRC_HIGH_CODES = ["H1", "H2", "H3", "H4", "H5", "H6", "H7", "H8", "H9", "FULL"]

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "enrch_hd_progress.json"
PROD_PROGRESS_KEY = "enrch_progress.json"

def get_airac_cycle_info() -> dict:
    cal_file = os.path.join(os.path.dirname(__file__), "calendar.json")
    master_cal = {}
    if os.path.exists(cal_file):
        try:
            with open(cal_file, "r", encoding="utf-8") as f:
                master_cal = json.load(f)
        except Exception:
            pass

    now = datetime.now(timezone.utc)
    all_cycles = []
    for yr, cycles in master_cal.items():
        for cid, dt_str in cycles.items():
            p = [int(x) for x in dt_str.split("/")]
            dt = datetime(p[2], p[1], p[0], tzinfo=timezone.utc)
            all_cycles.append({
                "cycle": cid,
                "amdt": cid,
                "effective_dt": dt,
                "effective_date": dt_str,
                "iso_effective_date": dt.strftime("%Y-%m-%d"),
                "publication_date": (dt - timedelta(days=14)).strftime("%d/%m/%Y"),
                "expiration_date": (dt + timedelta(days=28)).strftime("%d/%m/%Y"),
            })

    all_cycles.sort(key=lambda x: x["effective_dt"])

    current = None
    next_c = None
    for i, c in enumerate(all_cycles):
        if c["effective_dt"] <= now:
            current = c
            if i + 1 < len(all_cycles):
                next_c = all_cycles[i + 1]

    if not current and all_cycles:
        current = all_cycles[0]

    target = current
    if next_c:
        days = (next_c["effective_dt"] - now).days
        if 0 <= days <= 14:
            target = next_c

    return target or {
        "cycle": "2609",
        "amdt": "2609",
        "effective_date": "03/09/2026",
        "publication_date": "20/08/2026",
        "expiration_date": "01/10/2026"
    }

def send_telegram_notification(
    title: str,
    status: str,
    cycle: str = "",
    effective_date: str = "",
    processed_items: list = None,
    error_msg: str = None
) -> bool:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return False

    processed_items = processed_items or []
    if status == "FAILED":
        lines = [
            f"🚨 <b>ALERTA VERMELHO — {title}</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🛰️ <b>Ciclo / Alvo:</b> <code>{cycle or 'N/A'}</code>",
            f"⚠️ <b>Diagnóstico:</b> {error_msg or 'Falha desconhecida'}",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "❌ <b>Status:</b> FAILED (Intervenção Necessária)"
        ]
    elif status == "PROMOTED":
        lines = [
            f"🚀 <b>{title}</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🛰️ <b>Ciclo Oficial:</b> <code>{cycle}</code>",
        ]
        if effective_date:
            lines.append(f"⏳ <b>Vigência DECEA:</b> <code>{effective_date}</code>")
        lines.append(f"📦 <b>Cartas Promovidas ({len(processed_items)}):</b>")
        for item in processed_items:
            lines.append(f"  • {item}")
        lines.append("🌐 <b>Destino:</b> Produção Oficial (R2: <code>enrc_high/</code>)")
        lines.append("🗑️ <b>Staging:</b> Quarentena Limpa (<code>enrc_high/staging/</code>)")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("✅ <b>Status:</b> 100% CONCLUÍDO & VIGENTE")
    else:
        lines = [
            f"🗺️ <b>{title}</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🛰️ <b>Ciclo:</b> <code>{cycle}</code>",
            f"📦 <b>Cartas Processadas:</b> {len(processed_items)}",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            "✅ <b>Status:</b> SUCESSO"
        ]

    msg = "\n".join(lines)
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=15)
        return resp.status_code == 200
    except Exception as e:
        print(f"⚠️ [Telegram] Erro ao enviar: {e}", flush=True)
        return False

def main():
    print("=" * 70, flush=True)
    print("🚀 SkyFPL — Transferência ENRC HIGH HD: Staging ➔ Produção Oficial (R2)", flush=True)
    print("=" * 70, flush=True)
    print(f"  Bucket: {R2_BUCKET}", flush=True)
    print("  Origem : enrc_high/staging/{code}_HD.mbtiles e {code}.pmtiles", flush=True)
    print("  Destino: enrc_high/{code}.mbtiles e enrc_high/{code}.pmtiles", flush=True)
    print("  Política: TRANSFERÊNCIA REAL (Cópia Server-Side + Expurgo de Staging)", flush=True)
    print("=" * 70, flush=True)

    if not (R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET):
        print("❌ ERRO: Credenciais Cloudflare R2 não encontradas no ambiente.", flush=True)
        sys.exit(1)

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto"
    )

    progress_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
        progress_data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de staging '{PROGRESS_KEY}' carregada.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao carregar telemetria de staging: {e}. Prosseguindo...", flush=True)

    prod_data = {}
    try:
        resp_prod = s3.get_object(Bucket=R2_BUCKET, Key=PROD_PROGRESS_KEY)
        prod_data = json.loads(resp_prod["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de produção '{PROD_PROGRESS_KEY}' carregada.", flush=True)
    except Exception:
        print(f"ℹ️ Criando novo índice de produção '{PROD_PROGRESS_KEY}'.", flush=True)

    promoted_count = 0
    total_bytes = 0
    promoted_codes = []
    staging_metadata = progress_data.get("metadata", {})
    prod_metadata = prod_data.get("metadata", {})

    catalog = {}
    cat_path = os.path.join(os.path.dirname(__file__), "enrc_high_catalog.json")
    if os.path.exists(cat_path):
        try:
            with open(cat_path, "r", encoding="utf-8") as f:
                catalog = json.load(f)
        except Exception:
            pass
    airac_info = get_airac_cycle_info()

    target_env = os.environ.get("TARGET_CODES", "").strip()
    if target_env and target_env.upper() != "ALL":
        target_codes = [c.strip().upper() for c in target_env.split(",") if c.strip()]
        print(f"  🎯 Modo Seleção Ativo: Promovendo apenas {len(target_codes)} carta(s): {', '.join(target_codes)}", flush=True)
    else:
        target_codes = ALL_ENRC_HIGH_CODES
        print("  🌐 Modo Completo: Promovendo todas as cartas disponíveis em staging.", flush=True)

    print("\n📦 ETAPA 1: Transferindo cartas (Cópia Server-Side + Limpeza de Origem)...", flush=True)
    for idx, code in enumerate(target_codes, start=1):
        staging_mbtiles_key = f"enrc_high/staging/{code}_HD.mbtiles"
        prod_mbtiles_key = f"enrc_high/{code}.mbtiles"

        staging_pmtiles_key = f"enrc_high/staging/{code}.pmtiles"
        prod_pmtiles_key = f"enrc_high/{code}.pmtiles"

        try:
            head_resp = s3.head_object(Bucket=R2_BUCKET, Key=staging_mbtiles_key)
            size_bytes = head_resp.get("ContentLength", 0)
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"  [{idx:02d}/{len(target_codes):02d}] ⏭️ {code}: Não encontrado em staging ({staging_mbtiles_key}). Pulando.", flush=True)
                continue
            else:
                print(f"  [{idx:02d}/{len(target_codes):02d}] ❌ Erro ao verificar {code}: {e}", flush=True)
                continue

        print(f"  [{idx:02d}/{len(target_codes):02d}] 🚚 {code} ({(size_bytes / 1024 / 1024):.2f} MB):", flush=True)

        try:
            # 1. Copia MBTiles
            print(f"      ↳ Copiando MBTiles: {staging_mbtiles_key} ➔ {prod_mbtiles_key}...", flush=True)
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": staging_mbtiles_key},
                Key=prod_mbtiles_key,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="REPLACE"
            )

            # 2. Copia PMTiles se existir
            has_pmtiles = False
            pmtiles_size_mb = None
            try:
                head_pm = s3.head_object(Bucket=R2_BUCKET, Key=staging_pmtiles_key)
                pm_bytes = head_pm.get("ContentLength", 0)
                pmtiles_size_mb = f"{(pm_bytes / 1024 / 1024):.2f}"
                print(f"      ↳ Copiando PMTiles: {staging_pmtiles_key} ➔ {prod_pmtiles_key}...", flush=True)
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": staging_pmtiles_key},
                    Key=prod_pmtiles_key,
                    ContentType="application/x-pmtiles",
                    MetadataDirective="REPLACE"
                )
                has_pmtiles = True
            except ClientError as e_pm:
                if e_pm.response["Error"]["Code"] != "404":
                    print(f"      ⚠️ Aviso PMTiles para {code}: {e_pm}", flush=True)

            # 3. Expurga arquivos de staging
            print(f"      ↳ Limpando quarentena: deletando {staging_mbtiles_key}...", flush=True)
            s3.delete_object(Bucket=R2_BUCKET, Key=staging_mbtiles_key)
            if has_pmtiles:
                print(f"      ↳ Limpando quarentena: deletando {staging_pmtiles_key}...", flush=True)
                s3.delete_object(Bucket=R2_BUCKET, Key=staging_pmtiles_key)

            # 4. Registra metadados
            chart_info = catalog.get(code, {})
            chart_name = chart_info.get("name", code)
            stg_meta = staging_metadata.get(code, {})

            effective_dt = chart_info.get("effective_date") or stg_meta.get("effective_date") or airac_info.get("effective_date", "03/09/2026")
            amdt_val = chart_info.get("amdt") or stg_meta.get("amdt") or airac_info.get("cycle", "2609")

            prod_metadata[code] = {
                "name": f"SkyFPL ENRC HIGH {code}",
                "description": f"Carta {code} - {chart_name}",
                "size_bytes": size_bytes,
                "size_mb": f"{(size_bytes / 1024 / 1024):.2f}",
                "r2_url_mbtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{prod_mbtiles_key}",
                "r2_key": prod_mbtiles_key,
                "pmtiles_url": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{prod_pmtiles_key}" if has_pmtiles else None,
                "pmtiles_key": prod_pmtiles_key if has_pmtiles else None,
                "pmtiles_size_mb": pmtiles_size_mb or stg_meta.get("pmtiles_size_mb"),
                "effective_date": effective_dt,
                "publication_date": chart_info.get("effective_date", ""),
                "amdt": amdt_val,
                "cycle": airac_info.get("cycle", "2609"),
                "promoted_at": datetime.now(timezone.utc).isoformat(),
                "status": "production"
            }

            promoted_count += 1
            total_bytes += size_bytes
            promoted_codes.append(f"{code} ({(size_bytes / 1024 / 1024):.1f} MB)")
            print(f"      ✅ {code} promovido com sucesso!", flush=True)

        except Exception as e:
            print(f"      ❌ Falha na transferência de {code}: {e}", flush=True)

    if promoted_count == 0:
        print("\n⚠️ Nenhuma carta foi promovida. Verifique se o robô gerou artefatos em 'enrc_high/staging/'.", flush=True)
        return

    # Atualiza índices
    print("\n📝 ETAPA 2: Atualizando índices e telemetrias no Cloudflare R2...", flush=True)

    # 1. Salva enrch_progress.json
    prod_data["metadata"] = prod_metadata
    prod_data["total_charts"] = len(prod_metadata)
    prod_data["total_size_mb"] = f"{(total_bytes / 1024 / 1024):.2f}"
    prod_data["current_cycle"] = airac_info.get("cycle", "2609")
    prod_data["effective_date"] = airac_info.get("effective_date", "03/09/2026")
    prod_data["last_promotion"] = datetime.now(timezone.utc).isoformat()
    prod_data["status"] = "active"

    s3.put_object(
        Bucket=R2_BUCKET,
        Key=PROD_PROGRESS_KEY,
        Body=json.dumps(prod_data, indent=2).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache, no-store"
    )
    print(f"  ✅ Índice oficial '{PROD_PROGRESS_KEY}' atualizado com {len(prod_metadata)} cartas.", flush=True)

    # 2. Limpa cartas promovidas do staging
    for item in promoted_codes:
        c_code = item.split()[0]
        if c_code in staging_metadata:
            del staging_metadata[c_code]

    progress_data["metadata"] = staging_metadata
    progress_data["charts_processed"] = len(staging_metadata)
    progress_data["status"] = "idle" if len(staging_metadata) == 0 else "staging_partial"
    progress_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    if len(staging_metadata) == 0:
        progress_data["progress_percent"] = 100
        progress_data["current_chart"] = ""

    s3.put_object(
        Bucket=R2_BUCKET,
        Key=PROGRESS_KEY,
        Body=json.dumps(progress_data, indent=2).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache, no-store"
    )
    print(f"  ✅ Telemetria de staging '{PROGRESS_KEY}' atualizada (quarentena limpa).", flush=True)

    # Notificação Telegram
    send_telegram_notification(
        title="PROMOÇÃO ENRC HIGH HD ➔ PRODUÇÃO OFICIAL",
        status="PROMOTED",
        cycle=f"Ciclo {airac_info.get('cycle', '2609')} ({airac_info.get('effective_date', '')})",
        effective_date=airac_info.get('effective_date', ''),
        processed_items=promoted_codes
    )

    print("\n" + "=" * 70, flush=True)
    print(f"🎉 Promoção concluída com sucesso! {promoted_count} cartas transferidas para enrc_high/.", flush=True)
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
