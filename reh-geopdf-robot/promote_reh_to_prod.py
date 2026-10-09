#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção e Transferência REH HD para Produção (R2)
=============================================================================
Objetivo:
1. Transferência real (Move: Server-Side Copy + Delete da Origem) de todos os
   setores REH presentes em 'reh/staging/' para 'reh/' oficial em produção.
2. Copia 'reh/staging/{code}_HD.mbtiles' ➔ 'reh/{code}.mbtiles' (SQLite MBTiles).
3. Copia 'reh/staging/{code}.pmtiles' ➔ 'reh/{code}.pmtiles' (PMTiles Web).
4. Remove os arquivos de staging imediatamente após a cópia confirmada,
   evitando duplicação de armazenamento e mantendo a quarentena limpa.
5. Atualiza o manifesto oficial de produção 'reh_progress.json' e limpa os setores
   promovidos do índice de quarentena 'reh_hd_progress.json'.
6. Envia relatório de confirmação via Telegram Bot API (se configurado).
=============================================================================
"""

import os
import sys
import json
import requests
try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception
from datetime import datetime, timezone, timedelta

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")

STAGING_PROGRESS_KEY = "reh_hd_progress.json"
PROD_PROGRESS_KEY = "reh_progress.json"

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
                "expiration_dt": dt + timedelta(days=28),
                "expiration_date": (dt + timedelta(days=28)).strftime("%d/%m/%Y")
            })

    all_cycles.sort(key=lambda x: x["effective_dt"])
    current = None
    next_cycle = None
    for i, c in enumerate(all_cycles):
        if c["effective_dt"] <= now < c["expiration_dt"]:
            current = c
            if i + 1 < len(all_cycles):
                next_cycle = all_cycles[i + 1]
            break

    if not current and all_cycles:
        current = all_cycles[-1]

    return {
        "current": current or {"cycle": "2609", "effective_date": "03/09/2026", "expiration_date": "01/10/2026"},
        "next": next_cycle or {"cycle": "2610", "effective_date": "01/10/2026", "expiration_date": "29/10/2026"}
    }

def send_telegram_alert(message: str):
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if token and chat_id:
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {
                "chat_id": chat_id,
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True
            }
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            print(f"[Aviso] Falha ao enviar alerta no Telegram: {e}", flush=True)

def main():
    target_env = os.environ.get("TARGET_CODES", "ALL").strip()
    if len(sys.argv) > 1 and target_env in ("", "ALL"):
        target_env = sys.argv[1].strip()

    is_all = target_env.upper() == "ALL" or not target_env
    target_filter = None if is_all else set(c.strip().upper() for c in target_env.split(",") if c.strip())

    print("=" * 72, flush=True)
    print("🚀 SkyFPL — Promoção e Transferência de Setores REH HD (R2)", flush=True)
    print("=" * 72, flush=True)
    print(f"  Bucket       : {R2_BUCKET}", flush=True)
    print(f"  Alvo         : {'TODOS OS DISPONÍVEIS' if is_all else ', '.join(target_filter)}", flush=True)
    print(f"  Origem       : reh/staging/", flush=True)
    print(f"  Destino      : reh/", flush=True)
    print("=" * 72, flush=True)

    if not (boto3 and R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET):
        print("❌ ERRO: Credenciais Cloudflare R2 ausentes ou boto3 não instalado.", flush=True)
        sys.exit(1)

    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto"
    )

    airac_info = get_airac_cycle_info()
    current_cycle = airac_info["current"]["cycle"]
    effective_date = airac_info["current"]["effective_date"]

    # Obter metadados de quarentena existentes
    staging_metadata = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=STAGING_PROGRESS_KEY)
        sp_data = json.loads(resp["Body"].read().decode("utf-8"))
        staging_metadata = sp_data.get("metadata", {})
    except ClientError:
        pass

    # Obter metadados de produção existentes
    prod_data = {
        "status": "completed",
        "progress_percent": 100,
        "cycle": current_cycle,
        "effective_date": effective_date,
        "charts_done": 0,
        "charts_total": 12,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "metadata": {}
    }
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROD_PROGRESS_KEY)
        prod_data = json.loads(resp["Body"].read().decode("utf-8"))
    except ClientError:
        pass

    prod_metadata = prod_data.get("metadata", {})

    # Mapear objetos em reh/staging/
    paginator = s3.get_paginator("list_objects_v2")
    staging_files = []
    for page in paginator.paginate(Bucket=R2_BUCKET, Prefix="reh/staging/"):
        for obj in page.get("Contents", []):
            staging_files.append(obj)

    if not staging_files:
        print("⚠️ Nenhum arquivo encontrado em reh/staging/ para promoção.", flush=True)
        sys.exit(0)

    # Identificar setores disponíveis
    sectors_to_promote = {}
    for obj in staging_files:
        key = obj["Key"]
        fname = key.replace("reh/staging/", "")
        if fname.endswith("_HD.mbtiles"):
            code = fname.replace("_HD.mbtiles", "").upper()
            if is_all or (target_filter and code in target_filter):
                sectors_to_promote.setdefault(code, {})["mbtiles"] = key
                sectors_to_promote[code]["size_bytes"] = obj["Size"]
        elif fname.endswith(".pmtiles"):
            code = fname.replace(".pmtiles", "").upper()
            if is_all or (target_filter and code in target_filter):
                sectors_to_promote.setdefault(code, {})["pmtiles"] = key

    print(f"📦 Identificados {len(sectors_to_promote)} setor(es) prontos para promoção.\n", flush=True)

    promoted_sectors = []
    errors = []

    for code, files in sectors_to_promote.items():
        print(f"🔄 Processando {code}...", flush=True)
        mbtiles_staging = files.get("mbtiles")
        pmtiles_staging = files.get("pmtiles")

        if not mbtiles_staging:
            print(f"  ⚠️ Pulo: MBTiles não encontrado para {code}.", flush=True)
            continue

        try:
            # 1. Copiar MBTiles para produção
            mbtiles_prod_key = f"reh/{code}.mbtiles"
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": mbtiles_staging},
                Key=mbtiles_prod_key,
                ContentType="application/x-sqlite3",
                MetadataDirective="COPY"
            )
            print(f"  ✓ MBTiles promovido: {mbtiles_prod_key}", flush=True)

            # 2. Copiar PMTiles para produção (se presente)
            if pmtiles_staging:
                pmtiles_prod_key = f"reh/{code}.pmtiles"
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": pmtiles_staging},
                    Key=pmtiles_prod_key,
                    ContentType="application/octet-stream",
                    MetadataDirective="COPY"
                )
                print(f"  ✓ PMTiles promovido: {pmtiles_prod_key}", flush=True)

            # 3. Remover originais de staging
            s3.delete_object(Bucket=R2_BUCKET, Key=mbtiles_staging)
            if pmtiles_staging:
                s3.delete_object(Bucket=R2_BUCKET, Key=pmtiles_staging)
            print(f"  🗑️ Quarentena limpa para {code}.", flush=True)

            # 4. Atualizar metadados do setor
            old_stg_meta = staging_metadata.get(code, {})
            size_bytes = files.get("size_bytes", 0)
            size_mb = f"{size_bytes / (1024*1024):.1f}"

            prod_metadata[code] = {
                "code": code,
                "name": old_stg_meta.get("name", code),
                "cycle": current_cycle,
                "effective_date": effective_date,
                "size_bytes": size_bytes,
                "size_mb": size_mb,
                "promoted_at": datetime.now(timezone.utc).isoformat(),
                "r2_url_mbtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/{mbtiles_prod_key}",
                "r2_url_pmtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/reh/{code}.pmtiles"
            }

            # Remover do staging metadata
            staging_metadata.pop(code, None)
            promoted_sectors.append(code)

        except Exception as e:
            print(f"  ❌ Erro ao promover {code}: {e}", flush=True)
            errors.append(f"{code}: {str(e)}")

    # 5. Salvar novos metadados de produção
    prod_data["metadata"] = prod_metadata
    prod_data["charts_done"] = len(prod_metadata)
    prod_data["charts_total"] = 12
    prod_data["updated_at"] = datetime.now(timezone.utc).isoformat()

    s3.put_object(
        Bucket=R2_BUCKET,
        Key=PROD_PROGRESS_KEY,
        Body=json.dumps(prod_data, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache, no-store, must-revalidate"
    )
    print(f"\n✓ {PROD_PROGRESS_KEY} atualizado com sucesso no R2.", flush=True)

    # 6. Salvar metadados atualizados de quarentena
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=STAGING_PROGRESS_KEY)
        sp_data = json.loads(resp["Body"].read().decode("utf-8"))
        sp_data["metadata"] = staging_metadata
        sp_data["updated_at"] = datetime.now(timezone.utc).isoformat()
        logs = sp_data.get("logs", [])
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        logs.append(f"[{now_str}] [PROMOÇÃO] {len(promoted_sectors)} setor(es) promovido(s) para produção oficial (reh/). Quarentena limpa.")
        sp_data["logs"] = logs[-300:]

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=STAGING_PROGRESS_KEY,
            Body=json.dumps(sp_data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        print(f"✓ {STAGING_PROGRESS_KEY} atualizado com sucesso no R2.", flush=True)
    except ClientError:
        pass

    # 7. Relatório no Telegram
    if promoted_sectors:
        msg = f"🚁 <b>PROMOÇÃO OFICIAL REH HD CONCLUÍDA!</b>\n━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        msg += f"📅 <b>Ciclo:</b> {current_cycle} (Vigência: {effective_date})\n"
        msg += f"📦 <b>Setores Promovidos ({len(promoted_sectors)}):</b>\n"
        for sc in promoted_sectors:
            msg += f"  • <code>{sc}</code>\n"
        msg += f"\n✅ Arquivos disponíveis em <code>reh/</code>.\nQuarentena limpa com sucesso."
        send_telegram_alert(msg)

    print("\n" + "=" * 72, flush=True)
    print(f"🎉 Promoção concluída: {len(promoted_sectors)} setor(es) transferidos com sucesso.", flush=True)
    if errors:
        print(f"⚠️ Erros encontrados ({len(errors)}): {', '.join(errors)}", flush=True)
    print("=" * 72, flush=True)

if __name__ == "__main__":
    main()
