#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção e Transferência REUL HD para Produção (R2)
=============================================================================
Objetivo:
1. Transferência real (Move: Server-Side Copy + Delete da Origem) de todos os
   setores REUL presentes em 'reul/staging/' para 'reul/' oficial em produção.
2. Copia 'reul/staging/{code}_HD.mbtiles' ➔ 'reul/{code}.mbtiles' (SQLite MBTiles).
3. Copia 'reul/staging/{code}.pmtiles' ➔ 'reul/{code}.pmtiles' (PMTiles Web).
4. Remove os arquivos de staging imediatamente após a cópia confirmada,
   evitando duplicação de armazenamento e mantendo a quarentena limpa.
5. Atualiza o manifesto oficial de produção 'reul_progress.json' e limpa os setores
   promovidos do índice de quarentena 'reul_hd_progress.json'.
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

STAGING_PROGRESS_KEY = "reul_hd_progress.json"
PROD_PROGRESS_KEY = "reul_progress.json"

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

def send_telegram_notification(title: str, text: str, status: str = "SUCCESS") -> bool:
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return False
    icon = "🚀" if status == "SUCCESS" else "🚨"
    payload = {
        "chat_id": chat_id,
        "text": f"{icon} *{title}*\n\n{text}",
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        r = requests.post(f"https://api.telegram.org/bot{bot_token}/sendMessage", json=payload, timeout=12)
        return r.status_code == 200
    except Exception:
        return False

def main():
    target_env = os.environ.get("TARGET_CODES", "").strip()
    if len(sys.argv) > 1 and not target_env:
        target_env = sys.argv[1].strip()

    print("=" * 72, flush=True)
    print("🚀 SkyFPL — Promoção Oficial REUL HD (reul/staging/ ➔ reul/)", flush=True)
    print("=" * 72, flush=True)
    print(f"  Bucket   : {R2_BUCKET}", flush=True)
    print(f"  Filtro   : {target_env or 'TODOS (ALL)'}", flush=True)
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

    staging_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=STAGING_PROGRESS_KEY)
        staging_data = json.loads(resp["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"⚠️ Aviso ao ler {STAGING_PROGRESS_KEY}: {e}", flush=True)

    staging_meta = staging_data.get("metadata", {})
    if not staging_meta:
        print(f"❌ Nenhum setor REUL encontrado em quarentena ({STAGING_PROGRESS_KEY}).", flush=True)
        sys.exit(1)

    if not target_env or target_env.upper() == "ALL":
        target_codes = list(staging_meta.keys())
    else:
        requested = [c.strip().upper() for c in target_env.split(",") if c.strip()]
        target_codes = [c for c in staging_meta.keys() if c.upper() in requested or c.replace("REUL_", "").upper() in requested]

    if not target_codes:
        print(f"❌ Nenhum setor REUL correspondente ao filtro '{target_env}' encontrado em staging.", flush=True)
        sys.exit(1)

    prod_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROD_PROGRESS_KEY)
        prod_data = json.loads(resp["Body"].read().decode("utf-8"))
    except Exception:
        prod_data = {"status": "active", "metadata": {}}

    prod_meta = prod_data.setdefault("metadata", {})
    promoted_codes = []
    total_bytes_moved = 0

    for code in target_codes:
        info = staging_meta[code]
        print(f"\n📦 Promovendo setor: {code} ({info.get('name', code)})...", flush=True)

        # 1. MBTiles
        src_mbtiles = f"reul/staging/{code}_HD.mbtiles"
        dst_mbtiles = f"reul/{code}.mbtiles"
        dst_mbtiles_hd = f"reul/{code}_HD.mbtiles"
        try:
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": src_mbtiles},
                Key=dst_mbtiles,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="COPY"
            )
            # Também mantém o alias _HD.mbtiles para compatibilidade total
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": src_mbtiles},
                Key=dst_mbtiles_hd,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="COPY"
            )
            print(f"  ✓ Copiado: {src_mbtiles} ➔ {dst_mbtiles}", flush=True)
            s3.delete_object(Bucket=R2_BUCKET, Key=src_mbtiles)
            print(f"  ✓ Expurgo de staging: {src_mbtiles}", flush=True)
        except Exception as e:
            print(f"  ❌ Erro ao mover MBTiles de {code}: {e}", flush=True)
            continue

        # 2. PMTiles
        src_pmtiles = f"reul/staging/{code}.pmtiles"
        dst_pmtiles = f"reul/{code}.pmtiles"
        try:
            s3.head_object(Bucket=R2_BUCKET, Key=src_pmtiles)
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": src_pmtiles},
                Key=dst_pmtiles,
                ContentType="application/x-pmtiles",
                MetadataDirective="COPY"
            )
            print(f"  ✓ Copiado: {src_pmtiles} ➔ {dst_pmtiles}", flush=True)
            s3.delete_object(Bucket=R2_BUCKET, Key=src_pmtiles)
            print(f"  ✓ Expurgo de staging: {src_pmtiles}", flush=True)
        except ClientError:
            print(f"  ℹ️ {src_pmtiles} não encontrado em staging (pulando PMTiles).", flush=True)

        # Atualiza metadados
        now_iso = datetime.now(timezone.utc).isoformat()
        prod_meta[code] = {
            **info,
            "promoted_at": now_iso,
            "r2_url_mbtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/reul/{code}.mbtiles",
            "r2_url_pmtiles": f"https://pub-1b4a512269cb4fc496e8badb21acf51c.r2.dev/reul/{code}.pmtiles",
        }
        total_bytes_moved += int(info.get("size_bytes", 0))
        promoted_codes.append(code)

        # Remove do staging metadata
        del staging_meta[code]

    # Salva reul_progress.json
    prod_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    prod_data["charts_total"] = len(prod_meta)
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=PROD_PROGRESS_KEY,
        Body=json.dumps(prod_data, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache, no-store, must-revalidate"
    )
    print(f"\n✅ Manifesto de produção atualizado com sucesso: {PROD_PROGRESS_KEY}", flush=True)

    # Salva reul_hd_progress.json limpo
    staging_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    staging_data["charts_done"] = len(staging_meta)
    s3.put_object(
        Bucket=R2_BUCKET,
        Key=STAGING_PROGRESS_KEY,
        Body=json.dumps(staging_data, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
        CacheControl="no-cache, no-store, must-revalidate"
    )
    print(f"✅ Quarentena atualizada: {STAGING_PROGRESS_KEY} ({len(staging_meta)} setores restantes)", flush=True)

    # Notificação
    summary_msg = f"Setores Promovidos: `{', '.join(promoted_codes)}`\nTotal Transferido: `{total_bytes_moved / 1024 / 1024:.1f} MB`"
    send_telegram_notification("Cartas REUL HD Promovidas para Produção", summary_msg, "SUCCESS")

    print("=" * 72, flush=True)
    print(f"🎉 Promoção REUL HD concluída! {len(promoted_codes)} setor(es) ativos em produção.", flush=True)
    print("=" * 72, flush=True)

if __name__ == "__main__":
    main()
