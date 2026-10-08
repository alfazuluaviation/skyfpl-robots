#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção e Transferência REA HD para Produção (R2)
=============================================================================
Objetivo:
1. Transferência real (Move: Server-Side Copy + Delete da Origem) de todos os
   setores REA presentes em 'rea/staging/' para 'rea/' oficial em produção.
2. Copia 'rea/staging/{code}_HD.mbtiles' ➔ 'rea/{code}.mbtiles' (SQLite MBTiles).
3. Copia 'rea/staging/{code}.pmtiles' ➔ 'rea/{code}.pmtiles' (PMTiles Web).
4. Remove os arquivos de staging imediatamente após a cópia confirmada,
   evitando duplicação de armazenamento e mantendo a quarentena limpa.
5. Atualiza o manifesto oficial de produção 'rea_progress.json' e limpa os setores
   promovidos do índice de quarentena 'rea_hd_progress.json'.
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

STAGING_PROGRESS_KEY = "rea_hd_progress.json"
PROD_PROGRESS_KEY = "rea_progress.json"

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
    lines = [
        f"🚀 <b>{title}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🚁 <b>Cartas REA:</b> Promoção Staging ➔ Produção",
        f"🛰️ <b>Ciclo AIRAC:</b> <code>{cycle or 'N/A'}</code>",
    ]
    if effective_date:
        lines.append(f"⏳ <b>Vigência:</b> <code>{effective_date}</code>")

    if status == "PROMOTED":
        lines.append(f"📦 <b>Setores Promovidos ({len(processed_items)}):</b>")
        for item in processed_items[:20]:
            lines.append(f"  • <code>{item}</code>")
        if len(processed_items) > 20:
            lines.append(f"  • <i>... e mais {len(processed_items) - 20} setores.</i>")
        lines.append("🌐 <b>Destino:</b> Produção Oficial (rea/)")
        lines.append("🗑️ <b>Staging:</b> Quarentena Limpa")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("✅ <b>Status:</b> 100% PROMOVIDO & ATIVO")
    else:
        lines.append(f"⚠️ <b>Diagnóstico:</b> {error_msg or 'Falha na promoção'}")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("❌ <b>Status:</b> FAILED")

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
    except Exception:
        return False

def main():
    print("=" * 72, flush=True)
    print("🚀 SkyFPL — Promoção REA HD: Quarentena ➔ Produção Oficial (R2)", flush=True)
    print("=" * 72, flush=True)
    print(f"  Bucket   : {R2_BUCKET}", flush=True)
    print("  Origem   : rea/staging/{code}_HD.mbtiles e {code}.pmtiles", flush=True)
    print("  Destino  : rea/{code}.mbtiles e rea/{code}.pmtiles", flush=True)
    print("  Política : TRANSFERÊNCIA REAL (Cópia Server-Side + Limpeza de Origem)", flush=True)
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

    # 1. Carregar catálogo oficial
    catalog = {}
    cat_path = os.path.join(os.path.dirname(__file__), "rea_catalog.json")
    if os.path.exists(cat_path):
        try:
            with open(cat_path, "r", encoding="utf-8") as f:
                catalog = json.load(f)
        except Exception:
            pass

    # 2. Carregar telemetria de staging
    staging_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=STAGING_PROGRESS_KEY)
        staging_data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de quarentena '{STAGING_PROGRESS_KEY}' carregada.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao ler '{STAGING_PROGRESS_KEY}': {e}. Prosseguindo...", flush=True)

    # 3. Carregar telemetria existente de produção
    prod_data = {}
    try:
        resp_prod = s3.get_object(Bucket=R2_BUCKET, Key=PROD_PROGRESS_KEY)
        prod_data = json.loads(resp_prod["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de produção '{PROD_PROGRESS_KEY}' carregada.", flush=True)
    except Exception:
        print(f"ℹ️ Criando novo índice de produção '{PROD_PROGRESS_KEY}'.", flush=True)

    staging_metadata = staging_data.get("metadata", {})
    prod_metadata = prod_data.get("metadata", {})
    airac_info = get_airac_cycle_info()

    # 4. Determinar setores alvo
    target_env = os.environ.get("TARGET_CODES", "").strip()
    if len(sys.argv) > 1 and not target_env:
        target_env = sys.argv[1].strip()

    if target_env and target_env.upper() != "ALL":
        requested = [c.strip().upper() for c in target_env.split(",") if c.strip()]
        target_codes = []
        for req in requested:
            if req in catalog:
                target_codes.append(req)
            else:
                for k in catalog.keys():
                    if req in k or k.replace("REA_", "") == req:
                        if k not in target_codes:
                            target_codes.append(k)
        print(f"🎯 Modo Seleção Ativo: Promovendo {len(target_codes)} setor(es): {', '.join(target_codes)}", flush=True)
    else:
        target_codes = list(catalog.keys())
        print(f"🌐 Modo Completo: Verificando todos os {len(target_codes)} setores REA para promoção.", flush=True)

    promoted_codes = []
    promoted_count = 0
    total_bytes = 0

    print("\n📦 ETAPA 1: Transferindo setores (Cópia Server-Side + Expurgo de Quarentena)...", flush=True)
    for idx, code in enumerate(target_codes, start=1):
        candidates = [
            f"rea/staging/{code}_HD.mbtiles",
            f"rea/staging/{code}.mbtiles"
        ]

        src_key = None
        size = 0
        for cand in candidates:
            try:
                head = s3.head_object(Bucket=R2_BUCKET, Key=cand)
                src_key = cand
                size = head.get("ContentLength", 0)
                break
            except ClientError:
                continue

        dst_mbtiles_key = f"rea/{code}.mbtiles"
        dst_pmtiles_key = f"rea/{code}.pmtiles"

        if not src_key:
            # Verifica se já está em produção
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=dst_mbtiles_key)
                print(f"  [{idx:02d}/{len(target_codes):02d}] ℹ️ Setor {code} já ativo em produção (sem novos arquivos em staging).", flush=True)
            except ClientError:
                print(f"  [{idx:02d}/{len(target_codes):02d}] ⚠️ Setor {code} não encontrado em staging nem em produção.", flush=True)
            continue

        try:
            total_bytes += size
            print(f"  [{idx:02d}/{len(target_codes):02d}] 🔄 Transferindo {code} ({size / (1024*1024):.2f} MB)...", end=" ", flush=True)

            # A. Copiar MBTiles para produção
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": src_key},
                Key=dst_mbtiles_key,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="COPY"
            )
            print("✓ MBTILES", end=" ", flush=True)

            # B. Copiar PMTiles para produção se existir em staging
            pmtiles_src = f"rea/staging/{code}.pmtiles"
            has_pmtiles = False
            pmtiles_size = 0
            try:
                pm_head = s3.head_object(Bucket=R2_BUCKET, Key=pmtiles_src)
                pmtiles_size = pm_head.get("ContentLength", 0)
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": pmtiles_src},
                    Key=dst_pmtiles_key,
                    ContentType="application/x-pmtiles",
                    MetadataDirective="COPY"
                )
                has_pmtiles = True
                print("+ PMTILES", end=" ", flush=True)
            except ClientError:
                pass

            # C. Expurgo imediato de staging (Server-Side Delete)
            keys_to_delete = [{"Key": src_key}]
            if has_pmtiles:
                keys_to_delete.append({"Key": pmtiles_src})

            s3.delete_objects(
                Bucket=R2_BUCKET,
                Delete={"Objects": keys_to_delete}
            )
            print("➔ 🗑️ QUARENTENA LIMPA", flush=True)

            # D. Atualizar metadados de produção
            chart_info = catalog.get(code, {})
            if code in staging_metadata:
                prod_metadata[code] = staging_metadata[code]
            else:
                prod_metadata[code] = {
                    "size_bytes": size,
                    "size_mb": round(size / (1024 * 1024), 2),
                    "pmtiles_size_bytes": pmtiles_size,
                    "pmtiles_size_mb": round(pmtiles_size / (1024 * 1024), 2) if pmtiles_size else None
                }

            prod_metadata[code]["promoted_at"] = datetime.now(timezone.utc).isoformat()
            if not prod_metadata[code].get("name"):
                prod_metadata[code]["name"] = chart_info.get("name", code)
            if not prod_metadata[code].get("cycle"):
                prod_metadata[code]["cycle"] = airac_info.get("cycle", "2609")
            if not prod_metadata[code].get("amdt"):
                prod_metadata[code]["amdt"] = airac_info.get("cycle", "2609")
            if not prod_metadata[code].get("effective_date"):
                prod_metadata[code]["effective_date"] = airac_info.get("effective_date", "03/09/2026")

            # Remove da quarentena nos metadados
            staging_metadata.pop(code, None)

            promoted_codes.append(code)
            promoted_count += 1
        except Exception as e:
            print(f"❌ ERRO ao transferir {code}: {e}", flush=True)

    print(f"\n🎉 Transferência finalizada! {promoted_count} setor(es) promovido(s) ({total_bytes / (1024*1024):.2f} MB).", flush=True)

    # 5. Salvar novos metadados oficiais de produção (rea_progress.json)
    try:
        prod_payload = {
            "status": "production_active",
            "environment": "production",
            "charts_promoted": len(prod_metadata),
            "current_cycle": airac_info.get("cycle", "2609"),
            "effective_date": airac_info.get("effective_date", "03/09/2026"),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "metadata": prod_metadata
        }
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROD_PROGRESS_KEY,
            Body=json.dumps(prod_payload, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
        print(f"✅ Índice de produção '{PROD_PROGRESS_KEY}' atualizado com sucesso no R2.", flush=True)
    except Exception as e:
        print(f"⚠️ Erro ao atualizar '{PROD_PROGRESS_KEY}': {e}", flush=True)

    # 6. Salvar atualização dos metadados de quarentena (removendo os promovidos)
    try:
        staging_data["metadata"] = staging_metadata
        staging_data["updated_at"] = datetime.now(timezone.utc).isoformat()
        if "logs" in staging_data:
            now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
            staging_data["logs"].append(f"[{now_str}] [PROMOTION] {promoted_count} setor(es) promovido(s) para produção oficial.")
            staging_data["logs"] = staging_data["logs"][-300:]

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=STAGING_PROGRESS_KEY,
            Body=json.dumps(staging_data, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
        print(f"✅ Índice de quarentena '{STAGING_PROGRESS_KEY}' atualizado (promovidos removidos).", flush=True)
    except Exception as e:
        print(f"⚠️ Erro ao atualizar '{STAGING_PROGRESS_KEY}': {e}", flush=True)

    # 7. Telegram
    if promoted_count > 0:
        send_telegram_notification(
            title="SkyFPL REA HD — Promoção para Produção Concluída",
            status="PROMOTED",
            cycle=airac_info.get("cycle"),
            effective_date=airac_info.get("effective_date"),
            processed_items=promoted_codes
        )

if __name__ == "__main__":
    main()
