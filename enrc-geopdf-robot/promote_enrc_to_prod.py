#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção e Transferência ENRC LOW HD para Produção (R2)
=============================================================================
Objetivo:
1. Transferência real (Move: Server-Side Copy + Delete da Origem) de todas as
   folhas ENRC L presentes em 'enrc/staging/' para 'enrc/' oficial.
2. Copia 'enrc/staging/{code}_HD.mbtiles' ➔ 'enrc/{code}.mbtiles' (SQLite MBTiles).
3. Copia 'enrc/staging/{code}.pmtiles' ➔ 'enrc/{code}.pmtiles' (PMTiles Web).
4. Remove os arquivos de staging imediatamente após a cópia confirmada,
   evitando duplicação de armazenamento e mantendo a quarentena limpa.
5. Atualiza o índice oficial 'enrcl_progress.json' e remove as cartas promovidas
   de 'enrcl_hd_progress.json'.
=============================================================================
"""

import os
import sys
import json
import requests
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, timedelta

ALL_ENRC_CODES = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "FULL"]

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "enrcl_hd_progress.json"
PROD_PROGRESS_KEY = "enrcl_progress.json"

def get_airac_cycle_info() -> dict:
    """
    Carrega o calendário AIRAC oficial (calendar.json) e determina o ciclo vigente,
    a data de efetivação e a data de publicação.
    """
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
                "effective_date": dt_str,  # DD/MM/YYYY
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
    status: str,  # "SUCCESS", "PROMOTED", "FAILED"
    cycle: str = "",
    effective_date: str = "",
    processed_items: list = None,
    error_msg: str = None,
    step: str = "",
    recent_logs: list = None
) -> bool:
    """Dispara relatório de telemetria ou alerta de emergência diretamente via Telegram Bot API."""
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID")
    if not bot_token or not chat_id:
        return False

    processed_items = processed_items or []
    recent_logs = recent_logs or []

    if status == "FAILED":
        lines = [
            f"🚨 <b>ALERTA VERMELHO — {title}</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🛰️ <b>Ciclo / Alvo:</b> <code>{cycle or 'N/A'}</code>",
        ]
        if step:
            lines.append(f"📍 <b>Etapa / Carta:</b> <code>{step}</code>")
        if error_msg:
            lines.append(f"⚠️ <b>Diagnóstico:</b> {error_msg}")
        if recent_logs:
            lines.append("")
            lines.append("📋 <b>Últimos Logs:</b>")
            for l in recent_logs[-4:]:
                lines.append(f"• <code>{l}</code>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("❌ <b>Status:</b> FAILED (Intervenção Necessária)")
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
        lines.append("🌐 <b>Destino:</b> Produção Oficial (R2)")
        lines.append("🗑️ <b>Staging:</b> Quarentena Limpa")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("✅ <b>Status:</b> 100% CONCLUÍDO & VIGENTE")
    else:  # SUCCESS
        lines = [
            f"🗺️ <b>{title}</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🛰️ <b>Ciclo / Emenda:</b> <code>{cycle}</code>",
        ]
        if effective_date:
            lines.append(f"⏳ <b>Vigência DECEA:</b> <code>{effective_date}</code>")
        lines.append(f"📊 <b>Processamento Concluído ({len(processed_items)} cartas):</b>")
        for item in processed_items[:12]:
            lines.append(f"  • {item}")
        if len(processed_items) > 12:
            lines.append(f"  • ... e mais {len(processed_items) - 12} cartas.")
        lines.append("🛡️ <b>Quarentena:</b> Staging no R2 (Aguardando Homologação)")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("✅ <b>Status:</b> 100% SUCESSO (Zero Erros)")

    msg = "\n".join(lines)
    try:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }, timeout=15)
        if resp.status_code == 200:
            print("📱 [Telegram] Relatório despachado com sucesso!", flush=True)
            try:
                with open(".python_alert_sent", "w") as f:
                    f.write("alert_sent")
            except Exception:
                pass
            return True
        else:
            print(f"⚠️ [Telegram] Falha ao enviar: HTTP {resp.status_code} - {resp.text}", flush=True)
            return False
    except Exception as e:
        print(f"⚠️ [Telegram] Erro ao despachar alerta: {e}", flush=True)
        return False

def main():
    print("=" * 70, flush=True)
    print("🚀 SkyFPL — Transferência ENRC LOW HD: Staging ➔ Produção Oficial (R2)", flush=True)
    print("=" * 70, flush=True)
    print(f"  Bucket: {R2_BUCKET}", flush=True)
    print("  Origem : enrc/staging/{code}_HD.mbtiles e {code}.pmtiles", flush=True)
    print("  Destino: enrc/{code}.mbtiles e enrc/{code}.pmtiles", flush=True)
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

    # 1. Carrega telemetria atual de staging
    progress_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
        progress_data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de staging '{PROGRESS_KEY}' carregada.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao carregar telemetria de staging: {e}. Prosseguindo...", flush=True)

    # 2. Carrega telemetria existente de produção (para mesclar metadados com segurança)
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
    cat_path = os.path.join(os.path.dirname(__file__), "enrc_catalog.json")
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
        target_codes = ALL_ENRC_CODES
        print("  🌐 Modo Completo: Promovendo todas as cartas disponíveis em staging.", flush=True)

    print("\n📦 ETAPA 1: Transferindo cartas (Cópia Server-Side + Limpeza de Origem)...", flush=True)
    for idx, code in enumerate(target_codes, start=1):
        candidates = [
            f"enrc/staging/{code}_HD.mbtiles",
            f"enrc/staging/{code}.mbtiles",
            f"enrc-l-test/{code}_HD.mbtiles"
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

        dst_key = f"enrc/{code}.mbtiles"

        if not src_key:
            # Se não existe em staging, verifica se já está na produção
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=dst_key)
                print(f"  [{idx:02d}/10] ℹ️ Carta {code} já está ativa em produção (sem novos arquivos em staging).", flush=True)
            except ClientError:
                print(f"  [{idx:02d}/10] ⚠️ Carta {code} não encontrada em staging nem em produção.", flush=True)
            continue

        try:
            total_bytes += size
            print(f"  [{idx:02d}/10] 🔄 Transferindo {code} ({size / (1024*1024):.2f} MB)...", end=" ", flush=True)

            # A. Copiar MBTiles para produção
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": src_key},
                Key=dst_key,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="COPY"
            )
            print("✓ MBTILES", end=" ", flush=True)

            # B. Copiar PMTiles para produção (se existir em staging)
            pmtiles_src = f"enrc/staging/{code}.pmtiles"
            has_pmtiles = False
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=pmtiles_src)
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": pmtiles_src},
                    Key=f"enrc/{code}.pmtiles",
                    ContentType="application/x-pmtiles",
                    MetadataDirective="COPY"
                )
                has_pmtiles = True
                print("+ PMTILES", end=" ", flush=True)
            except ClientError:
                pass

            # C. EXPURGO DE STAGING (Transferência Real: Delete da origem)
            keys_to_delete = [{"Key": src_key}]
            if has_pmtiles:
                keys_to_delete.append({"Key": pmtiles_src})

            s3.delete_objects(
                Bucket=R2_BUCKET,
                Delete={"Objects": keys_to_delete}
            )
            print("➔ 🗑️ ORIGEM LIMPA", flush=True)

            # Atualizar metadados acumulativos da produção
            chart_info = catalog.get(code, {})
            if code in staging_metadata:
                prod_metadata[code] = staging_metadata[code]
                prod_metadata[code]["promoted_at"] = datetime.now(timezone.utc).isoformat()
            else:
                prod_metadata[code] = {
                    "size_bytes": size,
                    "size_mb": round(size / (1024 * 1024), 2),
                    "promoted_at": datetime.now(timezone.utc).isoformat()
                }

            if not prod_metadata[code].get("name"):
                prod_metadata[code]["name"] = chart_info.get("name", code)
            if not prod_metadata[code].get("cycle"):
                prod_metadata[code]["cycle"] = airac_info.get("cycle", "2609")
            if not prod_metadata[code].get("amdt"):
                prod_metadata[code]["amdt"] = airac_info.get("cycle", "2609")
            if not prod_metadata[code].get("effective_date"):
                prod_metadata[code]["effective_date"] = airac_info.get("effective_date", "03/09/2026")

            promoted_codes.append(code)
            promoted_count += 1
        except Exception as e:
            print(f"❌ ERRO ao transferir {code}: {e}", flush=True)

    print(f"\n🎉 Transferência concluída! {promoted_count} carta(s) transferida(s) ({total_bytes / (1024*1024):.2f} MB).", flush=True)

    # 3. Atualizar o índice oficial de produção (enrcl_progress.json)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        # Normaliza metadados de todas as cartas em produção para garantir schema canônico
        for c_code, c_data in prod_metadata.items():
            c_info = catalog.get(c_code, {})
            if not c_data.get("name"):
                c_data["name"] = c_info.get("name", c_code)
            if not c_data.get("cycle"):
                c_data["cycle"] = airac_info.get("cycle", "2609")
            if not c_data.get("amdt"):
                c_data["amdt"] = airac_info.get("cycle", "2609")
            if not c_data.get("effective_date"):
                c_data["effective_date"] = airac_info.get("effective_date", "03/09/2026")

        prod_meta = {
            "status": "completed",
            "last_promotion": now_iso,
            "charts_promoted": len(prod_metadata),
            "last_transferred_codes": promoted_codes,
            "current_cycle": airac_info.get("cycle", "2609"),
            "effective_date": airac_info.get("effective_date", "03/09/2026"),
            "engine": "SkyFPL ENRC HD Engine v2.0 (GeoPDF / GDAL)",
            "metadata": prod_metadata
        }
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROD_PROGRESS_KEY,
            Body=json.dumps(prod_meta, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
        print(f"✅ Índice de produção '{PROD_PROGRESS_KEY}' atualizado com {len(prod_metadata)} cartas.", flush=True)
    except Exception as e:
        print(f"⚠️ Erro ao gravar índice de produção: {e}", flush=True)

    # 4. Limpar as cartas promovidas do índice de staging (enrcl_hd_progress.json)
    try:
        for code in promoted_codes:
            staging_metadata.pop(code, None)

        progress_data["metadata"] = staging_metadata
        progress_data["updated_at"] = datetime.now(timezone.utc).isoformat()
        progress_data["status"] = "idle"

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_data, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
        print(f"✅ Telemetria de staging '{PROGRESS_KEY}' limpa ({len(staging_metadata)} cartas restantes em quarentena).", flush=True)
    except Exception as e:
        print(f"⚠️ Erro ao atualizar telemetria de staging: {e}", flush=True)

    print("\n🏁 Processo de promoção e transferência finalizado com sucesso.", flush=True)

    # 📱 Disparo do Relatório de Promoção no Telegram
    summary_items = [
        f"<b>{c}</b> ({prod_metadata.get(c, {}).get('name', c)}): {prod_metadata.get(c, {}).get('size_mb', 0)} MB"
        for c in promoted_codes
    ]
    send_telegram_notification(
        title="Promoção ENRC LOW HD para Produção",
        status="PROMOTED",
        cycle=airac_info.get("cycle", "2609"),
        effective_date=airac_info.get("effective_date", "03/09/2026"),
        processed_items=summary_items
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as fatal_e:
        err_str = str(fatal_e)
        print(f"\n🚨 [FALHA CRÍTICA NA PROMOÇÃO] {err_str}", flush=True)
        send_telegram_notification(
            title="Promoção ENRC LOW HD",
            status="FAILED",
            error_msg=f"Falha fatal na promoção para produção: {err_str[:250]}"
        )
        raise fatal_e
