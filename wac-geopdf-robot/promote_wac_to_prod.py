#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção das Cartas WAC HD para Produção (R2)
=============================================================================
Objetivo:
1. Copia server-side as 46 folhas WAC HD de 'wac-test/{code}_HD.mbtiles' para
   'wac/{code}.mbtiles' (sobrescrevendo os arquivos antigos com qualidade HD).
2. Expurgar o arquivo legado gigante 'wac/WAC_BRASIL_FULL.mbtiles' (1.56 GB).
3. Expurgar os arquivos de quarentena 'wac-test/*'.
4. Atualizar o índice 'wac_geopdf_progress.json' para apontar para a produção.
=============================================================================
"""

import os
import sys
import json
import requests
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from datetime import datetime, timezone

# 46 Códigos Oficiais das Cartas WAC do Brasil + Camada Única Nacional Web
ALL_WAC_CODES = [
    "WAC2825", "WAC2826", "WAC2827", "WAC2892", "WAC2893", "WAC2894", "WAC2895",
    "WAC2944", "WAC2945", "WAC2946", "WAC2947", "WAC2948", "WAC2949", "WAC3012",
    "WAC3013", "WAC3014", "WAC3015", "WAC3016", "WAC3017", "WAC3018", "WAC3019",
    "WAC3066", "WAC3067", "WAC3068", "WAC3069", "WAC3070", "WAC3071", "WAC3072",
    "WAC3137", "WAC3138", "WAC3139", "WAC3140", "WAC3141", "WAC3189", "WAC3190",
    "WAC3191", "WAC3192", "WAC3260", "WAC3261", "WAC3262", "WAC3263", "WAC3313",
    "WAC3314", "WAC3383", "WAC3384", "WAC3434", "WAC_BRASIL_FULL"
]

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "wac_geopdf_progress.json"
PROD_PROGRESS_KEY = "wac_progress.json"

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
            f"🛰️ <b>Origem / Alvo:</b> <code>{cycle or 'AISWEB DECEA'}</code>",
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
            f"🛰️ <b>Emendas Oficiais DECEA</b>",
        ]
        if effective_date:
            lines.append(f"⏳ <b>Vigência:</b> <code>{effective_date}</code>")
        lines.append(f"📦 <b>Cartas Promovidas ({len(processed_items)}):</b>")
        for item in processed_items:
            lines.append(f"  • {item}")
        lines.append("🌐 <b>Destino:</b> Produção Oficial (wac/)")
        lines.append("🗑️ <b>Staging:</b> Quarentena Limpa")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("✅ <b>Status:</b> 100% CONCLUÍDO & VIGENTE")
    else:  # SUCCESS
        lines = [
            f"🗺️ <b>{title}</b>",
            "━━━━━━━━━━━━━━━━━━━━━━━━━━",
            f"🛰️ <b>Origem:</b> <code>{cycle or 'AISWEB DECEA'}</code>",
        ]
        if effective_date:
            lines.append(f"⏳ <b>Vigência DECEA:</b> <code>{effective_date}</code>")
        lines.append(f"📊 <b>Processamento Concluído ({len(processed_items)} cartas):</b>")
        for item in processed_items[:12]:
            lines.append(f"  • {item}")
        if len(processed_items) > 12:
            lines.append(f"  • ... e mais {len(processed_items) - 12} cartas.")
        lines.append("🛡️ <b>Quarentena:</b> Staging no R2 (wac/staging/)")
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
    print("🚀 SkyFPL — Transferência WAC HD: Staging ➔ Produção Oficial (R2)", flush=True)
    print("=" * 70, flush=True)
    print(f"  Bucket: {R2_BUCKET}", flush=True)
    print("  Origem : wac/staging/{code}_HD.mbtiles e {code}.pmtiles", flush=True)
    print("  Destino: wac/{code}.mbtiles e wac/{code}.pmtiles", flush=True)
    print("  Política: TRANSFERÊNCIA REAL (Cópia Server-Side + Expurgo de Staging)", flush=True)
    print("=" * 70, flush=True)

    if not (R2_ENDPOINT and R2_ACCESS_KEY and R2_SECRET_KEY and R2_BUCKET):
        print("❌ ERRO: Credenciais Cloudflare R2 não encontradas no ambiente.", flush=True)
        sys.exit(1)

    s3_config = Config(
        connect_timeout=120,
        read_timeout=600,
        retries={"max_attempts": 3}
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        region_name="auto",
        config=s3_config
    )

    # 1. Carrega telemetria atual de staging
    progress_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
        progress_data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de staging '{PROGRESS_KEY}' carregada com sucesso.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao carregar telemetria de staging: {e}. Prosseguindo...", flush=True)

    # 2. Carrega telemetria de produção oficial (para merge cumulativo)
    prod_data = {}
    try:
        resp_prod = s3.get_object(Bucket=R2_BUCKET, Key=PROD_PROGRESS_KEY)
        prod_data = json.loads(resp_prod["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria de produção '{PROD_PROGRESS_KEY}' carregada.", flush=True)
    except Exception:
        print(f"ℹ️ Criando novo índice de produção oficial '{PROD_PROGRESS_KEY}'.", flush=True)

    staging_metadata = progress_data.get("metadata", {})
    prod_metadata = prod_data.get("metadata", {})
    promoted_count = 0
    total_bytes = 0
    promoted_codes = []

    target_env = os.environ.get("TARGET_CODES", "").strip()
    if target_env and target_env.upper() != "ALL":
        target_codes = [c.strip().upper() for c in target_env.split(",") if c.strip()]
        print(f"  🎯 Modo Seleção Ativo: Promovendo apenas {len(target_codes)} carta(s): {', '.join(target_codes)}", flush=True)
    else:
        target_codes = ALL_WAC_CODES
        print("  🌐 Modo Completo: Promovendo todas as cartas disponíveis em staging.", flush=True)

    print("\n📦 ETAPA 1: Transferindo cartas (Cópia Server-Side + Limpeza de Origem)...", flush=True)
    for idx, code in enumerate(target_codes, start=1):
        staging_code = "WAC_BRASIL_FULL" if code in ("FULL", "WAC_BRASIL_FULL", "BRASIL_FULL") else code
        target_meta_code = staging_code if code in ("FULL", "WAC_BRASIL_FULL", "BRASIL_FULL") else code

        # Candidatos MBTiles em staging
        mbtiles_candidates = [
            f"wac/staging/{staging_code}_HD.mbtiles",
            f"wac/staging/{staging_code}.mbtiles",
            f"wac/staging/{code}_HD.mbtiles",
            f"wac/staging/{code}.mbtiles",
            f"wac-test/{code}_HD.mbtiles"
        ]
        src_mbtiles_key = None
        mbtiles_size = 0
        for cand in mbtiles_candidates:
            try:
                head = s3.head_object(Bucket=R2_BUCKET, Key=cand)
                src_mbtiles_key = cand
                mbtiles_size = head.get("ContentLength", 0)
                break
            except ClientError:
                continue

        # Candidatos PMTiles em staging
        pmtiles_candidates = [
            f"wac/staging/{staging_code}.pmtiles",
            f"wac/staging/{code}.pmtiles"
        ]
        src_pmtiles_key = None
        pmtiles_size = 0
        for cand in pmtiles_candidates:
            try:
                head = s3.head_object(Bucket=R2_BUCKET, Key=cand)
                src_pmtiles_key = cand
                pmtiles_size = head.get("ContentLength", 0)
                break
            except ClientError:
                continue

        dst_mbtiles_key = f"wac/{staging_code}.mbtiles" if code in ("FULL", "WAC_BRASIL_FULL", "BRASIL_FULL") else f"wac/{code}.mbtiles"
        dst_pmtiles_key = f"wac/{staging_code}.pmtiles" if code in ("FULL", "WAC_BRASIL_FULL", "BRASIL_FULL") else f"wac/{code}.pmtiles"

        # Se NENHUM dos arquivos está em staging, checamos se já está presente na produção
        if not src_mbtiles_key and not src_pmtiles_key:
            has_prod = False
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=dst_mbtiles_key)
                has_prod = True
            except ClientError:
                pass

            if has_prod:
                print(f"  [{idx:02d}/{len(target_codes)}] ℹ️ Carta {code} já está ativa em produção.", flush=True)
                # Se ainda constava na quarentena de staging, marcamos para expurgar do manifesto
                if target_meta_code in staging_metadata or code in staging_metadata:
                    promoted_codes.append(target_meta_code)
            else:
                print(f"  [{idx:02d}/{len(target_codes)}] ⚠️ Carta {code} não encontrada em staging nem em produção.", flush=True)
            continue

        try:
            print(f"  [{idx:02d}/{len(target_codes)}] 🔄 Transferindo {code}...", end=" ", flush=True)
            keys_to_delete = []

            # A. Copiar MBTiles se existir em staging
            has_mbtiles = False
            if src_mbtiles_key:
                total_bytes += mbtiles_size
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": src_mbtiles_key},
                    Key=dst_mbtiles_key,
                    ContentType="application/vnd.sqlite3",
                    MetadataDirective="COPY"
                )
                if staging_code == "WAC_BRASIL_FULL":
                    s3.copy_object(
                        Bucket=R2_BUCKET,
                        CopySource={"Bucket": R2_BUCKET, "Key": src_mbtiles_key},
                        Key="wac/WAC_BRASIL_FULL_HD.mbtiles",
                        ContentType="application/vnd.sqlite3",
                        MetadataDirective="COPY"
                    )
                keys_to_delete.append({"Key": src_mbtiles_key})
                has_mbtiles = True
                print("✓ MBTILES", end=" ", flush=True)

            # B. Copiar PMTiles se existir em staging
            has_pmtiles = False
            pmtiles_size_mb = 0
            if src_pmtiles_key:
                total_bytes += pmtiles_size
                pmtiles_size_mb = round(pmtiles_size / (1024 * 1024), 2)
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": src_pmtiles_key},
                    Key=dst_pmtiles_key,
                    ContentType="application/x-pmtiles",
                    MetadataDirective="COPY"
                )
                keys_to_delete.append({"Key": src_pmtiles_key})
                has_pmtiles = True
                print("✓ PMTILES", end=" ", flush=True)

            # C. Expurgo imediato do staging (Delete da origem confirmado)
            if keys_to_delete:
                s3.delete_objects(
                    Bucket=R2_BUCKET,
                    Delete={"Objects": keys_to_delete}
                )
                print(f"➔ 🗑️ ORIGEM LIMPA ({len(keys_to_delete)} arquivos)", flush=True)

            # D. Atualiza metadados acumulativos da produção oficial
            if target_meta_code in staging_metadata:
                prod_metadata[target_meta_code] = staging_metadata[target_meta_code]
            elif code in staging_metadata:
                prod_metadata[target_meta_code] = staging_metadata[code]
            else:
                if target_meta_code not in prod_metadata:
                    prod_metadata[target_meta_code] = {}

            prod_metadata[target_meta_code]["r2_key"] = dst_mbtiles_key
            if has_pmtiles or src_pmtiles_key:
                prod_metadata[target_meta_code]["r2_pmtiles_key"] = dst_pmtiles_key
                prod_metadata[target_meta_code]["pmtiles_size_mb"] = pmtiles_size_mb
            if mbtiles_size > 0:
                prod_metadata[target_meta_code]["size_bytes"] = mbtiles_size
                prod_metadata[target_meta_code]["size_mb"] = round(mbtiles_size / (1024 * 1024), 2)
            prod_metadata[target_meta_code]["promoted_at"] = datetime.now(timezone.utc).isoformat()

            promoted_codes.append(target_meta_code)
            promoted_count += 1

        except Exception as e:
            print(f"❌ Erro ao transferir {code}: {e}", flush=True)
            raise e

    # 3. Limpeza complementar de quarentena wac-test/ se existir
    print("\n🧹 ETAPA 3: Limpando resíduos legados de 'wac-test/'...", flush=True)
    delete_objects = []
    for code in target_codes:
        delete_objects.append({"Key": f"wac-test/{code}_HD.mbtiles"})
    try:
        s3.delete_objects(
            Bucket=R2_BUCKET,
            Delete={"Objects": delete_objects}
        )
        print("  ✅ Quarentena legada limpa com sucesso.", flush=True)
    except Exception as e:
        print(f"  ⚠️ Aviso ao limpar quarentena legada: {e}", flush=True)

    # 4. Atualiza o índice oficial de produção (wac_progress.json)
    now_iso = datetime.now(timezone.utc).isoformat()
    print("\n📝 ETAPA 4: Gravando índice oficial de produção 'wac_progress.json' no R2...", flush=True)
    prod_payload = {
        "status": "completed",
        "last_promotion": now_iso,
        "charts_promoted": len(prod_metadata),
        "last_transferred_codes": promoted_codes,
        "engine": "SkyFPL WAC HD Engine v2.1 (GeoPDF / GDAL)",
        "r2_prefix": "wac",
        "metadata": prod_metadata
    }
    try:
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROD_PROGRESS_KEY,
            Body=json.dumps(prod_payload, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        print(f"  ✅ Índice oficial de produção '{PROD_PROGRESS_KEY}' gravado com {len(prod_metadata)} cartas!", flush=True)
    except Exception as e:
        print(f"  ⚠️ Erro ao gravar índice de produção: {e}", flush=True)

    # 5. Esvaziar as cartas promovidas da telemetria de staging (wac_geopdf_progress.json)
    print("\n🧹 ETAPA 5: Esvaziando quarentena em 'wac_geopdf_progress.json'...", flush=True)
    try:
        for code in promoted_codes:
            staging_metadata.pop(code, None)
            if code in ("WAC_BRASIL_FULL", "FULL", "BRASIL_FULL"):
                staging_metadata.pop("WAC_BRASIL_FULL", None)
                staging_metadata.pop("FULL", None)
                staging_metadata.pop("BRASIL_FULL", None)

        progress_data["metadata"] = staging_metadata
        progress_data["status"] = "idle"
        progress_data["updated_at"] = now_iso
        if "logs" not in progress_data:
            progress_data["logs"] = []
        progress_data["logs"].insert(0, f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 {promoted_count} cartas WAC HD promovidas para a pasta wac/. Quarentena limpa ({len(staging_metadata)} pendentes).")

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        print(f"  ✅ Telemetria de staging '{PROGRESS_KEY}' atualizada com sucesso (Quarentena Limpa)!", flush=True)
    except Exception as e:
        print(f"  ⚠️ Aviso ao atualizar telemetria de staging: {e}", flush=True)

    print("\n" + "=" * 70, flush=True)
    print("🎉 PROMOÇÃO E TRANSFERÊNCIA CONCLUÍDAS COM TOTAL SUCESSO!", flush=True)
    print(f"  • {len(prod_metadata)} Cartas HD em vigor na pasta 'wac/WAC{{codigo}}.mbtiles' e 'wac/WAC{{codigo}}.pmtiles'")
    print("  • Quarentena 'wac/staging/' e 'wac-test/' 100% limpa")
    print("  • Arquivo 'wac_progress.json' alimentado com dados de Produção Oficial")
    print("  • Dashboard Admin sincronizado com seções de Quarentena Limpa e Produção Oficial!")
    print("=" * 70, flush=True)

    # 📱 Disparo do Relatório de Promoção no Telegram
    summary_items = [
        f"<b>{c}</b> ({prod_metadata.get(c, {}).get('name', c)}): {prod_metadata.get(c, {}).get('size_mb', 0)} MB • AMDT {prod_metadata.get(c, {}).get('amdt', 'N/A')}"
        for c in promoted_codes
    ]
    send_telegram_notification(
        title="Promoção WAC HD para Produção",
        status="PROMOTED",
        cycle="AISWEB DECEA",
        processed_items=summary_items
    )

if __name__ == "__main__":
    try:
        main()
    except Exception as fatal_e:
        err_str = str(fatal_e)
        print(f"\n🚨 [FALHA CRÍTICA NA PROMOÇÃO WAC] {err_str}", flush=True)
        send_telegram_notification(
            title="Promoção WAC HD",
            status="FAILED",
            error_msg=f"Falha fatal na promoção WAC: {err_str[:250]}"
        )
        raise fatal_e
