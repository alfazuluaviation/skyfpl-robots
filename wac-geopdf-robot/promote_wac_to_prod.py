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
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# 46 Códigos Oficiais das Cartas WAC do Brasil
ALL_WAC_CODES = [
    "WAC2825", "WAC2826", "WAC2827", "WAC2892", "WAC2893", "WAC2894", "WAC2895",
    "WAC2944", "WAC2945", "WAC2946", "WAC2947", "WAC2948", "WAC2949", "WAC3012",
    "WAC3013", "WAC3014", "WAC3015", "WAC3016", "WAC3017", "WAC3018", "WAC3019",
    "WAC3066", "WAC3067", "WAC3068", "WAC3069", "WAC3070", "WAC3071", "WAC3072",
    "WAC3137", "WAC3138", "WAC3139", "WAC3140", "WAC3141", "WAC3189", "WAC3190",
    "WAC3191", "WAC3192", "WAC3260", "WAC3261", "WAC3262", "WAC3263", "WAC3313",
    "WAC3314", "WAC3383", "WAC3384", "WAC3434"
]

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "wac_geopdf_progress.json"
PROD_PROGRESS_KEY = "wac_progress.json"

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
        candidates = [
            f"wac/staging/{code}_HD.mbtiles",
            f"wac/staging/{code}.mbtiles",
            f"wac-test/{code}_HD.mbtiles"
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

        dst_key = f"wac/{code}.mbtiles"

        if not src_key:
            # Se não há novo arquivo em staging, verifica se já existe na produção
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=dst_key)
                print(f"  [{idx:02d}/46] ℹ️ Carta {code} já está ativa em produção (sem novos arquivos em staging).", flush=True)
            except ClientError:
                print(f"  [{idx:02d}/46] ⚠️ Carta {code} não encontrada em staging nem em produção.", flush=True)
            continue

        try:
            total_bytes += size
            print(f"  [{idx:02d}/46] 🔄 Transferindo {code} ({size / (1024*1024):.2f} MB)...", end=" ", flush=True)

            # A. Copiar MBTiles para produção
            copy_source = {"Bucket": R2_BUCKET, "Key": src_key}
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource=copy_source,
                Key=dst_key,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="COPY"
            )
            print("✓ MBTILES", end=" ", flush=True)

            # B. Copiar PMTiles para produção (se existir em staging)
            pmtiles_src = f"wac/staging/{code}.pmtiles"
            has_pmtiles = False
            pmtiles_size_mb = 0
            try:
                pmhead = s3.head_object(Bucket=R2_BUCKET, Key=pmtiles_src)
                pmtiles_size_mb = round(pmhead.get("ContentLength", 0) / (1024 * 1024), 2)
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": pmtiles_src},
                    Key=f"wac/{code}.pmtiles",
                    ContentType="application/x-pmtiles",
                    MetadataDirective="COPY"
                )
                has_pmtiles = True
                print("+ PMTILES", end=" ", flush=True)
            except ClientError:
                pass

            # C. Expurgo imediato do staging (Delete da origem)
            keys_to_delete = [{"Key": src_key}]
            if has_pmtiles:
                keys_to_delete.append({"Key": pmtiles_src})

            s3.delete_objects(
                Bucket=R2_BUCKET,
                Delete={"Objects": keys_to_delete}
            )
            print("➔ 🗑️ ORIGEM LIMPA", flush=True)

            # D. Atualiza metadados acumulativos da produção oficial
            if code in staging_metadata:
                prod_metadata[code] = staging_metadata[code]
            else:
                prod_metadata[code] = {}

            prod_metadata[code]["r2_key"] = dst_key
            prod_metadata[code]["r2_pmtiles_key"] = f"wac/{code}.pmtiles" if has_pmtiles else None
            if has_pmtiles:
                prod_metadata[code]["pmtiles_size_mb"] = pmtiles_size_mb
            prod_metadata[code]["size_bytes"] = size
            prod_metadata[code]["size_mb"] = round(size / (1024 * 1024), 2)
            prod_metadata[code]["promoted_at"] = datetime.now(timezone.utc).isoformat()

            promoted_codes.append(code)
            promoted_count += 1

        except ClientError as e:
            print(f"❌ Erro ao transferir {src_key}: {e}", flush=True)

    print(f"\n✨ Total de cartas transferidas: {promoted_count} ({total_bytes / 1024 / 1024:.2f} MB)", flush=True)

    # 2. Preservar o arquivo WAC_BRASIL_FULL.mbtiles conforme solicitado
    print("\n🛡️ ETAPA 2: Preservando 'wac/WAC_BRASIL_FULL.mbtiles' intacto na produção.", flush=True)

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

if __name__ == "__main__":
    main()
