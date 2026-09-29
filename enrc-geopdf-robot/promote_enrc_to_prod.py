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
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

ALL_ENRC_CODES = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "FULL"]

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "enrcl_hd_progress.json"
PROD_PROGRESS_KEY = "enrcl_progress.json"

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

    print("\n📦 ETAPA 1: Transferindo cartas (Cópia Server-Side + Limpeza de Origem)...", flush=True)
    for idx, code in enumerate(ALL_ENRC_CODES, start=1):
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
            if code in staging_metadata:
                prod_metadata[code] = staging_metadata[code]
                prod_metadata[code]["promoted_at"] = datetime.now(timezone.utc).isoformat()
            else:
                prod_metadata[code] = {
                    "size_bytes": size,
                    "size_mb": f"{size / (1024*1024):.2f}",
                    "promoted_at": datetime.now(timezone.utc).isoformat()
                }

            promoted_codes.append(code)
            promoted_count += 1
        except Exception as e:
            print(f"❌ ERRO ao transferir {code}: {e}", flush=True)

    print(f"\n🎉 Transferência concluída! {promoted_count} carta(s) transferida(s) ({total_bytes / (1024*1024):.2f} MB).", flush=True)

    # 3. Atualizar o índice oficial de produção (enrcl_progress.json)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        prod_meta = {
            "status": "completed",
            "last_promotion": now_iso,
            "charts_promoted": len(prod_metadata),
            "last_transferred_codes": promoted_codes,
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

if __name__ == "__main__":
    main()
