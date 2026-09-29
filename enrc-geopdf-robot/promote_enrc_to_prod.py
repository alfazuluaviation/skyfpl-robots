#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Script de Promoção das Cartas ENRC LOW HD para Produção (R2)
=============================================================================
Objetivo:
1. Copia server-side as 9 folhas ENRC L + Brasil Full de 'enrc/staging/{code}_HD.mbtiles'
   para 'enrc/{code}.mbtiles' (sobrescrevendo com qualidade HD sem download/upload local).
2. Copia simultaneamente 'enrc/staging/{code}.pmtiles' para 'enrc/{code}.pmtiles' para a Web.
3. Atualiza os cabeçalhos MIME e sincroniza o índice 'enrcl_progress.json'.
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
    print("🚀 SkyFPL — Promoção das Cartas ENRC LOW HD para Produção (R2)", flush=True)
    print("=" * 70, flush=True)
    print(f"  Bucket: {R2_BUCKET}", flush=True)
    print("  Origem: enrc/staging/{code}_HD.mbtiles e {code}.pmtiles", flush=True)
    print("  Destino: enrc/{code}.mbtiles e enrc/{code}.pmtiles", flush=True)
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

    # Carrega telemetria atual
    progress_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
        progress_data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria '{PROGRESS_KEY}' carregada com sucesso.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao carregar telemetria: {e}. Prosseguindo...", flush=True)

    promoted_count = 0
    total_bytes = 0

    print("\n📦 ETAPA 1: Copiando server-side de 'enrc/staging/' para 'enrc/'...", flush=True)
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
            print(f"  [{idx:02d}/10] ⚠️ Arquivo MBTiles de origem não encontrado em staging para {code}", flush=True)
            continue

        try:
            total_bytes += size
            print(f"  [{idx:02d}/10] 🔄 Promovendo MBTiles {code} ({size / (1024*1024):.2f} MB)...", end=" ", flush=True)

            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource={"Bucket": R2_BUCKET, "Key": src_key},
                Key=dst_key,
                ContentType="application/vnd.sqlite3",
                MetadataDirective="COPY"
            )
            print("✓ MBTILES OK", end=" ", flush=True)

            # Promover também PMTiles se existir
            pmtiles_src = f"enrc/staging/{code}.pmtiles"
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=pmtiles_src)
                s3.copy_object(
                    Bucket=R2_BUCKET,
                    CopySource={"Bucket": R2_BUCKET, "Key": pmtiles_src},
                    Key=f"enrc/{code}.pmtiles",
                    ContentType="application/x-pmtiles",
                    MetadataDirective="COPY"
                )
                print("+ PMTILES OK", flush=True)
            except ClientError:
                print("(Sem PMTiles em staging)", flush=True)

            promoted_count += 1
        except Exception as e:
            print(f"❌ ERRO ao promover {code}: {e}", flush=True)

    print(f"\n🎉 Promoção concluída! {promoted_count}/{len(ALL_ENRC_CODES)} cartas promovidas ({total_bytes / (1024*1024):.2f} MB).", flush=True)

    # Atualiza arquivo de telemetria da produção
    try:
        prod_meta = {
            "status": "completed",
            "last_promotion": datetime.now(timezone.utc).isoformat(),
            "charts_promoted": promoted_count,
            "engine": "SkyFPL ENRC HD Engine v2.0 (GeoPDF / GDAL)",
            "metadata": progress_data.get("metadata", {})
        }
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROD_PROGRESS_KEY,
            Body=json.dumps(prod_meta, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store"
        )
        print(f"✅ Índice de produção '{PROD_PROGRESS_KEY}' atualizado com sucesso.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao gravar índice de produção: {e}", flush=True)

if __name__ == "__main__":
    main()
