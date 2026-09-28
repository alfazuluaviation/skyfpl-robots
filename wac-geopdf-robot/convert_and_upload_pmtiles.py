#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🚀 SkyFPL — Conversão e Upload de Cartas WAC para PMTiles (Cloudflare R2)
=============================================================================
Objetivo:
1. Baixa as 46 cartas WAC HD (wac/{code}.mbtiles) diretamente do Cloudflare R2.
2. Converte cada carta para formato otimizado PMTiles v3 (específico para navegadores).
3. Faz o upload para 'wac/{code}.pmtiles' com headers de cache público e imutável.
4. Atualiza a telemetria 'wac_geopdf_progress.json' para permitir consumo pelo site.
=============================================================================
"""

import os
import sys
import json
import time
import tempfile
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone
from pmtiles.convert import mbtiles_to_pmtiles

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

def main():
    print("=" * 75, flush=True)
    print("🚀 SkyFPL — Conversão e Upload de Cartas WAC para PMTiles (Cloudflare R2)", flush=True)
    print("=" * 75, flush=True)
    print(f"  Bucket: {R2_BUCKET}", flush=True)
    print("  Origem MBTiles: wac/{code}.mbtiles", flush=True)
    print("  Destino PMTiles: wac/{code}.pmtiles", flush=True)
    print("=" * 75, flush=True)

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
        print(f"✅ Telemetria '{PROGRESS_KEY}' carregada.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao carregar telemetria: {e}. Iniciando com estrutura básica.", flush=True)

    metadata = progress_data.get("metadata", {})
    success_count = 0
    total_pmtiles_bytes = 0

    tmp_dir = tempfile.mkdtemp(prefix="wac_pmtiles_")
    print(f"\n📂 Diretório temporário de conversão: {tmp_dir}\n", flush=True)

    for idx, code in enumerate(ALL_WAC_CODES, start=1):
        mbtiles_key = f"wac/{code}.mbtiles"
        pmtiles_key = f"wac/{code}.pmtiles"

        local_mbtiles = os.path.join(tmp_dir, f"{code}.mbtiles")
        local_pmtiles = os.path.join(tmp_dir, f"{code}.pmtiles")

        t0 = time.time()
        print(f"[{idx:02d}/46] Processando {code}...", end=" ", flush=True)

        try:
            # 1. Download do MBTiles a partir do R2
            s3.download_file(R2_BUCKET, mbtiles_key, local_mbtiles)
            mbtiles_size = os.path.getsize(local_mbtiles)

            # 2. Conversão MBTiles -> PMTiles
            mbtiles_to_pmtiles(local_mbtiles, local_pmtiles, maxzoom=12)
            pmtiles_size = os.path.getsize(local_pmtiles)
            total_pmtiles_bytes += pmtiles_size

            # 3. Upload do PMTiles para o R2 com cabeçalhos de alta performance
            with open(local_pmtiles, "rb") as f:
                s3.put_object(
                    Bucket=R2_BUCKET,
                    Key=pmtiles_key,
                    Body=f,
                    ContentType="application/vnd.pmtiles",
                    CacheControl="public, max-age=2592000, immutable"
                )

            duration = time.time() - t0
            print(f"✅ Concluído em {duration:.1f}s | MBTiles: {mbtiles_size/1024/1024:.1f} MB ➔ PMTiles: {pmtiles_size/1024/1024:.1f} MB", flush=True)

            if code not in metadata:
                metadata[code] = {}
            metadata[code]["pmtiles_key"] = pmtiles_key
            metadata[code]["pmtiles_size_bytes"] = pmtiles_size
            metadata[code]["pmtiles_updated_at"] = datetime.now(timezone.utc).isoformat()

            success_count += 1

        except ClientError as e:
            print(f"❌ Erro de R2 em {code}: {e}", flush=True)
        except Exception as e:
            print(f"❌ Erro na conversão de {code}: {e}", flush=True)
        finally:
            if os.path.exists(local_mbtiles):
                os.remove(local_mbtiles)
            if os.path.exists(local_pmtiles):
                os.remove(local_pmtiles)

    print("\n" + "=" * 75, flush=True)
    print(f"✨ Concluído: {success_count}/46 cartas WAC convertidas para PMTiles!", flush=True)
    print(f"📦 Tamanho total dos PMTiles: {total_pmtiles_bytes / 1024 / 1024:.2f} MB", flush=True)
    print("=" * 75, flush=True)

    # 4. Atualizar telemetria no R2
    print("\n📝 Atualizando telemetria 'wac_geopdf_progress.json' no R2...", flush=True)
    progress_data["metadata"] = metadata
    progress_data["pmtiles_ready"] = (success_count == len(ALL_WAC_CODES))
    progress_data["pmtiles_updated_at"] = datetime.now(timezone.utc).isoformat()
    if "logs" not in progress_data:
        progress_data["logs"] = []
    progress_data["logs"].insert(0, f"[{datetime.now().strftime('%H:%M:%S')}] 🌐 46 cartas PMTiles geradas e disponibilizadas para o site web.")

    try:
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        print(f"✅ Telemetria '{PROGRESS_KEY}' atualizada com sucesso no R2!", flush=True)
    except Exception as e:
        print(f"⚠️ Erro ao atualizar telemetria: {e}", flush=True)

if __name__ == "__main__":
    main()
