#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🗑️ SkyFPL — Script de Exclusão de Cartas WAC em Quarentena / Staging (R2)
=============================================================================
Objetivo:
1. Exclusão permanente de cartas específicas presentes em 'wac/staging/'
2. Remove MBTiles e PMTiles correspondentes da quarentena no Cloudflare R2
3. Remove os metadados da carta do índice 'wac_geopdf_progress.json'
=============================================================================
"""

import os
import sys
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "wac_geopdf_progress.json"

def main():
    target_env = os.environ.get("TARGET_CODES", "").strip()
    if len(sys.argv) > 1 and not target_env:
        target_env = sys.argv[1].strip()

    if not target_env:
        print("❌ ERRO: Nenhuma carta especificada em TARGET_CODES para exclusão.", flush=True)
        sys.exit(1)

    target_codes = [c.strip().upper() for c in target_env.split(",") if c.strip()]

    print("=" * 70, flush=True)
    print("🗑️ SkyFPL — Exclusão de Cartas WAC em Quarentena (wac/staging/)", flush=True)
    print("=" * 70, flush=True)
    print(f"  Bucket : {R2_BUCKET}", flush=True)
    print(f"  Cartas : {', '.join(target_codes)}", flush=True)
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

    deleted_count = 0
    for code in target_codes:
        keys_to_delete = [
            f"wac/staging/{code}_HD.mbtiles",
            f"wac/staging/{code}.mbtiles",
            f"wac/staging/{code}.pmtiles",
            f"wac-test/{code}_HD.mbtiles"
        ]
        
        existing_to_delete = []
        for k in keys_to_delete:
            try:
                s3.head_object(Bucket=R2_BUCKET, Key=k)
                existing_to_delete.append({"Key": k})
            except ClientError:
                pass

        if existing_to_delete:
            s3.delete_objects(Bucket=R2_BUCKET, Delete={"Objects": existing_to_delete})
            print(f"  ✓ {code}: {len(existing_to_delete)} arquivo(s) expurgado(s) de wac/staging/", flush=True)
            deleted_count += 1
        else:
            print(f"  ℹ️ {code}: Nenhum arquivo físico encontrado em wac/staging/", flush=True)

    # Atualizar wac_geopdf_progress.json
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
        progress_data = json.loads(resp["Body"].read().decode("utf-8"))
        staging_metadata = progress_data.get("metadata", {})
        
        for code in target_codes:
            staging_metadata.pop(code, None)

        progress_data["metadata"] = staging_metadata
        progress_data["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        logs = progress_data.get("logs", [])
        now_str = datetime.now(timezone.utc).strftime("%H:%M:%S")
        logs.append(f"[{now_str}] [ADMIN] {len(target_codes)} carta(s) ({', '.join(target_codes)}) excluída(s) de wac/staging/ pelo operador.")
        progress_data["logs"] = logs[-300:]

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_data, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        print(f"✅ Índice '{PROGRESS_KEY}' atualizado com sucesso no R2.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao atualizar telemetria '{PROGRESS_KEY}': {e}", flush=True)

    print("=" * 70, flush=True)
    print(f"✨ Operação concluída. {deleted_count} carta(s) processada(s).", flush=True)
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
