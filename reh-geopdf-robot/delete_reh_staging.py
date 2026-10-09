#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=============================================================================
🗑️ SkyFPL — Script de Exclusão de Setores REH em Quarentena / Staging (R2)
=============================================================================
Objetivo:
1. Exclusão cirúrgica de setores REH específicos presentes em 'reh/staging/'.
2. Remove MBTiles e PMTiles correspondentes da quarentena no Cloudflare R2.
3. Remove os metadados do setor do índice de quarentena 'reh_hd_progress.json'.
4. Registra log de auditoria da ação administrativa.
=============================================================================
"""

import os
import sys
import json
try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    boto3 = None
    ClientError = Exception
from datetime import datetime, timezone

R2_ENDPOINT = os.environ.get("R2_ENDPOINT") or os.environ.get("CLOUDFLARE_R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY") or os.environ.get("R2_ACCESS_KEY_ID") or os.environ.get("CLOUDFLARE_R2_ACCESS_KEY_ID", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY") or os.environ.get("R2_SECRET_ACCESS_KEY") or os.environ.get("CLOUDFLARE_R2_SECRET_ACCESS_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET") or os.environ.get("CLOUDFLARE_R2_BUCKET", "skyfpl-charts")
PROGRESS_KEY = "reh_hd_progress.json"

def main():
    target_env = os.environ.get("TARGET_CODES", "").strip()
    if len(sys.argv) > 1 and not target_env:
        target_env = sys.argv[1].strip()

    if not target_env:
        print("❌ ERRO: Nenhum setor REH especificado em TARGET_CODES para exclusão.", flush=True)
        sys.exit(1)

    target_codes = [c.strip().upper() for c in target_env.split(",") if c.strip()]

    print("=" * 72, flush=True)
    print("🗑️ SkyFPL — Exclusão de Setores REH em Quarentena (reh/staging/)", flush=True)
    print("=" * 72, flush=True)
    print(f"  Bucket  : {R2_BUCKET}", flush=True)
    print(f"  Setores : {', '.join(target_codes)}", flush=True)
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

    deleted_count = 0
    for code in target_codes:
        keys_to_delete = [
            f"reh/staging/{code}_HD.mbtiles",
            f"reh/staging/{code}.mbtiles",
            f"reh/staging/{code}.pmtiles"
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
            print(f"  ✓ {code}: {len(existing_to_delete)} arquivo(s) expurgado(s) de reh/staging/", flush=True)
            deleted_count += 1
        else:
            print(f"  ℹ️ {code}: Nenhum arquivo físico encontrado em reh/staging/", flush=True)

    # Atualizar reh_hd_progress.json
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
        logs.append(f"[{now_str}] [ADMIN] {len(target_codes)} setor(es) ({', '.join(target_codes)}) excluído(s) de reh/staging/ pelo operador.")
        progress_data["logs"] = logs[-300:]

        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        print(f"  ✓ {PROGRESS_KEY} atualizado no R2.", flush=True)
    except ClientError as e:
        print(f"  ⚠️ Aviso ao atualizar {PROGRESS_KEY}: {e}", flush=True)

    print("=" * 72, flush=True)
    print(f"🎉 Exclusão concluída: {deleted_count} setor(es) removido(s) com sucesso.", flush=True)
    print("=" * 72, flush=True)

if __name__ == "__main__":
    main()
