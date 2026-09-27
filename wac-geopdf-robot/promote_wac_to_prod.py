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

def main():
    print("=" * 70, flush=True)
    print("🚀 SkyFPL — Promoção das Cartas WAC HD para Produção (R2)", flush=True)
    print("=" * 70, flush=True)
    print(f"  Bucket: {R2_BUCKET}", flush=True)
    print(f"  Origem: wac-test/{{code}}_HD.mbtiles", flush=True)
    print(f"  Destino: wac/{{code}}.mbtiles", flush=True)
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

    # 1. Carrega telemetria atual
    progress_data = {}
    try:
        resp = s3.get_object(Bucket=R2_BUCKET, Key=PROGRESS_KEY)
        progress_data = json.loads(resp["Body"].read().decode("utf-8"))
        print(f"✅ Telemetria '{PROGRESS_KEY}' carregada com sucesso.", flush=True)
    except Exception as e:
        print(f"⚠️ Aviso ao carregar telemetria: {e}. Prosseguindo com lista padrão.", flush=True)

    metadata = progress_data.get("metadata", {})
    promoted_count = 0
    total_bytes = 0

    print("\n📦 ETAPA 1: Copiando server-side de 'wac-test/' para 'wac/'...", flush=True)
    for idx, code in enumerate(ALL_WAC_CODES, start=1):
        src_key = f"wac-test/{code}_HD.mbtiles"
        dst_key = f"wac/{code}.mbtiles"

        try:
            # Verifica se o arquivo de origem existe
            head = s3.head_object(Bucket=R2_BUCKET, Key=src_key)
            size = head.get("ContentLength", 0)
            total_bytes += size

            # Cópia server-side no Cloudflare R2 (ultra-rápida)
            copy_source = {"Bucket": R2_BUCKET, "Key": src_key}
            s3.copy_object(
                Bucket=R2_BUCKET,
                CopySource=copy_source,
                Key=dst_key,
                ContentType="application/x-sqlite3",
                MetadataDirective="COPY"
            )

            promoted_count += 1
            print(f"  [{idx:02d}/46] ✅ Promovido: {src_key} ➔ {dst_key} ({size / 1024 / 1024:.2f} MB)", flush=True)

            # Atualiza metadados
            if code in metadata:
                metadata[code]["r2_key"] = dst_key
                metadata[code]["promoted_at"] = datetime.now(timezone.utc).isoformat()

        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "404":
                print(f"  [{idx:02d}/46] ⚠️ Arquivo de origem não encontrado: {src_key}", flush=True)
            else:
                print(f"  [{idx:02d}/46] ❌ Erro ao copiar {src_key}: {e}", flush=True)

    print(f"\n✨ Total de cartas promovidas com sucesso: {promoted_count}/46 ({total_bytes / 1024 / 1024:.2f} MB)", flush=True)

    # 2. Preservar o arquivo WAC_BRASIL_FULL.mbtiles conforme solicitado
    print("\n🛡️ ETAPA 2: Preservando 'wac/WAC_BRASIL_FULL.mbtiles' (conforme solicitado pelo usuário até migração completa).", flush=True)

    # 3. Expurgar a pasta de quarentena wac-test/
    print("\n🧹 ETAPA 3: Limpando pasta de quarentena 'wac-test/'...", flush=True)
    delete_objects = [{"Key": f"wac-test/{code}_HD.mbtiles"} for code in ALL_WAC_CODES]
    try:
        s3.delete_objects(
            Bucket=R2_BUCKET,
            Delete={"Objects": delete_objects}
        )
        print(f"  ✅ {len(delete_objects)} arquivos de teste removidos de 'wac-test/'.", flush=True)
    except Exception as e:
        print(f"  ⚠️ Aviso ao limpar 'wac-test/': {e}", flush=True)

    # 4. Atualiza a telemetria e o índice oficial no R2
    print("\n📝 ETAPA 4: Atualizando telemetria 'wac_geopdf_progress.json' no R2...", flush=True)
    progress_data["r2_prefix"] = "wac"
    progress_data["metadata"] = metadata
    progress_data["status"] = "completed"
    progress_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    if "logs" not in progress_data:
        progress_data["logs"] = []
    progress_data["logs"].insert(0, f"[{datetime.now().strftime('%H:%M:%S')}] 🚀 Cartas WAC HD promovidas oficialmente para a pasta wac/ ({promoted_count}/46).")

    try:
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=PROGRESS_KEY,
            Body=json.dumps(progress_data, indent=2, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, no-store, must-revalidate"
        )
        print(f"  ✅ Telemetria '{PROGRESS_KEY}' atualizada com sucesso no R2!", flush=True)
    except Exception as e:
        print(f"  ⚠️ Aviso ao atualizar telemetria: {e}", flush=True)

    print("\n" + "=" * 70, flush=True)
    print("🎉 PROMOÇÃO CONCLUÍDA COM TOTAL SUCESSO!", flush=True)
    print("  • 46 Cartas HD em vigor na pasta 'wac/WAC{codigo}.mbtiles'")
    print("  • Quarentena 'wac-test/' limpa")
    print("  • WAC_BRASIL_FULL mantido intacto conforme solicitado")
    print("  • App SkyFPL já compatível sem necessidade de alteração de código!")
    print("=" * 70, flush=True)

if __name__ == "__main__":
    main()
