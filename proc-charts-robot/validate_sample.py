"""
================================================================================
VALIDAÇÃO DE AMOSTRA — Pipeline GDAL v3.0.0
================================================================================
Testa 5–10 cartas antes de processar o lote completo de 1829.

Critério de sucesso:
  - georreferenciado: true em >= 80% das cartas testadas
  - Sem erros de abertura GDAL
  - Tamanho JPEG razoável (> 200 KB indica render real, não página em branco)

Uso:
    python validate_sample.py --input ./pdfs --output ./output_sample --count 10
================================================================================
"""

import os
import json
import argparse
import random
from pathlib import Path

os.environ["GDAL_PDF_LIB"] = "INTERNAL"

try:
    from osgeo import gdal
    gdal.UseExceptions()
except ImportError:
    raise ImportError("GDAL não encontrado. Execute: pip install gdal")

# Importa o pipeline principal
import sys
sys.path.insert(0, str(Path(__file__).parent))
from pipeline_gdal_cartas_v3 import processar_carta, CONFIG


def main():
    parser = argparse.ArgumentParser(description="Validação de amostra — Pipeline GDAL v3.0.0")
    parser.add_argument("--input",  "-i", required=True)
    parser.add_argument("--output", "-o", required=True)
    parser.add_argument("--count",  "-n", type=int, default=10,
                        help="Número de cartas a testar (default: 10)")
    parser.add_argument("--seed",   type=int, default=42,
                        help="Seed para seleção aleatória reproduzível")
    args = parser.parse_args()

    input_dir  = Path(args.input)
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(input_dir.glob("*.pdf"))
    if not pdfs:
        print(f"❌ Nenhum PDF encontrado em: {input_dir}")
        return

    random.seed(args.seed)
    amostra = random.sample(pdfs, min(args.count, len(pdfs)))

    print(f"\n🔬 VALIDAÇÃO DE AMOSTRA — {len(amostra)} cartas selecionadas aleatoriamente")
    print(f"   Config: {CONFIG['dpi']} DPI | JPEG q={CONFIG['jpeg_quality']} | {CONFIG['subsampling']}")
    print(f"   Driver: GDAL_PDF_LIB={os.environ.get('GDAL_PDF_LIB')}\n")

    resultados = []
    for pdf in amostra:
        print(f"  Processando: {pdf.name}...")
        r = processar_carta(pdf, output_dir)
        resultados.append(r)

        status_icon = "✅" if r["status"] == "ok" else "❌" if r["status"] == "erro" else "⏭"
        georef_icon = "🗺 SIM" if r.get("georreferenciado") else "⚠ NÃO"
        tamanho    = f"{r.get('tamanho_kb', 0):.0f} KB" if r.get("tamanho_kb") else "—"
        print(f"    {status_icon} {r['carta']} | Georef: {georef_icon} | Tamanho: {tamanho}")
        if r["status"] == "erro":
            print(f"    💬 Erro: {r.get('erro', '?')}")

    # Resumo
    ok     = [r for r in resultados if r["status"] == "ok"]
    erros  = [r for r in resultados if r["status"] == "erro"]
    georef = [r for r in ok if r.get("georreferenciado")]

    taxa_georef = (len(georef) / len(ok) * 100) if ok else 0
    media_kb    = (sum(r.get("tamanho_kb", 0) for r in ok) / len(ok)) if ok else 0

    print(f"\n{'═' * 55}")
    print(f"  RESULTADO DA VALIDAÇÃO")
    print(f"{'═' * 55}")
    print(f"  Testadas        : {len(amostra)}")
    print(f"  ✅ OK           : {len(ok)}")
    print(f"  ❌ Erros        : {len(erros)}")
    print(f"  🗺  Georef       : {len(georef)} ({taxa_georef:.0f}%)")
    print(f"  📄 Média tamanho : {media_kb:.0f} KB")
    print()

    if taxa_georef >= 80 and len(erros) == 0 and media_kb > 200:
        print("  ✅✅ VALIDAÇÃO APROVADA — pode rodar o lote completo!")
        print(f"     python pipeline_gdal_cartas_v3.py --input {args.input} "
              f"--output <output_final> --workers 8")
    elif taxa_georef < 80:
        print(f"  ⚠ ATENÇÃO: Taxa de georef abaixo de 80% ({taxa_georef:.0f}%)")
        print("     Verifique se os GeoPDFs da DECEA usam OGC LGIDict ou Adobe georef.")
        print("     Execute com 1 worker e verbose para inspecionar os logs.")
    elif len(erros) > 0:
        print(f"  ❌ ATENÇÃO: {len(erros)} erro(s) detectados.")
        print("     Verifique se o GDAL com suporte a PDF está instalado corretamente.")
        print("     Ubuntu: apt-get install gdal-bin python3-gdal")
    elif media_kb < 200:
        print(f"  ⚠ ATENÇÃO: Tamanho médio muito baixo ({media_kb:.0f} KB).")
        print("     Pode indicar páginas em branco ou DPI não aplicado corretamente.")
    print(f"{'═' * 55}\n")

    # Salva relatório da amostra
    relatorio_path = output_dir / "relatorio_validacao.json"
    with open(relatorio_path, "w", encoding="utf-8") as f:
        json.dump({
            "pipeline_versao": "3.0.0",
            "cartas_testadas": len(amostra),
            "ok": len(ok),
            "erros": len(erros),
            "georef": len(georef),
            "taxa_georef_pct": round(taxa_georef, 1),
            "media_kb": round(media_kb, 1),
            "config": CONFIG,
            "resultados": resultados,
        }, f, ensure_ascii=False, indent=2)
    print(f"  📋 Relatório salvo em: {relatorio_path}")


if __name__ == "__main__":
    main()
