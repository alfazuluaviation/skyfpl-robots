#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_rea_aisweb_links.py — Atualizador de Links GeoPDF Oficiais do AISWEB
========================================================================
Varre a página oficial de Cartas Visuais do DECEA (https://aisweb.decea.mil.br/?i=cartas&p=visuais)
e atualiza dinamicamente as URLs dos arquivos mestres GeoPDF de todas as 26 cartas REA
diretamente no arquivo rea_catalog.json.

Pode ser executado a qualquer momento em novos ciclos AIRAC para manter os links atualizados.
"""

import json
import requests
import re
import os
import sys

def sync_aisweb_links():
    catalog_path = os.path.join(os.path.dirname(__file__), "rea_catalog.json")
    if not os.path.exists(catalog_path):
        print(f"[ERRO] Catálogo não encontrado em {catalog_path}")
        return False

    with open(catalog_path, "r", encoding="utf-8") as f:
        catalog = json.load(f)

    print("🛰️ Consultando AISWEB (https://aisweb.decea.mil.br/?i=cartas&p=visuais)...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "close"
    }
    try:
        r = requests.get("https://aisweb.decea.mil.br/?i=cartas&p=visuais", headers=headers, timeout=25)
        if r.status_code != 200:
            print(f"[ERRO] AISWEB retornou status HTTP {r.status_code}")
            return False
    except Exception as e:
        print(f"[ERRO] Falha ao conectar ao AISWEB: {e}")
        return False

    rows = re.findall(r"<tr[^>]*>.*?</tr>", r.text, re.DOTALL)
    rea_rows = [row for row in rows if 'data-order="REA"' in row]

    pdf_map = {}
    for row in rea_rows:
        m = re.search(r'href=[\'"]([^\'"]+\.pdf)[\'"]', row)
        if m:
            url = m.group(1).strip()
            fname = url.split("/")[-1]
            pdf_map[fname] = url

    print(f"📄 {len(pdf_map)} arquivos PDF oficiais de cartas REA encontrados no portal.")

    updated_count = 0
    for code, item in catalog.items():
        matched_url = None
        c_lower = code.lower()

        for fname, url in pdf_map.items():
            fn_lower = fname.lower()
            if "cuiaba" in c_lower and "cuiaba" in fn_lower:
                matched_url = url
                break
            elif "curitiba" in c_lower and "curitiba" in fn_lower:
                matched_url = url
                break
            elif "ribeirao" in c_lower and "ribeirao" in fn_lower:
                matched_url = url
                break
            elif "anapolis" in c_lower and "anapolis" in fn_lower:
                matched_url = url
                break
            elif "londrina" in c_lower and "londrina" in fn_lower:
                matched_url = url
                break
            elif "xp1" in c_lower and "xp1" in fn_lower:
                matched_url = url
                break
            elif "xp2" in c_lower and "xp2" in fn_lower:
                matched_url = url
                break
            elif "wj1" in c_lower and "wj1" in fn_lower:
                matched_url = url
                break
            else:
                suffix = code.replace("REA_", "").lower().replace("-", "_")
                tokens = [t for t in suffix.split("_") if len(t) > 2]
                if tokens and all(t in fn_lower for t in tokens):
                    matched_url = url
                    break

        if matched_url:
            old_url = item.get("pdf_url")
            if old_url != matched_url:
                item["pdf_url"] = matched_url
                updated_count += 1
                print(f"  [ATUALIZADO] {code} -> {matched_url.split('/')[-1]}")
            else:
                print(f"  [OK] {code} -> {matched_url.split('/')[-1]}")
        else:
            print(f"  [ALERTA] Não foi possível mapear PDF para: {code}")

    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)

    print(f"✅ Sincronização concluída! {updated_count} URLs atualizadas no catálogo.")
    return True

if __name__ == "__main__":
    sync_aisweb_links()
