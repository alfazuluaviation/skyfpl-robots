# Plano de Implementação: Robô de Processamento REA (skyfpl-robots)

Este documento detalha o plano de engenharia para criar o **Robô Crawler Python** para cartas de corredores visuais (REA) no repositório dedicado de robôs (`skyfpl-robots`).

---

## 🎯 Escopo do Objetivo

1.  **Pasta Dedicada:** Criar a pasta `rea-robot` no repositório `skyfpl-robots` para abrigar a lógica Python isolada.
2.  **Robô Crawler Python (`build_rea.py`):** Desenvolver o motor de download paralelo WMS baseado na malha real do DECEA, aplicando mesclagem de transparências (Alpha Composite) e integridade de SQLite.
3.  **Workflow de Disparo (`process-rea.yml`):** Criar a Action no GitHub para rodar o crawler sob demanda.
4.  **Ajuste do Gatilho do Dashboard:** Apontar o botão de disparo da tela de administração para o repositório `skyfpl-robots` ao invés do repositório principal de front-end.

---

## ⚙️ Detalhamento das Alterações

### 1. Novo Robô Python: `rea-robot/build_rea.py`

*   **Dicionário de Bounding Boxes (Real):** Utilizaremos as coordenadas georreferenciadas exatas coletadas diretamente da malha do DECEA GeoServer:
    *   23 setores REA oficiais mapeados um a um.
*   **Downloads Assíncronos & Retries:** Download paralelo com `ThreadPoolExecutor`, limitador de concorrência dinâmico e resiliência a quedas (5 tentativas de GetMap WMS).
*   **Fusão Alpha Composite:** Lógica de mesclagem na zona de sobreposição atômica com PIL para manter os pixels inferiores vivos nas emendas.
*   **Progresso R2:** Publicação de logs rápidos no arquivo `rea_progress.json` do Cloudflare.

### 2. Nova Action: `.github/workflows/process-rea.yml`

*   **Workflow Dispatch:** Disparador manual com os inputs:
    *   `chart_codes` (ALL ou códigos individuais).
    *   `min_zoom` (Default 8).
    *   `max_zoom` (Default 11).
    *   `single_file` (Booleano).
    *   `workers` (Qtd de threads).
*   **Execução:** Instalação das dependências do `requirements.txt` e rodada de `python rea-robot/build_rea.py`.

### 3. Dashboard Admin: `ReaManagement.tsx`

*   Ajustar a variável `GITHUB_REPO` para `"alfazuluaviation/skyfpl-robots"`.

---

## 🔍 Plano de Verificação e QA

### 1. Auditoria Estática
*   Revisão minuciosa das conexões SQLite, tratamento de buffers e fechamento atômico de arquivos.

### 2. Aprovação do Usuário
*   **Parar e aguardar** a sua aprovação explícita sobre este plano antes de gerar ou enviar qualquer código para o repositório oficial de robôs.

---

## 🚀 Fase 2: Neatline Oficial v2 e Corte Conforme (Outubro/2026)

*   **Identificação do Problema:** Cartas REA originais são produzidas em projeção métrica UTM (ex: Zona 23S em SP). O uso anterior do BBOX WMS envolvente gerava "buracos" no mapa e corte com bordas de papel cinza/branco nos cantos.
*   **Extrator Canônico v2 (`extract_all_rea_polygons.py`):**
    *   Extrai a tag oficial `NEATLINE` do GeoPDF mestre (ArcGIS Pro DECEA).
    *   Reprojeta os vértices de cada zona UTM para `EPSG:4326` preservando a rotação/inclinação correta.
    *   Aplica **Inset Buffer de segurança** (~80 m / 0.0008°) para eliminar 100% da linha preta e da margem de papel.
    *   Fallback automático para extensão do GeoTIFF DECEA.
*   **Arquivos Atualizados:**
    *   `rea_catalog.json`: Catálogo com novos BBOXes reais e links dinâmicos do AISWEB.
    *   `rea_official_polygons.json`: GeoJSON canônico com os anéis poligonais do NEATLINE consumidos pelo robô `-cutline`.
    *   `reaOfficialPolygons.ts`: Tipagem TypeScript para o Dashboard Admin desenhar o contorno exato no mapa sem sobras.

