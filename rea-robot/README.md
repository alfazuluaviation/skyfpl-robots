# 🛰️ SkyFPL - Robô Processador de Rotas Especiais de Aeronaves (REA)

Este robô é responsável por coletar, georreferenciar, mesclar e otimizar as **23 cartas brasileiras de Rotas Especiais de Aeronaves (REA)** do GeoServer oficial do DECEA via WMS, convertendo-as em arquivos SQLite `.mbtiles` ultra-leves e eficientes no Cloudflare R2.

---

## ⚙️ Principais Funcionalidades

1.  **Coleta de Alta Fidelidade (GeoServer):** Realiza requisições `GetMap` na projeção `EPSG:3857` (Web Mercator) em blocos de `256x256` pixels com fundo transparente.
2.  **Zoom Inteligente (Z8 a Z11):** Respeita estritamente a profundidade padrão de visualização das cartas visuais especiais (Mínimo Z8, Máximo Z11).
3.  **Fusão Alpha Composite:** Quando múltiplos corredores com áreas geográficas sobrepostas são compilados de forma unificada (modo consolidado), as emendas dos tiles são fundidas de maneira transparente usando PIL/Pillow para evitar cortes bruscos ou falhas de emenda.
4.  **Otimização SQLite (VACUUM):** Todos os arquivos de banco `.mbtiles` gerados passam por um processo de compactação ativa para reduzir em até 30% o tamanho final de download.
5.  **Telemetria em Tempo Real (R2 Pipe):** Atualiza um status JSON (`rea_progress.json`) a cada progresso incremental com porcentagens e metadados de tamanho físico, permitindo monitoramento remoto em tempo real.

---

## 🇧🇷 Grade de Cartas Cobertas (23 Setores)

O robô possui um inventário estático das coordenadas limite exatas ($BBOX$) mapeadas do DECEA GetCapabilities:

*   **REA Parintins** (`REA_PI_PARINTINS`)
*   **REA Tabatinga** (`REA_WA_TABATINGA`)
*   **REA Belém** (`REA_WB_BELEM`)
*   **REA Recife** (`REA_WF_RECIFE`)
*   **REA Campo Grande** (`REA_WG_CAMPO_GRANDE`)
*   **REA Rio de Janeiro** (`REA_WJ1_RIO_DE_JANEIRO`)
*   **REA Porto Seguro** (`REA_WK_PORTO_SEGURO`)
*   **REA Manaus** (`REA_WN_MANAUS`)
*   **REA Porto Alegre** (`REA_WP1_PORTO_ALEGRE`)
*   **REA Brasília** (`REA_WR_BRASILIA`)
*   **REA São Luís** (`REA_WS_SAO_LUIS`)
*   **REA Santarém** (`REA_WX_SANTAREM`)
*   **REA Cuiabá** (`REA_WY_CUIABA`)
*   **REA Fortaleza** (`REA_WZ_FORTALEZA`)
*   **REA Florianópolis** (`REA_XF_FLORIANOPOLIS`)
*   **REA Macapá** (`REA_XK_MACAPA`)
*   **REA Anápolis** (`REA_XN_ANAPOLIS`)
*   **REA Londrina** (`REA_XO_LONDRINA`)
*   **REA São Paulo** (`REA_XP1_SAO_PAULO`)
*   **REA Ribeirão Preto** (`REA_XQ_RIBEIRAO_PRETO`)
*   **REA Vitória** (`REA_XR_VITORIA`)
*   **REA Salvador** (`REA_XS_SALVADOR`)
*   **REA Natal** (`REA_XT_NATAL`)
*   **Consolidado Brasil Full** (`REA_BR_COMPLETO`)

---

## 🚀 Como Executar Localmente

### Pré-requisitos
Certifique-se de ter instalado as dependências do `requirements.txt` do repositório raiz:
```bash
pip install -r requirements.txt
```

### Variáveis de Ambiente Necessárias
Configure as credenciais e parâmetros:
```bash
export R2_ENDPOINT="https://xxxx.r2.cloudflarestorage.com"
export R2_ACCESS_KEY_ID="sua_chave_id"
export R2_SECRET_ACCESS_KEY="sua_chave_secreta"
export R2_BUCKET="skyfpl-charts"
```

### Execução de Teste Simples (Parintins)
```bash
export CHART_CODES="REA_PI_PARINTINS"
export MIN_ZOOM=8
export MAX_ZOOM=11
export SINGLE_FILE="false"
export WORKERS=4

python rea-robot/build_rea.py
```
