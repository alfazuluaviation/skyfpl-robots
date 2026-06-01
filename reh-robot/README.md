# 🚁 SkyFPL - Robô Processador de Rotas Especiais de Helicópteros (REH)

Este robô é responsável por coletar, georreferenciar, mesclar e otimizar as **cartas brasileiras de Rotas Especiais de Helicópteros (REH)** do GeoServer oficial do DECEA via WMS, convertendo-as em arquivos SQLite `.mbtiles` ultra-leves e eficientes no Cloudflare R2.

---

## ⚙️ Principais Funcionalidades

1.  **Coleta de Alta Fidelidade (GeoServer):** Realiza requisições `GetMap` na projeção `EPSG:3857` (Web Mercator) em blocos de `512x512` pixels com fundo transparente. *(Espelhando exatamente a configuração de qualidade do robô REA)*
2.  **Zoom Inteligente (Z8 a Z11):** Respeita estritamente a profundidade padrão de visualização das cartas visuais especiais (Mínimo Z8, Máximo Z11). Pode ser expandido para suportar helipontos em Z13/Z14 no futuro.
3.  **Fusão Alpha Composite:** Quando múltiplos corredores com áreas geográficas sobrepostas são compilados de forma unificada (modo consolidado), as emendas dos tiles são fundidas de maneira transparente usando PIL/Pillow para evitar cortes bruscos ou falhas de emenda.
4.  **Otimização SQLite (VACUUM):** Todos os arquivos de banco `.mbtiles` gerados passam por um processo de compactação ativa para reduzir em até 30% o tamanho final de download.
5.  **Telemetria em Tempo Real (R2 Pipe):** Atualiza um status JSON (`telemetry.json` em `charts/reh/`) a cada progresso incremental com porcentagens e metadados de tamanho físico, permitindo monitoramento remoto em tempo real pelo Dashboard SkyNav Pro.

---

## 🇧🇷 Grade de Cartas Cobertas (REH)

O robô possui um inventário estático das coordenadas limite exatas ($BBOX$) mapeadas do DECEA GetCapabilities:

*   **REH Rio de Janeiro** (`REH_WJ2_RIO_DE_JANEIRO`)
*   **REH São Paulo** (`REH_XP_SAO_PAULO`)
*   **REH Vitória** (`REH_XR_VITORIA`)
*   **Consolidado Brasil Full** (`REH_BR_COMPLETO`)

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

### Execução de Teste Simples (São Paulo)
```bash
export CHART_CODES="REH_XP_SAO_PAULO"
export MIN_ZOOM=8
export MAX_ZOOM=11
export SINGLE_FILE="false"
export WORKERS=4

python reh-robot/build_reh.py
```

