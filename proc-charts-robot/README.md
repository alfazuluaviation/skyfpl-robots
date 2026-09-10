# 🗺️ SkyFPL Procedural Charts Robot (proc-charts-robot)

Robô inteligente de ingestão, rasterização de alta resolução (250 DPI) e georreferenciamento de precisão micrométrica para Cartas de Procedimento Aeronáutico (ADC, PDC, IAC, SID, STAR, VAC) do DECEA/AISWEB.

---

## 🚀 Novidades da Versão 14.3 (Fase 1 - Multi-Block Precision)

* **Multi-Block Intelligent Selector**: Detecção automática de múltiplos blocos `/GPTS` e `/LPTS` no GeoPDF original do DECEA. Cartas complexas de aeródromos (ex: ADC de SBRF e SBCF com caixas de visão geral / Lagoa Santa) agora selecionam o bloco mais próximo ao ARP oficial do aeródromo com desvio zero.
* **Extrapolação Afim 4-Pontos (`solve_affine_4point`)**: Solucionador afim por determinantes de Cramer 3x3 e mínimos quadrados que extrapola as coordenadas nativas dos metadados para os 4 cantos exatos da imagem renderizada `[0.0, 1.0]`, eliminando qualquer distorção de borda.
* **Cache Inteligente de ARPs**: Pré-carregamento de coordenadas oficiais do ROTAER no início da execução.
* **Shadow Mode QA**: Comparação contínua de delta geodésico (Haversine) contra a base de dados histórica do Supabase.

---

## 🛠️ Instalação & Dependências

```bash
cd proc-charts-robot
pip install -r requirements.txt
```

---

## ⚙️ Execução

### Modo Auditoria / Dry-Run (Sem upload ao R2/Banco):
```bash
python index_proc_charts.py --icao SBRF,SBCF,SBAR,SBSV --dry-run true
```

### Execução de Ciclo AIRAC Completo:
```bash
python index_proc_charts.py --airac 2609
```
