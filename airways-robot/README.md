# Airway Navigation Robot

Este robô extrai dados de aerovias (Alta e Baixa) do DECEA via WFS (Web Feature Service) e gera um banco de dados SQLite otimizado para o aplicativo SkyFPL.

## Funcionamento

1.  Consome as camadas `ICA:vw_aerovia_baixa_v2` e `ICA:vw_aerovia_alta_v2`.
2.  Processa os segmentos, preservando a sequência e os fixos de entrada/saída.
3.  Gera um arquivo `airways_v1.db`.
4.  Realiza o upload automático para o Cloudflare R2.

## Instalação

```bash
pip install -r requirements.txt
```

## Execução

```bash
# Variáveis de ambiente necessárias:
# R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT, SUPABASE_ANON_KEY

python build_airways.py
```
