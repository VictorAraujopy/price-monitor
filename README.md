# Price Monitor — Projeto ML End-to-End

Sistema de monitoramento de preços de hardware em e-commerces brasileiros. Coleta preços automaticamente via API, aplica modelos de Machine Learning para detectar anomalias e prever tendências, e notifica os resultados via Discord.

## Arquitetura

```
[Cron: a cada 6h]
       │
       ▼
[Coleta] ── API Mercado Livre ──▶ [PostgreSQL]
                                       │
                                       ▼
                                [Pipeline ML]
                                 │    │    │
                       features  │ anomaly │  forecast
                                 │    │    │
                                 ▼    ▼    ▼
                              [Discord Webhook]
```


## Stack

| Componente | Tecnologia |
|------------|-----------|
| Coleta | Python, requests, API Mercado Livre |
| Banco | PostgreSQL (Docker) |
| Feature Engineering | Pandas, NumPy |
| Anomaly Detection | scikit-learn (Isolation Forest) |
| Forecasting | XGBoost, Prophet |
| Deploy do modelo | FastAPI |
| Interface | Discord (webhook + bot) |
| Infra | Docker, docker-compose |

## Estrutura

```
price-monitor/
├── docker-compose.yml
├── .env
├── scraper/          # coleta via API do Mercado Livre
├── ml/               # pipeline ML (features, anomaly, forecast, alertas)
│   └── models/       # modelos serializados (.joblib)
├── api/              # FastAPI — deploy do modelo (mês 4-5)
├── bot/              # bot Discord (mês 4-5)
├── db/               # schema SQL
└── notebooks/        # experimentação e análise
```

## Setup

```bash
cp .env.example .env
# editar .env com suas credenciais
docker-compose up -d
```
