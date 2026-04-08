# Price Monitor — Projeto ML End-to-End

Sistema de monitoramento de preços de hardware no Mercado Livre. Coleta preços automaticamente via web scraping, aplica Machine Learning para detectar anomalias e prever tendências, e envia alertas via Discord.

## Arquitetura

```
[Schedule: a cada 6h]
       │
       ▼
[Scraper] ── Web Scraping Mercado Livre ──▶ [PostgreSQL]
                                                 │
                                                 ▼
                                          [Pipeline ML]
                                           │    │    │
                                 features  │ anomaly │  forecast
                                           │    │    │
                                           ▼    ▼    ▼
                                   [API FastAPI] + [Discord Bot]
```

## Stack

| Componente | Tecnologia |
|------------|-----------|
| Coleta | Python, requests, BeautifulSoup (web scraping) |
| Banco | PostgreSQL 16 (Docker) |
| Feature Engineering | Pandas, NumPy |
| Detecção de Anomalias | scikit-learn (Isolation Forest) |
| Previsão de Preços | XGBoost |
| API REST | FastAPI + Uvicorn |
| Alertas | Discord (webhook) |
| Infra | Docker, Docker Compose |

## Estrutura

```
price-monitor/
├── docker-compose.yml      # orquestra os 5 serviços
├── .env.example             # template de configuração
├── db/
│   └── init.sql             # schema: products, price_history, anomalies
├── scraper/
│   ├── Dockerfile
│   ├── main.py              # web scraping do Mercado Livre
│   └── requirements.txt
├── ml/
│   ├── Dockerfile
│   ├── main.py              # pipeline: features + Isolation Forest + XGBoost
│   ├── models/              # modelos serializados (.joblib)
│   └── requirements.txt
├── api/
│   ├── Dockerfile
│   ├── main.py              # FastAPI com 7 endpoints
│   └── requirements.txt
└── bot/
    ├── Dockerfile
    ├── main.py              # alertas e resumo diário via Discord
    └── requirements.txt
```

## Como funciona

### Scraper
- Faz web scraping do Mercado Livre (5 categorias configuráveis)
- Paginação automática (até 4 páginas por categoria, ~200 produtos)
- Upsert de produtos (ON CONFLICT) + histórico de preços
- Roda imediatamente na subida + a cada 6 horas

### Pipeline ML
- **Feature Engineering**: desconto %, variação de preço, média móvel, sell ratio, features temporais
- **Isolation Forest**: detecta ~5% de preços anômalos (ex: queda brusca, preço fora do padrão)
- **XGBoost**: treina modelo de regressão para prever preço com base em 7 features
- Modelos salvos com joblib em volume compartilhado com a API
- Auto-treina quando há dados suficientes (10+ registros para anomalias, 20+ para previsão)
- Retreina a cada 6 horas com todos os dados acumulados

### API (FastAPI)
- `GET /` — health check
- `GET /products` — lista produtos (filtro por categoria, paginação)
- `GET /products/{ml_id}` — detalhes de um produto
- `GET /products/{ml_id}/prices` — histórico de preços
- `GET /anomalies` — anomalias detectadas pelo ML
- `GET /stats` — estatísticas gerais do sistema
- `GET /predict/{ml_id}` — previsão de preço via XGBoost
- Documentação interativa em `http://localhost:8000/docs`

### Bot Discord
- Alertas de anomalias a cada 6h com níveis de classificação:
  - 🟢 Possível oportunidade (desconto legítimo provável)
  - 🟡 Variação atípica (vale investigar)
  - 🟠 Preço incomum (bem fora do padrão)
  - 🔴 Preço muito fora do padrão (possível erro ou golpe)
- Embeds com preço formatado (R$ 1.649,90), nível, link e thumbnail
- Resumo diário às 09:00 (total de produtos, coletas, variações)
- Usa webhook (não precisa de bot token)

## Setup

```bash
# 1. Configurar variáveis de ambiente
cp .env.example .env
# editar .env com suas credenciais

# 2. Instalar Docker Desktop (Mac)
brew install --cask docker

# 3. Subir todos os serviços
docker compose up --build

# 4. Acessar
# API: http://localhost:8000/docs
# Logs: docker compose logs -f scraper
```

## Variáveis de ambiente (.env)

| Variável | Descrição |
|----------|-----------|
| `POSTGRES_USER` | Usuário do banco |
| `POSTGRES_PASSWORD` | Senha do banco |
| `POSTGRES_DB` | Nome do banco |
| `POSTGRES_HOST` | Host (usar `db` no Docker) |
| `DISCORD_WEBHOOK_URL` | URL do webhook do Discord |
| `ML_CATEGORIES` | Categorias do ML separadas por vírgula |
| `COLLECTION_INTERVAL_HOURS` | Intervalo entre coletas (padrão: 6) |

## Timeline de execução

1. `docker compose up --build`
2. PostgreSQL cria as tabelas (init.sql)
3. Scraper coleta ~200 produtos por categoria (5 categorias)
4. Após 2 min: ML treina Isolation Forest + XGBoost
5. API online em `localhost:8000`
6. Após 5 min: Bot envia alertas no Discord
7. A cada 6h: coleta → retreino → alertas
8. Diariamente às 09:00: resumo no Discord
