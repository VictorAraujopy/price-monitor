# Price Monitor — Monitoramento de Preços com ML

Sistema que monitora preços de produtos em **múltiplas lojas** via Buscapé, detecta anomalias com **Isolation Forest** (por categoria), prevê tendências com **Prophet** (por produto) e envia alertas segmentados no **Discord** com menção de cargo.

## Arquitetura

```
[Schedule: a cada 6h]
       │
       ▼
[Scraper] ── Buscapé (multi-loja) ──▶ [PostgreSQL]
                                            │
                                            ▼
                                     [Pipeline ML]
                                      │         │
                            Isolation Forest   Prophet
                            (por categoria)  (por produto)
                                      │         │
                                      ▼         ▼
                               [Anomalias]  [Modelos .joblib]
                                      │         │
                              ┌───────┴─────────┘
                              ▼                 ▼
                       [Discord Bot]      [API FastAPI]
                        │        │         /predict
                   #apple   #beleza       /compare
                   @cargo   @cargo        /anomalies
```

## Stack

| Componente | Tecnologia |
|---|---|
| Coleta | Python, requests, BeautifulSoup (Buscapé) |
| Banco | PostgreSQL 16 (Docker) |
| Detecção de Anomalias | scikit-learn — Isolation Forest (por categoria) |
| Previsão de Preços | Prophet — Meta (por produto, 30-90 dias) |
| API REST | FastAPI + Uvicorn (8 endpoints) |
| Alertas | Discord webhooks (multi-canal com cargos) |
| Infra | Docker Compose (5 serviços) |

## Estrutura

```
price-monitor/
├── docker-compose.yml        # 5 serviços + volumes compartilhados
├── .env                      # configuração (webhooks, categorias, cargos)
├── db/
│   └── init.sql              # schema: products, price_history, anomalies
├── scraper/
│   ├── Dockerfile
│   ├── main.py               # web scraping Buscapé (preços de várias lojas)
│   └── requirements.txt
├── ml/
│   ├── Dockerfile
│   ├── main.py               # Isolation Forest + Prophet
│   └── requirements.txt
├── api/
│   ├── Dockerfile
│   ├── main.py               # FastAPI — /predict, /compare, /anomalies
│   └── requirements.txt
├── bot/
│   ├── Dockerfile
│   ├── main.py               # alertas Discord multi-canal com cargos
│   └── requirements.txt
└── COMO_FUNCIONA.docx        # documentação detalhada do projeto
```

## Como funciona

### Scraper
- Coleta preços de **múltiplas lojas** por produto via Buscapé (comparador de preços)
- Calcula média, mínimo e máximo do mercado **na hora da coleta**
- Anti-bloqueio: 5 User-Agents rotativos, headers completos, delays aleatórios
- UPSERT de produtos (`ON CONFLICT slug`) + histórico de preços por loja
- Categorias configuráveis via `.env`

### Pipeline ML
- **Isolation Forest por categoria**: compara iPad com iPad, MacBook com MacBook (nunca mistura)
  - Features: `price`, `discount_pct`, `price_spread`, `num_stores_norm`
  - Filtro duplo: IF marcou como anomalia **E** preço 10%+ abaixo da média
  - `contamination=0.05` (5% de anomalias esperadas)
- **Prophet por produto**: treina um modelo de séries temporais pra cada produto
  - Prevê preços de 7 a 365 dias no futuro
  - Precisa de 2+ dias de dados pra treinar, melhora com o tempo
  - Modelos salvos em volume compartilhado (`ml-models`)
- Espera 30 min pro scraper terminar, retry a cada 5 min se banco vazio

### API (FastAPI)
| Endpoint | Descrição |
|---|---|
| `GET /` | Health check |
| `GET /products` | Lista produtos (`?category=ipad&limit=50`) |
| `GET /products/{slug}` | Detalhes de um produto |
| `GET /products/{slug}/prices` | Preços de todas as lojas |
| `GET /compare/{slug}` | Compara lojas (diff_pct por loja) |
| `GET /anomalies` | Anomalias detectadas pelo ML |
| `GET /stats` | Estatísticas do sistema |
| `GET /predict/{slug}?dias=40` | Previsão Prophet (tendência + recomendação) |

Documentação interativa: `http://localhost:8000/docs`

### Bot Discord (multi-canal)
- **Canais segmentados**: cada canal tem seu webhook, cargo e categorias
  - `#price-monitor` → produtos Apple (marca `@Produtos_V`)
  - `#price-monitor-l` → produtos de beleza (marca `@cargo Beleza`)
- **Embeds** com preço, média mercado, desconto %, nível e insight do Prophet
- **Níveis de alerta**:
  - 🔴 Muito abaixo do mercado (> 30% desconto)
  - 🟠 Bem abaixo do mercado (20-30%)
  - 🟡 Abaixo do mercado (10-20%)
  - 🛒 Melhor oferta (categorias sem anomalias)
- Envio em lotes de 5 embeds por mensagem
- Resumo diário às 09:00 em todos os canais

## Setup

```bash
# 1. Clonar e configurar
git clone <repo>
cd price-monitor
cp .env.example .env
# editar .env com webhooks, cargos e categorias

# 2. Subir tudo
docker compose up --build -d

# 3. Acompanhar
docker compose logs -f scraper    # logs do scraper
docker compose logs -f ml         # logs do ML
docker compose logs -f bot        # logs do bot

# 4. Acessar API
open http://localhost:8000/docs
```

## Configuração (.env)

```bash
# Banco
POSTGRES_USER=pricemonitor
POSTGRES_PASSWORD=sua_senha
POSTGRES_DB=pricemonitor
POSTGRES_HOST=db
POSTGRES_PORT=5432

# Discord — Canal Apple
WEBHOOK_APPLE=https://discord.com/api/webhooks/xxx
CARGO_APPLE=123456789              # ID do cargo (Modo Dev → Copiar ID)
CATEGORIES_APPLE=ipad,macbook,imac,monitores

# Discord — Canal Beleza
WEBHOOK_BELEZA=https://discord.com/api/webhooks/yyy
CARGO_BELEZA=987654321
CATEGORIES_BELEZA=wella-oil

# Scraper (todas as categorias pra coletar)
SCRAPER_CATEGORIES=ipad,macbook,imac,monitores,wella-oil

# Intervalo entre ciclos
COLLECTION_INTERVAL_HOURS=6
```

### Adicionar categoria nova
1. Adicionar query em `CATEGORY_QUERIES` no `scraper/main.py`
2. Adicionar no `SCRAPER_CATEGORIES` do `.env`
3. Adicionar no `CATEGORIES_` do canal certo no `.env`
4. `docker compose restart scraper bot`

### Adicionar canal novo
1. Criar webhook no Discord (Configurações do canal → Integrações → Webhooks)
2. Criar cargo (Configurações do servidor → Cargos) e copiar ID
3. Adicionar no `.env`: `WEBHOOK_NOVO`, `CARGO_NOVO`, `CATEGORIES_NOVO`
4. Adicionar `"NOVO"` na lista de prefixos no `bot/main.py`
5. `docker compose up --build -d bot`

## Timeline de execução

```
[0s]        Docker builda e sobe PostgreSQL
[~5s]       Banco healthy → cria tabelas (init.sql)
[~10s]      Scraper começa coleta no Buscapé
[~15-20m]   Scraper termina (~165 produtos × ~5 lojas)
[~10s]      API online em localhost:8000/docs
[30m]       ML acorda → Isolation Forest + Prophet
[45m]       Bot acorda → envia alertas no Discord
[+6h]       Tudo repete automaticamente
[+2 dias]   Prophet começa fazer previsões
[+3 meses]  Previsões 30-90 dias confiáveis
```

## Comandos úteis

```bash
docker compose up --build -d                     # sobe tudo em background
docker compose down -v && docker compose up --build  # reset total
docker compose restart scraper bot               # reinicia sem rebuild
docker compose up --build -d bot                 # rebuilda só o bot
docker compose logs -f scraper --tail 50         # últimos 50 logs do scraper

# Acessar banco
docker exec -it pricemonitor-db psql -U pricemonitor -d pricemonitor

# Queries úteis
SELECT COUNT(*) FROM products;
SELECT COUNT(*) FROM price_history;
SELECT title, discount_pct FROM anomalies ORDER BY discount_pct LIMIT 10;

# API
curl localhost:8000/stats
curl localhost:8000/anomalies
curl 'localhost:8000/predict/SLUG?dias=40'
curl 'localhost:8000/compare/SLUG'
```

## Troubleshooting

| Problema | Solução |
|---|---|
| Scraper 0 produtos | Buscapé mudou HTML → atualizar seletores CSS |
| ML fica em loop "Sem dados" | Normal nos primeiros 30 min. Se persistir, checar logs do scraper |
| Bot não enviou | Webhook errado no `.env` ou 0 anomalias detectadas |
| `/predict` retorna 503 | Prophet precisa de 2+ dias de coleta |
| `database "X" does not exist` | Verificar `POSTGRES_DB` no `.env` |
| Produtos duplicados no Discord | Verificar se o bot está na versão com `DISTINCT ON` |
