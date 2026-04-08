import os
import psycopg2
import psycopg2.extras
import joblib
import numpy as np
from fastapi import FastAPI, Query, HTTPException
from contextlib import contextmanager

app = FastAPI(
    title="Price Monitor API",
    description="API para monitoramento de preços de hardware no Mercado Livre",
    version="1.0.0",
)

MODELS_DIR = "/app/models"


@contextmanager
def get_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        yield conn
    finally:
        conn.close()


@app.get("/")
def root():
    return {"status": "online", "service": "Price Monitor API"}


@app.get("/products")
def listar_produtos(
    category: str = Query(None, description="Filtrar por categoria"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Lista produtos monitorados."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if category:
                cur.execute(
                    "SELECT * FROM products WHERE category_id = %s ORDER BY updated_at DESC LIMIT %s OFFSET %s",
                    (category, limit, offset),
                )
            else:
                cur.execute(
                    "SELECT * FROM products ORDER BY updated_at DESC LIMIT %s OFFSET %s",
                    (limit, offset),
                )
            return cur.fetchall()


@app.get("/products/{ml_id}")
def detalhe_produto(ml_id: str):
    """Retorna detalhes de um produto pelo ml_id."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM products WHERE ml_id = %s", (ml_id,))
            produto = cur.fetchone()
            if not produto:
                raise HTTPException(status_code=404, detail="Produto não encontrado")
            return produto


@app.get("/products/{ml_id}/prices")
def historico_precos(
    ml_id: str,
    limit: int = Query(100, ge=1, le=1000),
):
    """Retorna histórico de preços de um produto."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ph.* FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE p.ml_id = %s
                ORDER BY ph.collected_at DESC
                LIMIT %s
                """,
                (ml_id, limit),
            )
            return cur.fetchall()


@app.get("/anomalies")
def listar_anomalias(
    limit: int = Query(50, ge=1, le=200),
):
    """Lista anomalias de preço detectadas pelo ML."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM anomalies
                ORDER BY detected_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()


@app.get("/stats")
def estatisticas():
    """Estatísticas gerais do sistema."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM products")
            total_produtos = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) as total FROM price_history")
            total_precos = cur.fetchone()["total"]

            cur.execute(
                "SELECT COUNT(*) as total FROM anomalies"
                " WHERE detected_at > NOW() - INTERVAL '24 hours'"
            )
            anomalias_24h = cur.fetchone()["total"]

            cur.execute(
                "SELECT MAX(collected_at) as ultima FROM price_history"
            )
            ultima_coleta = cur.fetchone()["ultima"]

            return {
                "total_produtos": total_produtos,
                "total_registros_preco": total_precos,
                "anomalias_24h": anomalias_24h,
                "ultima_coleta": str(ultima_coleta) if ultima_coleta else None,
            }


@app.get("/predict/{ml_id}")
def prever_preco(ml_id: str):
    """Retorna a previsão de preço para um produto usando o modelo treinado."""
    model_path = f"{MODELS_DIR}/xgb_forecast.joblib"

    try:
        model = joblib.load(model_path)
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="Modelo ainda não treinado. Aguarde o pipeline ML.",
        )

    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ph.*, p.ml_id FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE p.ml_id = %s
                ORDER BY ph.collected_at DESC
                LIMIT 5
                """,
                (ml_id,),
            )
            rows = cur.fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail="Sem dados de preço para esse produto")

    ultimo = rows[0]
    price = float(ultimo["price"])
    original_price = float(ultimo["original_price"]) if ultimo["original_price"] else price

    discount_pct = ((original_price - price) / original_price * 100) if original_price > 0 else 0
    available = ultimo["available_quantity"] or 0
    sold = ultimo["sold_quantity"] or 0
    sell_ratio = sold / (available + sold) if (available + sold) > 0 else 0

    prices = [float(r["price"]) for r in rows]
    price_ma3 = np.mean(prices[:3])
    price_change_pct = ((prices[0] - prices[1]) / prices[1] * 100) if len(prices) > 1 and prices[1] > 0 else 0

    from datetime import datetime
    now = datetime.now()

    features = np.array([[
        discount_pct,
        now.hour, now.weekday(), now.day,
        price_change_pct, price_ma3, sell_ratio,
    ]])

    predicted_price = float(model.predict(features)[0])

    return {
        "ml_id": ml_id,
        "preco_atual": price,
        "preco_previsto": round(predicted_price, 2),
        "variacao_prevista_pct": round((predicted_price - price) / price * 100, 2) if price > 0 else 0,
    }
