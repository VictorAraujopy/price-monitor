import os
import psycopg2
import psycopg2.extras
import numpy as np
import joblib
from fastapi import FastAPI, Query, HTTPException
from contextlib import contextmanager

MODELS_DIR = "/app/models"

app = FastAPI(
    title="Price Monitor API",
    description="API para monitoramento de preços de hardware — compara preços entre lojas",
    version="2.0.0",
)


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
    return {"status": "online", "service": "Price Monitor API v2"}


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
                    "SELECT * FROM products WHERE category = %s ORDER BY updated_at DESC LIMIT %s OFFSET %s",
                    (category, limit, offset),
                )
            else:
                cur.execute(
                    "SELECT * FROM products ORDER BY updated_at DESC LIMIT %s OFFSET %s",
                    (limit, offset),
                )
            return cur.fetchall()


@app.get("/products/{slug}")
def detalhe_produto(slug: str):
    """Retorna detalhes de um produto pelo slug."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM products WHERE slug = %s", (slug,))
            produto = cur.fetchone()
            if not produto:
                raise HTTPException(status_code=404, detail="Produto não encontrado")
            return produto


@app.get("/products/{slug}/prices")
def precos_produto(
    slug: str,
    limit: int = Query(50, ge=1, le=500),
):
    """Retorna preços de todas as lojas pra um produto."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT ph.store_name, ph.price, ph.avg_price, ph.min_price,
                       ph.max_price, ph.num_stores, ph.collected_at
                FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE p.slug = %s
                ORDER BY ph.price ASC
                LIMIT %s
                """,
                (slug, limit),
            )
            return cur.fetchall()


@app.get("/anomalies")
def listar_anomalias(
    limit: int = Query(50, ge=1, le=200),
):
    """Lista ofertas com preço muito abaixo da média do mercado."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT a.*, p.permalink, p.thumbnail
                FROM anomalies a
                JOIN products p ON p.id = a.product_id
                ORDER BY a.discount_pct ASC
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
            cur.execute("SELECT COUNT(DISTINCT slug) as total FROM products")
            total_produtos = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) as total FROM price_history")
            total_ofertas = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) as total FROM anomalies")
            total_anomalias = cur.fetchone()["total"]

            cur.execute("SELECT MAX(collected_at) as ultima FROM price_history")
            ultima_coleta = cur.fetchone()["ultima"]

            return {
                "total_produtos": total_produtos,
                "total_ofertas": total_ofertas,
                "total_anomalias": total_anomalias,
                "ultima_coleta": str(ultima_coleta) if ultima_coleta else None,
            }


@app.get("/compare/{slug}")
def comparar_precos(slug: str):
    """Compara preços entre lojas pra um produto (coleta mais recente)."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (ph.store_name)
                    p.title, ph.store_name, ph.price,
                    ph.avg_price, ph.min_price, ph.max_price,
                    ph.num_stores, ph.collected_at,
                    ROUND(((ph.price - ph.avg_price) / ph.avg_price * 100)::numeric, 1) as diff_pct
                FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE p.slug = %s
                ORDER BY ph.store_name, ph.collected_at DESC
                """,
                (slug,),
            )
            ofertas = cur.fetchall()

            if not ofertas:
                raise HTTPException(status_code=404, detail="Produto sem ofertas")

            return {
                "produto": ofertas[0]["title"],
                "num_lojas": len(ofertas),
                "media": float(ofertas[0]["avg_price"]),
                "menor": float(ofertas[0]["min_price"]),
                "maior": float(ofertas[0]["max_price"]),
                "ofertas": ofertas,
            }


@app.get("/predict/{slug}")
def prever_preco(
    slug: str,
    dias: int = Query(30, ge=1, le=365, description="Quantos dias prever"),
):
    """Previsão de preço pra X dias no futuro usando Prophet."""
    safe_slug = slug[:100].replace("/", "_")
    model_path = f"{MODELS_DIR}/prophet_{safe_slug}.joblib"

    try:
        model = joblib.load(model_path)
    except FileNotFoundError:
        raise HTTPException(
            status_code=503,
            detail="Modelo ainda não treinado para esse produto. "
                   "Precisa de pelo menos 2 coletas em dias diferentes.",
        )

    # Busca preço atual
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (ph.product_id)
                    p.title, ph.avg_price, ph.min_price, ph.max_price, ph.num_stores
                FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE p.slug = %s AND ph.avg_price > 0
                ORDER BY ph.product_id, ph.collected_at DESC
                """,
                (slug,),
            )
            atual = cur.fetchone()

    if not atual:
        raise HTTPException(status_code=404, detail="Sem dados para esse produto")

    # Gera previsões pro período solicitado
    import pandas as pd
    future = model.make_future_dataframe(periods=dias, freq="D")
    forecast = model.predict(future)

    # Pega só as datas futuras
    from datetime import datetime
    hoje = datetime.now().date()
    futuro = forecast[forecast["ds"].dt.date > hoje][["ds", "yhat", "yhat_lower", "yhat_upper"]]

    # Monta resposta com marcos (7, 14, 30, 60, 90 dias)
    preco_atual = float(atual["avg_price"])
    previsoes = []
    marcos = [7, 14, 30, 60, 90]

    for d in marcos:
        if d > dias:
            break
        if d <= len(futuro):
            row = futuro.iloc[d - 1]
            previsto = round(float(row["yhat"]), 2)
            variacao = round((previsto - preco_atual) / preco_atual * 100, 2) if preco_atual > 0 else 0
            previsoes.append({
                "dia": d,
                "data": row["ds"].strftime("%Y-%m-%d"),
                "preco_previsto": previsto,
                "preco_minimo": round(float(row["yhat_lower"]), 2),
                "preco_maximo": round(float(row["yhat_upper"]), 2),
                "variacao_pct": variacao,
            })

    # Previsão final (dia solicitado)
    if len(futuro) >= dias:
        row_final = futuro.iloc[dias - 1]
        previsto_final = round(float(row_final["yhat"]), 2)
        variacao_final = round((previsto_final - preco_atual) / preco_atual * 100, 2) if preco_atual > 0 else 0
    else:
        previsto_final = previsoes[-1]["preco_previsto"] if previsoes else preco_atual
        variacao_final = previsoes[-1]["variacao_pct"] if previsoes else 0

    # Tendência
    if variacao_final < -5:
        tendencia = "queda"
        recomendacao = f"Esperar — preco deve cair {abs(variacao_final)}% em {dias} dias"
    elif variacao_final > 5:
        tendencia = "alta"
        recomendacao = f"Comprar agora — preco deve subir {variacao_final}% em {dias} dias"
    else:
        tendencia = "estavel"
        recomendacao = f"Preco deve se manter estavel nos proximos {dias} dias"

    return {
        "produto": atual["title"],
        "slug": slug,
        "preco_medio_atual": preco_atual,
        "menor_preco_atual": round(float(atual["min_price"]), 2),
        "maior_preco_atual": round(float(atual["max_price"]), 2),
        "num_lojas": atual["num_stores"],
        "dias_previsao": dias,
        "preco_previsto_final": previsto_final,
        "variacao_prevista_pct": variacao_final,
        "tendencia": tendencia,
        "recomendacao": recomendacao,
        "previsoes": previsoes,
    }
