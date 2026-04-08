import os
import logging
import schedule
import time
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from prophet import Prophet
import joblib

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("ml-pipeline")

MODELS_DIR = "/app/models"


def conectar():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def carregar_dados():
    """Carrega dados do banco em DataFrame."""
    conn = conectar()
    query = """
        SELECT
            ph.product_id,
            p.title,
            p.slug,
            p.category,
            ph.store_name,
            ph.price,
            ph.avg_price,
            ph.min_price,
            ph.max_price,
            ph.num_stores,
            ph.collected_at
        FROM price_history ph
        JOIN products p ON p.id = ph.product_id
        ORDER BY p.category, ph.product_id, ph.collected_at
    """
    df = pd.read_sql(query, conn)
    conn.close()
    log.info("Dados carregados: %d registros", len(df))
    return df


def criar_features(df):
    """Feature engineering com dados de múltiplas lojas."""
    if df.empty:
        return df

    df = df.copy()
    df["collected_at"] = pd.to_datetime(df["collected_at"])

    # Desconto em relação à média do mercado
    df["discount_pct"] = np.where(
        df["avg_price"].notna() & (df["avg_price"] > 0),
        ((df["price"] - df["avg_price"]) / df["avg_price"]) * 100,
        0,
    )

    # Amplitude de preço no mercado
    df["price_spread"] = np.where(
        df["avg_price"].notna() & (df["avg_price"] > 0),
        ((df["max_price"] - df["min_price"]) / df["avg_price"]) * 100,
        0,
    )

    # Número de lojas normalizado
    max_stores = df["num_stores"].max()
    df["num_stores_norm"] = df["num_stores"].fillna(1) / (max_stores if max_stores > 0 else 1)

    df = df.fillna(0)
    return df


def detectar_anomalias_por_categoria(df):
    """Isolation Forest POR CATEGORIA — compara produtos similares."""
    if len(df) < 10:
        log.warning("Poucos dados para anomalias (%d registros)", len(df))
        df["anomaly"] = 0
        df["anomaly_score"] = 0.0
        return df

    all_results = []

    for category, group in df.groupby("category"):
        if len(group) < 5:
            group = group.copy()
            group["anomaly"] = 0
            group["anomaly_score"] = 0.0
            all_results.append(group)
            continue

        features = ["price", "discount_pct", "price_spread", "num_stores_norm"]
        X = group[features].fillna(0)

        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        model = IsolationForest(
            n_estimators=100,
            contamination=0.05,
            random_state=42,
        )
        group = group.copy()
        group["anomaly"] = model.fit_predict(X_scaled)
        group["anomaly_score"] = model.decision_function(X_scaled)

        n_anomalias = (group["anomaly"] == -1).sum()
        log.info("Categoria %s: %d anomalias de %d registros", category, n_anomalias, len(group))

        joblib.dump(model, f"{MODELS_DIR}/isolation_forest_{category}.joblib")
        joblib.dump(scaler, f"{MODELS_DIR}/scaler_{category}.joblib")

        all_results.append(group)

    return pd.concat(all_results, ignore_index=True)


def treinar_prophet(df):
    """
    Treina um modelo Prophet por produto pra previsão de preço de longo prazo.
    Prophet precisa de pelo menos 2 pontos de dados por produto.
    Com poucos dados, salva o que dá. Com mais dados, fica preciso.
    """
    df["collected_at"] = pd.to_datetime(df["collected_at"])

    # Agrupa por produto: média diária do avg_price
    produtos = df.groupby("slug").agg(
        product_id=("product_id", "first"),
        title=("title", "first"),
        category=("category", "first"),
    ).reset_index()

    modelos_treinados = 0

    for _, prod in produtos.iterrows():
        slug = prod["slug"]

        # Pega série temporal do preço médio desse produto
        serie = df[df["slug"] == slug][["collected_at", "avg_price"]].copy()
        serie = serie[serie["avg_price"] > 0]

        # Prophet precisa de pelo menos 2 datas diferentes
        serie["date"] = serie["collected_at"].dt.date
        serie_diaria = serie.groupby("date")["avg_price"].mean().reset_index()
        serie_diaria.columns = ["ds", "y"]
        serie_diaria["ds"] = pd.to_datetime(serie_diaria["ds"])

        if len(serie_diaria) < 2:
            continue

        try:
            model = Prophet(
                daily_seasonality=False,
                weekly_seasonality=True if len(serie_diaria) >= 14 else False,
                yearly_seasonality=False,
                changepoint_prior_scale=0.05,
            )
            model.fit(serie_diaria)

            # Salva modelo por produto
            safe_slug = slug[:100].replace("/", "_")
            joblib.dump(model, f"{MODELS_DIR}/prophet_{safe_slug}.joblib")
            modelos_treinados += 1

        except Exception as e:
            log.warning("Erro ao treinar Prophet para %s: %s", slug[:40], e)
            continue

    log.info("Prophet: %d modelos treinados de %d produtos", modelos_treinados, len(produtos))


def salvar_anomalias(df):
    """Salva anomalias no banco — IF detectou E preço 10%+ abaixo da média."""
    anomalias = df[
        (df["anomaly"] == -1) & (df["discount_pct"] < -10)
    ].copy()

    if anomalias.empty:
        log.info("Nenhuma anomalia relevante para salvar")
        return

    conn = conectar()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM anomalies")

        for _, row in anomalias.iterrows():
            cur.execute("""
                INSERT INTO anomalies
                    (product_id, title, store_name, price, avg_price, discount_pct)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                int(row["product_id"]),
                row["title"],
                row["store_name"],
                float(row["price"]),
                float(row["avg_price"]),
                float(row["discount_pct"]),
            ))

    conn.commit()
    conn.close()
    log.info("Salvas %d anomalias no banco", len(anomalias))


def pipeline():
    """Pipeline completo."""
    log.info("=== Iniciando pipeline ML ===")

    while True:
        df = carregar_dados()
        if not df.empty:
            break
        log.warning("Sem dados no banco. Tentando novamente em 5 min...")
        time.sleep(300)

    df = criar_features(df)
    log.info("Features criadas: %d colunas", len(df.columns))

    df = detectar_anomalias_por_categoria(df)

    treinar_prophet(df)

    salvar_anomalias(df)

    log.info("=== Pipeline ML concluído ===")


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)

    log.info("ML Pipeline iniciado. Aguardando 30 min para coleta do scraper...")
    time.sleep(1800)

    pipeline()

    intervalo = int(os.getenv("COLLECTION_INTERVAL_HOURS", "6"))
    schedule.every(intervalo).hours.do(pipeline)
    log.info("Agendado: pipeline a cada %d horas", intervalo)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
