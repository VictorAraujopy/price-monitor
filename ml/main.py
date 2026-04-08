import os
import logging
import schedule
import time
import psycopg2
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import xgboost as xgb
import joblib
from datetime import datetime, timedelta

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
    """Carrega histórico de preços do banco em um DataFrame."""
    conn = conectar()
    query = """
        SELECT
            ph.product_id,
            p.ml_id,
            p.title,
            p.category_id,
            ph.price,
            ph.original_price,
            ph.available_quantity,
            ph.sold_quantity,
            ph.collected_at
        FROM price_history ph
        JOIN products p ON p.id = ph.product_id
        ORDER BY ph.product_id, ph.collected_at
    """
    df = pd.read_sql(query, conn)
    conn.close()
    log.info("Dados carregados: %d registros", len(df))
    return df


def criar_features(df):
    """Feature engineering: cria variáveis derivadas para o ML."""
    if df.empty:
        return df

    df = df.copy()
    df["collected_at"] = pd.to_datetime(df["collected_at"])

    # Desconto percentual
    df["discount_pct"] = np.where(
        df["original_price"].notna() & (df["original_price"] > 0),
        ((df["original_price"] - df["price"]) / df["original_price"]) * 100,
        0,
    )

    # Features temporais
    df["hour"] = df["collected_at"].dt.hour
    df["day_of_week"] = df["collected_at"].dt.dayofweek
    df["day_of_month"] = df["collected_at"].dt.day

    # Variação de preço por produto (em relação à coleta anterior)
    df = df.sort_values(["product_id", "collected_at"])
    df["price_prev"] = df.groupby("product_id")["price"].shift(1)
    df["price_change"] = df["price"] - df["price_prev"]
    df["price_change_pct"] = np.where(
        df["price_prev"].notna() & (df["price_prev"] > 0),
        (df["price_change"] / df["price_prev"]) * 100,
        0,
    )

    # Média móvel de preço (últimas 3 coletas)
    df["price_ma3"] = df.groupby("product_id")["price"].transform(
        lambda x: x.rolling(3, min_periods=1).mean()
    )

    # Razão vendas/disponível
    df["sell_ratio"] = np.where(
        (df["available_quantity"].notna()) & (df["available_quantity"] > 0),
        df["sold_quantity"] / (df["available_quantity"] + df["sold_quantity"]),
        0,
    )

    return df


def detectar_anomalias(df):
    """Usa Isolation Forest para detectar preços anômalos."""
    if len(df) < 10:
        log.warning("Poucos dados para detecção de anomalias (%d registros)", len(df))
        df["anomaly"] = 0
        df["anomaly_score"] = 0.0
        return df

    features = ["price", "discount_pct", "price_change_pct", "sell_ratio"]
    X = df[features].fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,  # espera ~5% de anomalias
        random_state=42,
    )
    df["anomaly"] = model.fit_predict(X_scaled)  # 1 = normal, -1 = anomalia
    df["anomaly_score"] = model.decision_function(X_scaled)

    # Salva modelos
    joblib.dump(model, f"{MODELS_DIR}/isolation_forest.joblib")
    joblib.dump(scaler, f"{MODELS_DIR}/scaler_anomaly.joblib")

    n_anomalias = (df["anomaly"] == -1).sum()
    log.info("Anomalias detectadas: %d de %d registros", n_anomalias, len(df))

    return df


def treinar_forecasting(df):
    """Treina modelo XGBoost para previsão de preço."""
    if len(df) < 20:
        log.warning("Poucos dados para treinar forecasting (%d registros)", len(df))
        return None

    feature_cols = [
        "discount_pct",
        "hour", "day_of_week", "day_of_month",
        "price_change_pct", "price_ma3", "sell_ratio",
    ]

    df_train = df.dropna(subset=["price"]).copy()
    if len(df_train) < 20:
        log.warning("Dados insuficientes após limpeza")
        return None

    X = df_train[feature_cols].fillna(0)
    y = df_train["price"]

    model = xgb.XGBRegressor(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
    )
    model.fit(X, y)

    # Salva modelo
    joblib.dump(model, f"{MODELS_DIR}/xgb_forecast.joblib")
    log.info("Modelo XGBoost treinado e salvo (R² treino: %.3f)", model.score(X, y))

    return model


def salvar_anomalias_no_banco(df):
    """Salva anomalias detectadas em uma tabela no banco."""
    anomalias = df[df["anomaly"] == -1].copy()
    if anomalias.empty:
        log.info("Nenhuma anomalia para salvar")
        return

    conn = conectar()
    with conn.cursor() as cur:
        for _, row in anomalias.iterrows():
            cur.execute("""
                INSERT INTO anomalies (product_id, ml_id, title, price, anomaly_score)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                int(row["product_id"]),
                row["ml_id"],
                row["title"],
                float(row["price"]),
                float(row["anomaly_score"]),
            ))

    conn.commit()
    conn.close()
    log.info("Salvas %d anomalias no banco", len(anomalias))


def pipeline():
    """Executa o pipeline completo de ML."""
    log.info("=== Iniciando pipeline ML ===")

    # 1. Carregar dados
    df = carregar_dados()
    if df.empty:
        log.warning("Sem dados no banco. Aguardando coletas do scraper.")
        return

    # 2. Feature engineering
    df = criar_features(df)
    log.info("Features criadas: %s", list(df.columns))

    # 3. Detecção de anomalias
    df = detectar_anomalias(df)

    # 4. Treinar modelo de previsão
    treinar_forecasting(df)

    # 5. Salvar anomalias no banco
    salvar_anomalias_no_banco(df)

    log.info("=== Pipeline ML concluído ===")


def main():
    os.makedirs(MODELS_DIR, exist_ok=True)

    # Espera o scraper fazer pelo menos uma coleta antes de rodar
    log.info("ML Pipeline iniciado. Aguardando 2 min para primeira coleta do scraper...")
    time.sleep(120)

    pipeline()

    # Roda o pipeline a cada 6 horas (logo após o scraper)
    intervalo = int(os.getenv("COLLECTION_INTERVAL_HOURS", "6"))
    schedule.every(intervalo).hours.do(pipeline)
    log.info("Agendado: pipeline a cada %d horas", intervalo)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
