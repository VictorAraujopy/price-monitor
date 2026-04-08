import os
import logging
import time
import schedule
import requests
import psycopg2
import psycopg2.extras
import joblib
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("discord-bot")

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
MODELS_DIR = "/app/models"


def get_insight(slug, preco_atual):
    """Tenta carregar o Prophet e gerar insight de tendência."""
    safe_slug = slug[:100].replace("/", "_")
    model_path = f"{MODELS_DIR}/prophet_{safe_slug}.joblib"

    try:
        model = joblib.load(model_path)
        future = model.make_future_dataframe(periods=30, freq="D")
        forecast = model.predict(future)

        from datetime import datetime
        hoje = datetime.now().date()
        futuro = forecast[forecast["ds"].dt.date > hoje]

        if len(futuro) < 7:
            return None

        preco_7d = float(futuro.iloc[6]["yhat"])
        preco_30d = float(futuro.iloc[min(29, len(futuro) - 1)]["yhat"])

        var_7d = round((preco_7d - preco_atual) / preco_atual * 100, 1)
        var_30d = round((preco_30d - preco_atual) / preco_atual * 100, 1)

        if var_30d < -5:
            return f"📉 Tendencia de queda: {var_30d}% em 30 dias. Espere pra comprar."
        elif var_30d > 5:
            return f"📈 Tendencia de alta: +{var_30d}% em 30 dias. Compre agora!"
        else:
            return f"➡️ Preco estavel nos proximos 30 dias ({var_30d}%)"

    except Exception:
        return None


def conectar():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def enviar_webhook(embeds):
    """Envia uma mensagem com embeds para o webhook do Discord."""
    if not WEBHOOK_URL or "your/webhook" in WEBHOOK_URL:
        log.warning("DISCORD_WEBHOOK_URL não configurado, pulando envio")
        return

    payload = {
        "username": "Price Monitor",
        "embeds": embeds,
    }

    try:
        resp = requests.post(WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        log.info("Webhook enviado com sucesso")
    except Exception as e:
        log.error("Erro ao enviar webhook: %s", e)


def fmt_preco(valor):
    """Formata float pra R$ brasileiro."""
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def montar_embed(a):
    """Monta um embed de anomalia."""
    preco = float(a["price"])
    media = float(a["avg_price"])
    desconto = float(a["discount_pct"])

    if desconto < -30:
        nivel = "🔴 Muito abaixo do mercado"
        color = 15158332
    elif desconto < -20:
        nivel = "🟠 Bem abaixo do mercado"
        color = 15105570
    else:
        nivel = "🟡 Abaixo do mercado"
        color = 16776960

    fields = [
        {"name": "Preço", "value": fmt_preco(preco), "inline": True},
        {"name": "Média mercado", "value": fmt_preco(media), "inline": True},
        {"name": "Desconto", "value": f"{round(desconto, 1)}%", "inline": True},
        {"name": "Nível", "value": nivel, "inline": True},
    ]

    insight = get_insight(a.get("slug", ""), media)
    if insight:
        fields.append({"name": "Previsão", "value": insight, "inline": False})

    return {
        "title": f"💰 {a['title'][:80]}",
        "url": a.get("permalink", ""),
        "color": color,
        "fields": fields,
        "thumbnail": {"url": a.get("thumbnail", "")},
    }


def alerta_anomalias():
    """Busca TODAS as anomalias não notificadas e envia em lotes no Discord."""
    conn = conectar()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT a.*, p.permalink, p.thumbnail, p.slug
                FROM anomalies a
                JOIN products p ON p.id = a.product_id
                WHERE a.notified = FALSE
                ORDER BY a.discount_pct ASC
            """)
            anomalias = cur.fetchall()
    except Exception as e:
        log.warning("Erro ao buscar anomalias: %s", e)
        conn.close()
        return False

    if not anomalias:
        log.info("Nenhuma anomalia nova para alertar")
        conn.close()
        return False

    # Monta todos os embeds
    embeds = [montar_embed(a) for a in anomalias]

    # Envia em lotes de 5 (limite do Discord por mensagem)
    total_enviado = 0
    for i in range(0, len(embeds), 5):
        lote = embeds[i:i + 5]
        enviar_webhook(lote)
        total_enviado += len(lote)
        if i + 5 < len(embeds):
            time.sleep(2)  # delay entre lotes pra não bater rate limit

    # Marca TODAS como notificadas
    try:
        with conn.cursor() as cur:
            ids = [a["id"] for a in anomalias]
            cur.execute(
                "UPDATE anomalies SET notified = TRUE WHERE id = ANY(%s)",
                (ids,),
            )
        conn.commit()
    except Exception as e:
        log.warning("Erro ao marcar notificadas: %s", e)
    finally:
        conn.close()

    log.info("Alertas enviados: %d anomalias em %d mensagens", total_enviado, (len(embeds) + 4) // 5)
    return True


def resumo_diario():
    """Envia um resumo diário no Discord."""
    conn = conectar()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(DISTINCT slug) as total FROM products")
            total_produtos = cur.fetchone()["total"]

            cur.execute(
                "SELECT COUNT(*) as total FROM price_history "
                "WHERE collected_at > NOW() - INTERVAL '24 hours'"
            )
            ofertas_24h = cur.fetchone()["total"]

            cur.execute("SELECT COUNT(*) as total FROM anomalies")
            total_anomalias = cur.fetchone()["total"]

            # Top 5 maiores descontos ativos
            cur.execute("""
                SELECT title, store_name, price, avg_price, discount_pct
                FROM anomalias
                ORDER BY discount_pct ASC
                LIMIT 5
            """)
            top_descontos = cur.fetchall()
    except Exception as e:
        log.warning("Erro no resumo: %s", e)
        # Tenta query simplificada se anomalias não existe
        try:
            top_descontos = []
        except:
            pass
    finally:
        conn.close()

    embed = {
        "title": "📊 Resumo Diário — Price Monitor",
        "color": 3447003,
        "fields": [
            {"name": "Produtos monitorados", "value": str(total_produtos), "inline": True},
            {"name": "Ofertas coletadas (24h)", "value": str(ofertas_24h), "inline": True},
            {"name": "Anomalias ativas", "value": str(total_anomalias), "inline": True},
        ],
    }

    if top_descontos:
        desc_text = ""
        for d in top_descontos:
            desc_text += f"• {d['title'][:40]}: {fmt_preco(float(d['price']))} ({round(float(d['discount_pct']), 1)}%)\n"
        embed["fields"].append({
            "name": "Top descontos",
            "value": desc_text[:1024],
            "inline": False,
        })

    enviar_webhook([embed])
    log.info("Resumo diário enviado")


def main():
    log.info("=== Discord Bot iniciado ===")

    log.info("Aguardando 45 min para scraper + ML processarem...")
    time.sleep(2700)

    # Tenta enviar alertas. Se não tiver, retry 1x depois de 5 min
    enviou = alerta_anomalias()
    if not enviou:
        log.info("Sem anomalias. Retry em 5 min...")
        time.sleep(300)
        alerta_anomalias()

    intervalo = int(os.getenv("COLLECTION_INTERVAL_HOURS", "6"))
    schedule.every(intervalo).hours.do(alerta_anomalias)
    schedule.every().day.at("09:00").do(resumo_diario)

    log.info("Agendado: alertas a cada %dh, resumo diário às 09:00", intervalo)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
