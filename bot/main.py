import os
import locale
import logging
import time
import schedule
import requests
import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("discord-bot")

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")


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


def alerta_anomalias():
    """Busca anomalias recentes e envia alertas no Discord."""
    conn = conectar()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Busca anomalias das últimas 7 horas (janela do intervalo de coleta + margem)
            cur.execute("""
                SELECT a.*, p.permalink, p.thumbnail
                FROM anomalies a
                JOIN products p ON p.ml_id = a.ml_id
                WHERE a.detected_at > NOW() - INTERVAL '7 hours'
                ORDER BY a.anomaly_score ASC
                LIMIT 10
            """)
            anomalias = cur.fetchall()
    except Exception as e:
        log.warning("Tabela anomalies ainda não existe ou erro: %s", e)
        conn.close()
        return
    finally:
        conn.close()

    if not anomalias:
        log.info("Nenhuma anomalia recente para alertar")
        return

    embeds = []
    for a in anomalias[:5]:  # Máximo 5 embeds por mensagem
        preco = float(a["price"])
        preco_fmt = f"R$ {preco:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        score = float(a["anomaly_score"])
        if score < -0.3:
            nivel = "🔴 Preco muito fora do padrao"
        elif score < -0.2:
            nivel = "🟠 Preco incomum"
        elif score < -0.1:
            nivel = "🟡 Variacao atipica"
        else:
            nivel = "🟢 Possivel oportunidade"

        embed = {
            "title": f"⚠️ {a['title'][:80]}",
            "url": a.get("permalink", ""),
            "color": 15158332,  # vermelho
            "fields": [
                {"name": "Preço", "value": preco_fmt, "inline": True},
                {"name": "Nível", "value": nivel, "inline": True},
                {"name": "ML ID", "value": a["ml_id"], "inline": True},
            ],
            "thumbnail": {"url": a.get("thumbnail", "")},
        }
        embeds.append(embed)

    enviar_webhook(embeds)
    log.info("Alertas enviados: %d anomalias", len(embeds))


def resumo_diario():
    """Envia um resumo diário no Discord."""
    conn = conectar()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) as total FROM products")
            total_produtos = cur.fetchone()["total"]

            cur.execute(
                "SELECT COUNT(*) as total FROM price_history WHERE collected_at > NOW() - INTERVAL '24 hours'"
            )
            coletas_24h = cur.fetchone()["total"]

            try:
                cur.execute(
                    "SELECT COUNT(*) as total FROM anomalies WHERE detected_at > NOW() - INTERVAL '24 hours'"
                )
                anomalias_24h = cur.fetchone()["total"]
            except Exception:
                conn.rollback()
                anomalias_24h = 0

            # Top 5 maiores quedas de preço nas últimas 24h
            cur.execute("""
                SELECT p.title, p.ml_id,
                       MIN(ph.price) as menor_preco,
                       MAX(ph.price) as maior_preco
                FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE ph.collected_at > NOW() - INTERVAL '24 hours'
                GROUP BY p.id, p.title, p.ml_id
                HAVING MAX(ph.price) - MIN(ph.price) > 0
                ORDER BY (MAX(ph.price) - MIN(ph.price)) DESC
                LIMIT 5
            """)
            maiores_variacoes = cur.fetchall()
    finally:
        conn.close()

    embed = {
        "title": "📊 Resumo Diário — Price Monitor",
        "color": 3447003,  # azul
        "fields": [
            {"name": "Produtos monitorados", "value": str(total_produtos), "inline": True},
            {"name": "Coletas (24h)", "value": str(coletas_24h), "inline": True},
            {"name": "Anomalias (24h)", "value": str(anomalias_24h), "inline": True},
        ],
    }

    if maiores_variacoes:
        variacao_text = ""
        for v in maiores_variacoes:
            diff = float(v["maior_preco"]) - float(v["menor_preco"])
            diff_fmt = f"R$ {diff:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            variacao_text += f"• {v['title'][:50]}: {diff_fmt}\n"
        embed["fields"].append({
            "name": "Maiores variações (24h)",
            "value": variacao_text[:1024],
            "inline": False,
        })

    enviar_webhook([embed])
    log.info("Resumo diário enviado")


def main():
    log.info("=== Discord Bot iniciado ===")

    # Espera ML pipeline rodar ao menos uma vez
    log.info("Aguardando 5 min para pipeline ML processar dados...")
    time.sleep(300)

    # Roda alertas logo após iniciar
    alerta_anomalias()

    # Agenda alertas a cada 6 horas e resumo diário
    intervalo = int(os.getenv("COLLECTION_INTERVAL_HOURS", "6"))
    schedule.every(intervalo).hours.do(alerta_anomalias)
    schedule.every().day.at("09:00").do(resumo_diario)

    log.info("Agendado: alertas a cada %dh, resumo diário às 09:00", intervalo)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
