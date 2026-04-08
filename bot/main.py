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

MODELS_DIR = "/app/models"

# Carrega canais do .env
CANAIS = []
for prefix in ["APPLE", "BELEZA"]:
    webhook = os.getenv(f"WEBHOOK_{prefix}", "")
    cargo = os.getenv(f"CARGO_{prefix}", "")
    categorias = os.getenv(f"CATEGORIES_{prefix}", "")
    if webhook and "COLE_O_WEBHOOK" not in webhook:
        CANAIS.append({
            "nome": prefix.lower(),
            "webhook": webhook,
            "cargo": cargo,
            "categorias": [c.strip() for c in categorias.split(",") if c.strip()],
        })

log.info("Canais configurados: %s", [c["nome"] for c in CANAIS])


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


def enviar_webhook(webhook_url, embeds, cargo_id=None):
    """Envia embeds pro webhook do Discord, marcando o cargo se configurado."""
    if not webhook_url:
        return

    payload = {
        "username": "Price Monitor",
        "embeds": embeds,
    }

    # Marca o cargo na mensagem
    if cargo_id:
        payload["content"] = f"<@&{cargo_id}>"

    try:
        resp = requests.post(webhook_url, json=payload, timeout=10)
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


def encontrar_canal(category):
    """Encontra qual canal deve receber alertas dessa categoria."""
    for canal in CANAIS:
        if category in canal["categorias"]:
            return canal
    return None


def montar_embed_oferta(produto):
    """Monta embed pra melhor oferta (categorias sem anomalias, ex: wella-oil)."""
    preco = float(produto["min_price"])
    media = float(produto["avg_price"])
    desconto = round((preco - media) / media * 100, 1) if media > 0 else 0

    fields = [
        {"name": "Menor preço", "value": fmt_preco(preco), "inline": True},
        {"name": "Média mercado", "value": fmt_preco(media), "inline": True},
        {"name": "Diferença", "value": f"{desconto}%", "inline": True},
        {"name": "Lojas", "value": str(produto["num_stores"]), "inline": True},
    ]

    insight = get_insight(produto.get("slug", ""), media)
    if insight:
        fields.append({"name": "Previsão", "value": insight, "inline": False})

    return {
        "title": f"🛒 {produto['title'][:80]}",
        "url": produto.get("permalink", ""),
        "color": 3066993,  # Verde
        "fields": fields,
        "thumbnail": {"url": produto.get("thumbnail", "")},
    }


def buscar_melhores_ofertas(conn, categorias):
    """Busca a melhor oferta de cada produto nas categorias sem anomalias."""
    if not categorias:
        return []

    placeholders = ",".join(["%s"] * len(categorias))
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT DISTINCT ON (p.id)
                    p.title, p.slug, p.permalink, p.thumbnail, p.category,
                    ph.price as min_price, ph.avg_price, ph.num_stores
                FROM price_history ph
                JOIN products p ON p.id = ph.product_id
                WHERE p.category IN ({placeholders})
                ORDER BY p.id, ph.collected_at DESC, ph.price ASC
            """, categorias)
            return cur.fetchall()
    except Exception as e:
        log.warning("Erro ao buscar melhores ofertas: %s", e)
        return []


def alerta_anomalias():
    """Busca anomalias não notificadas e envia pro canal certo baseado na categoria."""
    conn = conectar()

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # DISTINCT ON (product_id) pega só a melhor oferta por produto
            cur.execute("""
                SELECT DISTINCT ON (a.product_id)
                    a.*, p.permalink, p.thumbnail, p.slug, p.category
                FROM anomalies a
                JOIN products p ON p.id = a.product_id
                WHERE a.notified = FALSE
                ORDER BY a.product_id, a.discount_pct ASC
            """)
            anomalias = cur.fetchall()
    except Exception as e:
        log.warning("Erro ao buscar anomalias: %s", e)
        conn.close()
        return False

    # Agrupa anomalias por canal
    por_canal = {}
    categorias_com_anomalia = set()
    for a in anomalias:
        canal = encontrar_canal(a.get("category", ""))
        if not canal:
            continue
        categorias_com_anomalia.add(a["category"])
        nome = canal["nome"]
        if nome not in por_canal:
            por_canal[nome] = {"canal": canal, "embeds": []}
        por_canal[nome]["embeds"].append(montar_embed(a))

    # Pra canais que não tiveram anomalias, envia melhores ofertas
    for canal in CANAIS:
        cats_sem_anomalia = [c for c in canal["categorias"] if c not in categorias_com_anomalia]
        if cats_sem_anomalia:
            ofertas = buscar_melhores_ofertas(conn, cats_sem_anomalia)
            if ofertas:
                nome = canal["nome"]
                if nome not in por_canal:
                    por_canal[nome] = {"canal": canal, "embeds": []}
                enviadas = 0
                for o in ofertas:
                    # Só envia se tem diferença real (min_price < avg_price)
                    min_p = float(o["min_price"])
                    avg_p = float(o["avg_price"])
                    if avg_p > 0 and min_p < avg_p * 0.97:  # pelo menos 3% abaixo
                        por_canal[nome]["embeds"].append(montar_embed_oferta(o))
                        enviadas += 1
                if enviadas:
                    log.info("Canal %s: %d melhores ofertas (sem anomalias)", nome, enviadas)

    if not por_canal:
        log.info("Nenhuma anomalia ou oferta para alertar")
        conn.close()
        return False

    # Envia pra cada canal em lotes de 5
    total_enviado = 0
    for nome, dados in por_canal.items():
        canal = dados["canal"]
        embeds = dados["embeds"]

        for i in range(0, len(embeds), 5):
            lote = embeds[i:i + 5]
            cargo = canal["cargo"] if i == 0 else None
            enviar_webhook(canal["webhook"], lote, cargo)
            total_enviado += len(lote)
            if i + 5 < len(embeds):
                time.sleep(2)

        log.info("Canal %s: %d alertas enviados", nome, len(embeds))

    # Marca anomalias como notificadas
    if anomalias:
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

    conn.close()
    log.info("Alertas enviados: %d total", total_enviado)
    return True


def resumo_diario():
    """Envia um resumo diário em TODOS os canais configurados."""
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
    except Exception as e:
        log.warning("Erro no resumo: %s", e)
        conn.close()
        return
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

    for canal in CANAIS:
        enviar_webhook(canal["webhook"], [embed])

    log.info("Resumo diário enviado em %d canais", len(CANAIS))


def main():
    log.info("=== Discord Bot iniciado ===")

    log.info("Aguardando 45 min para scraper + ML processarem...")
    time.sleep(2700)

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
