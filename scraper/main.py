import psycopg2
import os
import re
import random
import requests
from bs4 import BeautifulSoup
import schedule
import time
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("scraper")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

CATEGORY_QUERIES = {
    "ipad": "ipad apple",
    "macbook": "macbook",
    "apple-watch": "apple watch",
    "airpods": "airpods",
    "imac": "imac",
    "monitores": "monitor gamer",
    "wella-oil": "Wella Professionals Oil Reflections Oleo Capilar 100ml",
}


def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def conectar():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def parse_preco(text):
    """Converte 'R$ 1.299,90' pra float 1299.90."""
    if not text:
        return None
    match = re.search(r"R\$\s*([\d.]+,\d{2})", text)
    if not match:
        return None
    limpo = match.group(1).replace(".", "").replace(",", ".")
    try:
        return float(limpo)
    except ValueError:
        return None


def extrair_slug(url):
    """Extrai o slug do produto da URL do Buscapé."""
    # https://www.buscape.com.br/tablet-ipad/tablet-apple-ipad-11-... -> tablet-apple-ipad-11-...
    match = re.search(r"buscape\.com\.br/[^/]+/([^?]+)", url)
    if match:
        return match.group(1)[:200]
    return None


def buscar_links_produtos(query, max_paginas=2):
    """Busca a listagem do Buscapé e retorna os links das páginas de produto."""
    links = []

    for pagina in range(1, max_paginas + 1):
        url = f"https://www.buscape.com.br/search?q={query.replace(' ', '+')}&page={pagina}"

        try:
            resp = requests.get(url, headers=get_headers(), timeout=30)
            resp.raise_for_status()
        except Exception as e:
            log.warning("Erro ao buscar listagem: %s", e)
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        cards = soup.select("[data-testid='product-card']")

        for card in cards:
            link_el = card.select_one("a[href]")
            if not link_el:
                continue
            href = link_el.get("href", "")
            if "/lead" in href:
                continue
            if not href.startswith("http"):
                href = "https://www.buscape.com.br" + href
            # Remove query params
            href = href.split("?")[0]
            if href not in links:
                links.append(href)

        log.info("Query '%s', página %d: %d links coletados", query, pagina, len(cards))
        time.sleep(random.uniform(3, 6))

    return links


def coletar_produto(url):
    """Entra na página de um produto e coleta preços de todas as lojas."""
    try:
        resp = requests.get(url, headers=get_headers(), timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Erro ao acessar produto %s: %s", url, e)
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    title_el = soup.select_one("h1")
    if not title_el:
        return None

    title = title_el.text.strip()
    slug = extrair_slug(url)
    if not slug:
        return None

    # Imagem do produto
    img_el = soup.select_one("[class*=Hero] img, [class*=gallery] img, .swiper img")
    thumbnail = img_el.get("src", "") if img_el else ""

    # Coleta preços de cada oferta (loja)
    offers = soup.select("[class*=OfferCardMin_OfferCardWrapper]")
    precos = []

    for offer in offers:
        text = offer.get_text()
        price = parse_preco(text)
        if price and price > 0:
            # Tenta extrair nome da loja da imagem alt ou texto
            store_img = offer.select_one("img")
            store_name = "Desconhecida"
            if store_img and store_img.get("alt"):
                store_name = store_img.get("alt", "")[:200]

            precos.append({
                "store": store_name,
                "price": price,
            })

    if not precos:
        return None

    # Calcula estatísticas
    prices_list = [p["price"] for p in precos]
    # Remove duplicatas (mesma loja aparece 2x)
    seen = set()
    unique_precos = []
    for p in precos:
        key = f"{p['store']}_{p['price']}"
        if key not in seen:
            seen.add(key)
            unique_precos.append(p)

    unique_prices = [p["price"] for p in unique_precos]
    avg_price = sum(unique_prices) / len(unique_prices)
    min_price = min(unique_prices)
    max_price = max(unique_prices)

    return {
        "title": title,
        "slug": slug,
        "permalink": url,
        "thumbnail": thumbnail,
        "offers": unique_precos,
        "avg_price": round(avg_price, 2),
        "min_price": min_price,
        "max_price": max_price,
        "num_stores": len(unique_precos),
    }


def salvar_produto(conn, produto, category):
    """Insere/atualiza produto e retorna o id interno."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO products (slug, title, category, permalink, thumbnail)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (slug) DO UPDATE SET
                title = EXCLUDED.title,
                updated_at = NOW()
            RETURNING id
        """, (
            produto["slug"],
            produto["title"],
            category,
            produto["permalink"],
            produto["thumbnail"],
        ))
        return cur.fetchone()[0]


def salvar_precos(conn, product_id, produto):
    """Salva o preço de cada loja no histórico."""
    with conn.cursor() as cur:
        for oferta in produto["offers"]:
            cur.execute("""
                INSERT INTO price_history
                    (product_id, store_name, price, avg_price, min_price, max_price, num_stores)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                product_id,
                oferta["store"],
                oferta["price"],
                produto["avg_price"],
                produto["min_price"],
                produto["max_price"],
                produto["num_stores"],
            ))


def coletar():
    """Executa uma rodada completa de coleta."""
    categorias = os.getenv(
        "SCRAPER_CATEGORIES",
        "ipad,macbook,apple-watch,airpods,imac,monitores,placas-de-video",
    ).split(",")
    log.info("Iniciando coleta para categorias: %s", categorias)

    conn = conectar()
    total_produtos = 0
    total_ofertas = 0

    try:
        for cat in categorias:
            cat = cat.strip()
            query = CATEGORY_QUERIES.get(cat)
            if not query:
                log.warning("Categoria %s sem query mapeada", cat)
                continue

            links = buscar_links_produtos(query)
            log.info("Categoria %s: %d páginas de produto encontradas", cat, len(links))

            for link in links:
                try:
                    produto = coletar_produto(link)
                    if not produto:
                        continue

                    product_id = salvar_produto(conn, produto, cat)
                    salvar_precos(conn, product_id, produto)
                    total_produtos += 1
                    total_ofertas += produto["num_stores"]

                    log.info(
                        "  %s: %d lojas, R$%.2f - R$%.2f (média R$%.2f)",
                        produto["title"][:40],
                        produto["num_stores"],
                        produto["min_price"],
                        produto["max_price"],
                        produto["avg_price"],
                    )

                except Exception as e:
                    log.warning("Erro ao salvar produto: %s", e)
                    conn.rollback()
                    continue

                time.sleep(random.uniform(2, 4))

            conn.commit()
            log.info("Categoria %s commitada", cat)
            time.sleep(random.uniform(5, 10))

    except Exception as e:
        log.error("Erro na coleta: %s", e)
        conn.rollback()
    finally:
        conn.close()

    log.info("Coleta finalizada: %d produtos, %d ofertas", total_produtos, total_ofertas)


def main():
    log.info("=== Price Monitor Scraper iniciado (Buscape) ===")
    coletar()

    intervalo = int(os.getenv("COLLECTION_INTERVAL_HOURS", "6"))
    schedule.every(intervalo).hours.do(coletar)
    log.info("Agendado: coleta a cada %d horas", intervalo)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
