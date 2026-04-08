import psycopg2
import os
import re
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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# Mapeia categoria -> URL de listagem no Mercado Livre
CATEGORY_URLS = {
    "MLB1648": "https://lista.mercadolivre.com.br/informatica/",
    "MLB1051": "https://lista.mercadolivre.com.br/celulares-e-smartphones/",
    "MLB1672": "https://lista.mercadolivre.com.br/informatica/monitores-e-acessorios/",
    "MLB1694": "https://lista.mercadolivre.com.br/informatica/componentes-de-computadores/placas-de-video/",
    "MLB1652": "https://lista.mercadolivre.com.br/informatica/tablets/iPads/",
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
    """Converte texto de preço ('1.299' ou '599') pra float."""
    if not text:
        return None
    limpo = text.strip().replace(".", "").replace(",", ".")
    try:
        return float(limpo)
    except ValueError:
        return None


def extrair_ml_id(url):
    """Extrai o ID do produto (MLB...) da URL."""
    match = re.search(r"(MLB-?\d+)", url)
    if match:
        return match.group(1).replace("-", "")
    return None


def buscar_pagina(url):
    """Faz scraping de uma página de listagem do Mercado Livre."""
    produtos = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning("Erro ao acessar %s: %s", url, e)
        return produtos

    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select("li.ui-search-layout__item")

    for item in items:
        try:
            title_el = item.select_one(
                ".poly-component__title, .ui-search-item__title"
            )
            # Preço atual: dentro de .poly-price__current
            # Se não achar, pega o primeiro .andes-money-amount__fraction
            current_price_container = item.select_one(".poly-price__current")
            if current_price_container:
                price_el = current_price_container.select_one(
                    ".andes-money-amount__fraction"
                )
                cents_el = current_price_container.select_one(
                    ".andes-money-amount__cents"
                )
            else:
                price_el = item.select_one(".andes-money-amount__fraction")
                cents_el = item.select_one(".andes-money-amount__cents")

            # Preço original (riscado): dentro de <s>
            old_price_el = item.select_one(
                "s .andes-money-amount__fraction"
            )
            link_el = item.select_one("a[href]")
            img_el = item.select_one("img[data-src], img[src]")

            if not title_el or not price_el or not link_el:
                continue

            href = link_el.get("href", "")
            ml_id = extrair_ml_id(href)
            if not ml_id:
                continue

            # Monta o preco: fraction (inteiro) + cents (decimal)
            # fraction vem com ponto de milhar (ex: "1.299"), cents vem limpo (ex: "90")
            fraction_text = price_el.text.strip().replace(".", "")  # remove ponto de milhar
            if cents_el:
                price = float(f"{fraction_text}.{cents_el.text.strip()}")
            else:
                price = float(fraction_text)

            original_price = None
            if old_price_el:
                old_fraction = old_price_el.text.strip().replace(".", "")
                original_price = float(old_fraction)

            img_url = ""
            if img_el:
                img_url = img_el.get("data-src") or img_el.get("src", "")

            produtos.append({
                "id": ml_id,
                "title": title_el.text.strip(),
                "price": price,
                "original_price": original_price,
                "permalink": href.split("?")[0],  # remove tracking params
                "thumbnail": img_url,
                "condition": "new",
                "available_quantity": None,
                "sold_quantity": None,
            })

        except Exception as e:
            log.warning("Erro ao parsear item: %s", e)
            continue

    return produtos


def buscar_todos_produtos(category, max_paginas=4):
    """Busca múltiplas páginas de uma categoria."""
    base_url = CATEGORY_URLS.get(category)
    if not base_url:
        log.warning("Categoria %s sem URL mapeada, pulando", category)
        return []

    todos = []

    for pagina in range(max_paginas):
        if pagina == 0:
            url = base_url
        else:
            offset = pagina * 48 + 1
            url = f"{base_url}_Desde_{offset}"

        produtos = buscar_pagina(url)
        if not produtos:
            break

        todos.extend(produtos)
        log.info(
            "Categoria %s, página %d: %d produtos",
            category, pagina + 1, len(produtos),
        )

        # Pequeno delay entre páginas pra não sobrecarregar
        time.sleep(2)

    log.info("Categoria %s: %d produtos coletados no total", category, len(todos))
    return todos


def salvar_produto(conn, item, category_id):
    """Insere/atualiza produto e retorna o id interno."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO products (ml_id, title, category_id, seller_id,
                                  condition, permalink, thumbnail)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ml_id) DO UPDATE SET
                title = EXCLUDED.title,
                updated_at = NOW()
            RETURNING id
        """, (
            item["id"],
            item["title"],
            category_id,
            None,  # seller_id não disponível via scraping
            item.get("condition", "new"),
            item["permalink"],
            item["thumbnail"],
        ))
        product_id = cur.fetchone()[0]
    return product_id


def salvar_preco(conn, product_id, item):
    """Insere um registro no histórico de preços."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO price_history
                (product_id, price, original_price, available_quantity, sold_quantity)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            product_id,
            item.get("price"),
            item.get("original_price"),
            item.get("available_quantity"),
            item.get("sold_quantity"),
        ))


def coletar():
    """Executa uma rodada completa de coleta."""
    categorias = os.getenv("ML_CATEGORIES", "MLB1648").split(",")
    log.info("Iniciando coleta para categorias: %s", categorias)

    conn = conectar()
    total_salvo = 0

    try:
        for cat in categorias:
            cat = cat.strip()
            produtos = buscar_todos_produtos(cat)

            for item in produtos:
                try:
                    product_id = salvar_produto(conn, item, cat)
                    salvar_preco(conn, product_id, item)
                    total_salvo += 1
                except Exception as e:
                    log.warning("Erro ao salvar produto %s: %s", item.get("id"), e)
                    conn.rollback()
                    continue

            conn.commit()
            log.info("Categoria %s commitada no banco", cat)

    except Exception as e:
        log.error("Erro na coleta: %s", e)
        conn.rollback()
    finally:
        conn.close()

    log.info("Coleta finalizada: %d registros salvos", total_salvo)


def main():
    log.info("=== Price Monitor Scraper iniciado ===")
    coletar()

    intervalo = int(os.getenv("COLLECTION_INTERVAL_HOURS", "6"))
    schedule.every(intervalo).hours.do(coletar)
    log.info("Agendado: coleta a cada %d horas", intervalo)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
