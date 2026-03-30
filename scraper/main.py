import psycopg2
import os
import requests


def conectar():
    connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "db"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    return connection


def buscar_produtos():
    # sem query string na URL — os params já mandam tudo
    url = "https://api.mercadolibre.com/sites/MLB/search"

    params = {
        "category": "MLB1144",
        "limit": "50",
        "offset": "0",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()
    dados = response.json()

    # dados["results"] é a lista de produtos — cada item é um dicionário
    return dados["results"]


def salvar_produto(conn, item):
    cursor = conn.cursor()

    # INSERT com upsert:
    # - tenta inserir o produto com os dados da API
    # - ON CONFLICT (ml_id): se já existe um produto com esse ml_id no banco...
    # - DO UPDATE SET: ...atualiza os campos cadastrais (título pode mudar no anúncio)
    # - EXCLUDED.title: "title da linha que TENTOU entrar" (o dado novo da API)
    # - RETURNING id: devolve o id (PK automática) pra usar como FK no price_history
    #
    # cursor.execute recebe 2 argumentos separados por vírgula:
    #   1º: string SQL com %s como placeholders
    #   2º: tupla com os valores, na mesma ordem dos %s (7 colunas = 7 valores)
    cursor.execute("""
        INSERT INTO products (ml_id, title, category_id, seller_id,
        condition, permalink, thumbnail)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ml_id) DO UPDATE SET
            title = EXCLUDED.title,
            updated_at = NOW()
        RETURNING id
    """, (
        item["id"],              # ml_id — string tipo "MLB1234567890"
        item["title"],           # title — string
        item["category_id"],     # category_id — string tipo "MLB1648"
        item["seller"]["id"],    # seller_id — int (tá dentro do sub-objeto "seller")
        item["condition"],       # condition — string ("new" ou "used")
        item["permalink"],       # permalink — string (link do anúncio)
        item["thumbnail"],       # thumbnail — string (URL da imagem)
    ))

    cursor.close()

