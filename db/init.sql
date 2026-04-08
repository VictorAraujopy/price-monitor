-- Tabela de produtos: um registro por produto único no Buscapé
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    slug VARCHAR(200) UNIQUE NOT NULL,      -- slug do Buscapé (ex: tablet-apple-ipad-11-geracao)
    title VARCHAR(500) NOT NULL,
    category VARCHAR(50) NOT NULL,           -- categoria de busca (ex: ipad, macbook)
    permalink TEXT,                          -- link da página do produto no Buscapé
    thumbnail TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Tabela de preços por loja: cada oferta de cada loja pra cada produto
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    store_name VARCHAR(200),                -- nome da loja (ex: Amazon, Kabum)
    price NUMERIC(12,2) NOT NULL,           -- preço nessa loja
    avg_price NUMERIC(12,2),                -- média de todas as lojas naquele momento
    min_price NUMERIC(12,2),                -- menor preço entre as lojas
    max_price NUMERIC(12,2),                -- maior preço entre as lojas
    num_stores INTEGER,                     -- quantas lojas vendem esse produto
    collected_at TIMESTAMP DEFAULT NOW()
);

-- Tabela de anomalias: ofertas com preço muito abaixo da média do mercado
CREATE TABLE IF NOT EXISTS anomalies (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id),
    title VARCHAR(500),
    store_name VARCHAR(200),
    price NUMERIC(12,2),
    avg_price NUMERIC(12,2),
    discount_pct FLOAT,                     -- % abaixo da média (ex: -25.3)
    notified BOOLEAN DEFAULT FALSE,
    detected_at TIMESTAMP DEFAULT NOW()
);

-- Índices
CREATE INDEX idx_price_history_product_id ON price_history(product_id);
CREATE INDEX idx_price_history_collected_at ON price_history(collected_at);
CREATE INDEX idx_products_slug ON products(slug);
CREATE INDEX idx_products_category ON products(category);
