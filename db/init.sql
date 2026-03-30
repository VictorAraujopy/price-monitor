-- Tabela de produtos: dados "fixos" de cada produto do Mercado Livre
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    ml_id VARCHAR(20) UNIQUE NOT NULL,     -- ID do produto no Mercado Livre (ex: MLB1234567890)
    title VARCHAR(500) NOT NULL,            -- título do anúncio
    category_id VARCHAR(20) NOT NULL,       -- categoria (ex: MLB1648 = Processadores)
    seller_id BIGINT,                       -- ID do vendedor
    condition VARCHAR(10),                  -- 'new' ou 'used'
    permalink TEXT,                         -- link direto pro anúncio
    thumbnail TEXT,                         -- URL da imagem
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Tabela de histórico de preços: um registro cada vez que o scraper coleta
CREATE TABLE IF NOT EXISTS price_history (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES products(id),
    price NUMERIC(12,2) NOT NULL,           -- preço atual
    original_price NUMERIC(12,2),           -- preço "de" (antes do desconto, pode ser NULL)
    available_quantity INTEGER,             -- quantas unidades disponíveis
    sold_quantity INTEGER,                  -- quantas já venderam
    collected_at TIMESTAMP DEFAULT NOW()    -- quando o scraper coletou esse dado
);

-- Índices pra consultas que vão ser frequentes no ML pipeline
CREATE INDEX idx_price_history_product_id ON price_history(product_id);
CREATE INDEX idx_price_history_collected_at ON price_history(collected_at);
CREATE INDEX idx_products_ml_id ON products(ml_id);
CREATE INDEX idx_products_category_id ON products(category_id);
