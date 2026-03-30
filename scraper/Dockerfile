# imagem base: Python 3.12 versão slim (mais leve, sem pacotes extras desnecessários)
FROM python:3.12-slim

# cria e define a pasta de trabalho dentro do container
# tudo que vier depois acontece dentro de /app
WORKDIR /app

# copia só o requirements primeiro (pra aproveitar cache do Docker)
# se as dependências não mudarem, ele pula o pip install no próximo build
COPY requirements.txt .

# instala as dependências do requirements.txt
# --no-cache-dir: não guarda cache do pip (deixa a imagem menor)
RUN pip install --no-cache-dir -r requirements.txt

# agora copia o resto do código pra dentro do container
COPY . .

# comando que roda quando o container subir
CMD ["python", "main.py"]
