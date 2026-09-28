# Build from the repo root: docker build -f services/feed/Dockerfile .
FROM python:3.12-slim
WORKDIR /app
COPY services/feed/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY services/feed/app ./app
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
