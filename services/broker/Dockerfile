# Build from the repo root: docker build -f services/broker/Dockerfile .
FROM python:3.12-slim
WORKDIR /app
COPY services/broker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY shared ./shared
COPY services/broker/app ./app
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
