FROM python:3.12.13 AS builder

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1
WORKDIR /app


RUN python -m venv .venv
COPY requirements.txt ./
RUN .venv/bin/pip install -r requirements.txt
FROM python:3.12.13-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PORT=8080
COPY --from=builder /app/.venv .venv/
COPY . .
EXPOSE 8080
# Fly sets PORT to match http_service.internal_port (default 8080).
CMD ["/bin/sh", "-c", "exec /app/.venv/bin/uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}"]
