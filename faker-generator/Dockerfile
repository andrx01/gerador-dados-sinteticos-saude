FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src

# Não executa nada automaticamente; rodamos pelo compose
CMD ["bash", "-lc", "python -V && ls -la /landing || true"]