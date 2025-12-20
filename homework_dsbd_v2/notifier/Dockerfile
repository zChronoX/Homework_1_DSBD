FROM python:3.9-slim
WORKDIR /app

# Copia cartella condivisa (per la config di Kafka)
COPY shared/ /app/shared/

# Copia requirements
COPY notifier/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice
COPY notifier/ .

ENV PYTHONPATH=/app
CMD ["python", "app.py"]