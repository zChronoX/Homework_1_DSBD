FROM python:3.9-slim
WORKDIR /app

# Copia cartella condivisa
COPY shared/ /app/shared/

# Copia requirements e installa
COPY user_manager/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice sorgente
COPY user_manager/ .

# Serve impostare PYTHONPATH per trovare 'shared'
ENV PYTHONPATH=/app

EXPOSE 5000 50051
CMD ["python", "app.py"]