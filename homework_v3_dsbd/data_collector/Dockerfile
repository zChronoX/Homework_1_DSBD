FROM python:3.9-slim
WORKDIR /app

# Copia cartella condivisa
COPY shared/ /app/shared/

# Copia requirements
COPY data_collector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia codice e file credenziali
COPY data_collector/ .

ENV PYTHONPATH=/app
EXPOSE 5001
CMD ["python", "app.py"]