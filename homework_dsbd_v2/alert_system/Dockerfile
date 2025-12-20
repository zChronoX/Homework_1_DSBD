FROM python:3.9-slim
WORKDIR /app

# Copia cartella condivisa
COPY shared/ /app/shared/

# Copia requirements
COPY alert_system/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il codice
COPY alert_system/ .

ENV PYTHONPATH=/app
CMD ["python", "app.py"]