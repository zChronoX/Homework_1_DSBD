#!/bin/bash

# Script di inizializzazione per macOS/Linux
echo "INIZIALIZZAZIONE SCRIPT (macOS/Linux)"

# 1. Controllo esistenza cluster
# Verifica se esiste un cluster chiamato "kind"
if kind get clusters | grep -q "^kind$"; then
    echo "Il cluster 'kind' esiste già. Salto la creazione."
else
    echo "Creazione del cluster Kind in corso"
    kind create cluster --config k8s/kind-config.yaml
fi

# 2. Installazione Ingress NGINX
echo "Installazione Ingress NGINX"
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

echo "Attesa creazione Pod NGINX (10 secondi)..."
sleep 10

echo "Attesa avvio Ingress Controller (timeout 90s)"
kubectl wait --namespace ingress-nginx \
  --for=condition=ready pod \
  --selector=app.kubernetes.io/component=controller \
  --timeout=90s

echo "Attesa stabilizzazione NGINX (15 secondi)"
sleep 15

# 3. Build delle immagini Docker
echo "Building User Manager"
docker build -t user-manager:v3.0 -f user_manager/Dockerfile .

echo "Building Data Collector"
docker build -t data-collector:v3.5 -f data_collector/Dockerfile .

echo "Building Alert System"
docker build -t alert-system:v3.1 -f alert_system/Dockerfile .

echo "Building Notifier"
docker build -t notifier:v3.1 -f notifier/Dockerfile .

# 4. Caricamento immagini nel cluster Kind
echo "Loading Images into Kind"
kind load docker-image user-manager:v3.0
kind load docker-image data-collector:v3.5
kind load docker-image alert-system:v3.1
kind load docker-image notifier:v3.1

# 5. Applicazione configurazioni Kubernetes
echo "Applying K8s configs"
kubectl apply -f k8s/

# 6. Riavvio forzato dei Pod
echo "Restarting Pods"
kubectl rollout restart deployment/user-manager
kubectl rollout restart deployment/data-collector
kubectl rollout restart deployment/alert-system
kubectl rollout restart deployment/notifier

echo "FINITO"