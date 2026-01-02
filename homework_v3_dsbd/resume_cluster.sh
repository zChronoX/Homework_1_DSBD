#!/bin/bash

echo "Riavvio del cluster Kind in corso"

# Riavvia il nodo di controllo
docker start kind-control-plane

# Riavvia i nodi worker
docker start kind-worker
docker start kind-worker2

echo "Attesa che i nodi siano pronti (10 secondi)"
sleep 10

echo "Verifica stato nodi:"
kubectl get nodes

echo "Cluster riavviato"