#!/bin/bash

echo "Arresto del cluster Kind (Master  Workers) in corso"

# Stoppa il nodo di controllo (Master)
docker stop kind-control-plane

# Stoppa i nodi worker
docker stop kind-worker
docker stop kind-worker2

echo "Cluster fermato correttamente."