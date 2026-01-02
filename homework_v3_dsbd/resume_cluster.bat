@echo off
echo Riavvio del cluster Kind in corso

:: Riavvia il nodo di controllo
docker start kind-control-plane

:: Riavvia i nodi worker
docker start kind-worker
docker start kind-worker2

echo Attesa che i nodi siano pronti (10 secondi)
timeout /t 10 /nobreak

echo Verifica stato nodi:
kubectl get nodes

echo Cluster riavviato
pause