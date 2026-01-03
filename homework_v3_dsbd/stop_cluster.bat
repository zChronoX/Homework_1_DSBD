@echo off
echo Arresto del cluster Kind (Master e Workers) in corso

:: Stoppa il nodo di controllo (Master)
docker stop kind-control-plane

:: Stoppa i nodi worker
docker stop kind-worker
docker stop kind-worker2
docker stop kind-worker3

echo Cluster fermato correttamente.
pause