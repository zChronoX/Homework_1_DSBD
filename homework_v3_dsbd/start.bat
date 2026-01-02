@echo off
setlocal

:: Script di inizializzazione del progetto
:: Esegue tutte le operazioni necessarie affinché venga creato il cluster, creato e caricato
:: le immagini, e applica le configurazioni di Kubernetes tutto in modo automatico

echo INIZIALIZZAZIONE SCRIPT

:: 1. Crea il cluster (se non esiste)
:: Controlla se c'è già un cluster chiamato "kind" per evitare di perdere tempo
:: "kind" è il nome di default dato al cluster

kind get clusters | findstr "kind" >nul
if %errorlevel% equ 0 (
    echo Il cluster 'kind' esiste gia'. Salto la creazione.
) else (
    echo Creazione del cluster Kind in corso...
    kind create cluster --config k8s/kind-config.yaml
)

:: 2. INSTALLAZIONE INGRESS NGINX
echo Installazione Ingress NGINX

:: Scarica il software INGRESS NGINX che gestisce tutto il traffico HTTP
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml

:: Aspettiamo 10 secondi che Kubernetes crei fisicamente il Pod prima di interrogarlo
:: Mi serve per registrare la richiesta di creazione del Pod
:: Senza questo il comando successivo fallirebbe perché non troverebbe nulla
echo Attesa creazione Pod NGINX
timeout /t 10 /nobreak



echo Attesa avvio Ingress Controller (timeout 90s)

:: Blocca lo script finche il Pod di NGINX è pronto
kubectl wait --namespace ingress-nginx --for=condition=ready pod --selector=app.kubernetes.io/component=controller --timeout=90s


:: Aspetto altri 15 secondi per far si che il componente interno (Webhook)
:: che valida tutti i file di configurazione
:: sia anch'esso pronto
echo Attesa stabilizzazione NGINX (15 secondi)
timeout /t 15 /nobreak

:: 3. Build delle immagini Docker
:: Ricostruiamo le immagini per includere eventuali modifiche al codice (es. app.py user_manager/data_collector, ecc.)
echo Building User Manager
docker build -t user-manager:v3.0 -f user_manager/Dockerfile .

echo Building Data Collector
docker build -t data-collector:v3.5 -f data_collector/Dockerfile .

echo Building Alert System
docker build -t alert-system:v3.1 -f alert_system/Dockerfile .

echo Building Notifier
docker build -t notifier:v3.1 -f notifier/Dockerfile .

:: 4. Caricamento immagini nel cluster Kind
:: Prendiamo le immagini create in precedenza e le carichiamo nel cluster Kind.
:: Senza questo comando Kubernetes non le troverebbe e proverebbe a scaricarle
:: da internet, fallendo ovviamente
echo Loading Images
kind load docker-image user-manager:v3.0
kind load docker-image data-collector:v3.5
kind load docker-image alert-system:v3.1
kind load docker-image notifier:v3.1

:: 5. Applicazione configurazioni Kubernetes
echo Applying K8s configs
:: Applica tutti i file YAML nella cartella k8s (Deployments, Services, ConfigMap, Secrets, Ingress)
kubectl apply -f k8s/

:: 6. Riavvio forzato dei Pod
:: Serve a forzare i pod a rileggere le nuove immagini o configurazioni se erano già accesi
echo Restarting Pods
kubectl rollout restart deployment/user-manager
kubectl rollout restart deployment/data-collector
kubectl rollout restart deployment/alert-system
kubectl rollout restart deployment/notifier

echo FINITO!
pause