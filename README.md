# Homework 3 - Sistemi Distribuiti e Big Data (A.A. 2025-2026)

![Python](https://img.shields.io/badge/Python-3.9-blue?style=for-the-badge&logo=python)
![Kubernetes](https://img.shields.io/badge/Kubernetes-Kind-326ce5?style=for-the-badge&logo=kubernetes)
![Docker](https://img.shields.io/badge/Docker-Container-2496ed?style=for-the-badge&logo=docker)
![Kafka](https://img.shields.io/badge/Apache_Kafka-KRaft-231f20?style=for-the-badge&logo=apache-kafka)
![Prometheus](https://img.shields.io/badge/Prometheus-Monitoring-e6522c?style=for-the-badge&logo=prometheus)

## Descrizione del Progetto

**OpenSky Flight Monitor** è un sistema distribuito a microservizi progettato per il monitoraggio in tempo reale del traffico aereo, l'analisi dei dati di volo e la gestione proattiva delle notifiche agli utenti.

Il sistema permette agli utenti di registrare "interessi" su specifici aeroporti (es. Fiumicino, Heathrow) definendo soglie personalizzate (numero di voli). Grazie all'integrazione con le API di **OpenSky Network**, il sistema raccoglie dati periodicamente e, sfruttando un'architettura **Event-Driven**, analizza i flussi e invia notifiche via email qualora le soglie vengano superate.

Questa versione (**v3.0**) rappresenta l'evoluzione finale del progetto: l'intera infrastruttura è stata migrata da Docker Compose a un cluster **Kubernetes** (Kind), introducendo pattern di **Self-Healing**, **High Availability** e **White-Box Monitoring**.

---

## Architettura del Sistema

Il sistema è progettato seguendo il pattern **Microservices Architecture**. La logica di business è disaccoppiata in 5 container principali che comunicano tramite protocolli ibridi (gRPC per operazioni sincrone interne, Kafka per eventi asincroni).

### I Microservizi

1.  **User Manager Service**
    * Gestisce l'autenticazione, la registrazione e la sicurezza (Hashing SHA-256).
    * Implementa la politica **At-Most-Once** (tramite Cache e RequestID) per garantire l'idempotenza delle richieste.
    * **DB:** PostgreSQL (Relazionale).

2.  **Data Collector Service**
    * Cuore dell'ingestion dati. Interroga ciclicamente le API OpenSky (OAuth2).
    * Protetto dal pattern **Circuit Breaker** per gestire i fallimenti delle API esterne.
    * Agisce come **Kafka Producer** pubblicando i voli grezzi.
    * **DB:** MongoDB (Time-Series / NoSQL).

3.  **Alert System**
    * Motore di analisi in tempo reale (Stream Processing).
    * Consuma i dati da Kafka, recupera le regole utente e verifica il superamento delle soglie (`high_value`/`low_value`).
    * In caso di violazione, produce un evento di allarme.

4.  **Notifier Service**
    * Gestisce l'invio fisico delle email tramite server **SMTP Gmail (SSL)**.
    * Disaccoppia la latenza dell'invio email dal flusso di analisi dati.

5.  **Prometheus (Monitoring)**
    * Componente infrastrutturale per l'osservabilità.
    * Esegue lo scraping delle metriche esposte dagli altri servizi (registrazioni, numero voli, errori, risorse CPU/RAM).

---

## Tecnologie e Pattern

### Core Stack
* **Linguaggio:** Python 3.9 (Flask, APScheduler, gRPC, Confluent Kafka).
* **Orchestrazione:** Kubernetes (K8s) su cluster **Kind** (Topologia: 1 Master + 2 Workers).
* **Networking:** NGINX Ingress Controller.
* **Message Broker:** Apache Kafka (Modalità **KRaft** senza Zookeeper).

### Design Patterns
* **Persistence:** Uso combinato di SQL (Postgres) per dati strutturati e NoSQL (MongoDB) per dati volumetrici.
* **Circuit Breaker:** Protezione contro i guasti a cascata verso servizi esterni.
* **Event-Driven Architecture:** Pipeline asincrona per massimizzare il throughput.


---

##  Installazione e Deployment

Il progetto include una suite di script automatizzati per gestire l'intero ciclo di vita del cluster (Build $\to$ Deploy $\to$ Teardown) senza dover digitare manualmente complessi comandi `kubectl`.

### Prerequisiti
* **Docker Desktop** (o Engine) attivo.
* **Kind** (Kubernetes in Docker) installato e aggiunto al PATH.
* **Kubectl** configurato.

### 1. Primo Avvio (Build & Deploy)
Per avviare il sistema da zero (creazione cluster, compilazione immagini, deploy):

* **Windows:** Eseguire `start.bat`
* **macOS / Linux:** Eseguire `sh start.sh`

> **Cosa fa lo script?**
> 1.  Crea un cluster Kind multi-nodo (1 Master, 2 Workers).
> 2.  Installa e configura l'Ingress NGINX.
> 3.  Esegue il build locale delle immagini Docker (`v3.0`).
> 4.  Carica le immagini nei nodi del cluster (`kind load`).
> 5.  Applica i manifest Kubernetes (`k8s/`).

### 2. Stop & Resume (Sospensione)
Per fermare il lavoro senza perdere i dati nei database:

* **Stop:** Eseguire `stop_cluster.bat` (o `.sh`). Congela i container del cluster.
* **Resume:** Eseguire `resume_cluster.bat` (o `.sh`). Riavvia i nodi istantaneamente.

---

##  Monitoraggio (Prometheus)

Il sistema integra il **White-Box Monitoring**. Una volta avviato il cluster, la dashboard è accessibile via browser grazie all'Ingress Controller:

 **Dashboard:** `http://localhost/`

### Metriche Chiave (Query PromQL)
Ecco le principali query per monitorare lo stato di salute e di business:

| Metrica | Tipo | Descrizione |
| :--- | :--- | :--- |
| `up` | Gauge | **Health Check:** 1 = Servizio Vivo, 0 = Morto. |
| `data_collector_fetch_total` | Counter | **Business:** Numero di cicli di fetch verso OpenSky eseguiti. |
| `data_collector_last_flights` | Gauge | **Business:** Volume di voli scaricati nell'ultimo ciclo. |
| `notifier_email_sent_total` | Counter | **Business:** Totale email di allerta inviate agli utenti. |
| `user_manager_registration_total` | Counter | **Business:** Registrazioni utente (filtrabili per `outcome`). |
| `process_resident_memory_bytes` | Gauge | **System:** Utilizzo RAM (utile per memory leaks). |
| `rate(process_cpu_seconds_total[1m])` | Gauge | **System:** % Utilizzo CPU nell'ultimo minuto. |

---

##  Accesso ai Database



###  Aprire i Tunnel
Eseguire in due terminali separati:

```bash
# Terminale 1: PostgreSQL (Porta locale 5432)
kubectl port-forward service/postgres-service 5432:5432

# Terminale 2: MongoDB (Porta locale 27017)
kubectl port-forward service/mongo-service 27017:27017
```
##  Credenziali di Accesso 

I database (`postgres`, `mongo`) risiedono nella rete privata del cluster. Per accedervi tramite client locali o IDE è necessario aver prima attivato il **Port Forwarding** su `localhost`.

| Database | Host | Port | User | Password | DB / Auth DB |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **PostgreSQL** | `localhost` | `5432` | `postgres` | `postgrespassword` | `user_db` |
| **MongoDB** | `localhost` | `27017` | `admin` | `adminpassword` | `admin` |

---

##  Autori

Progetto realizzato per il corso di **Sistemi Distribuiti e Big Data** (A.A. 2025-2026).

* **Giovanni Maria Contarino** - Matricola: 1000007029
* **Alessia Provvidenza Tomarchio** - Matricola: 1000005160
