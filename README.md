# Homework 2 - Sistemi Distribuiti e Big Data (A.A. 2025-2026)

Questo repository contiene l'evoluzione ("v2.0") del sistema distribuito a microservizi per il monitoraggio dei voli aerei. Rispetto alla prima versione, l'architettura è stata arricchita con pattern di **Resilienza**, **Disaccoppiamento** tramite eventi, **Sicurezza** di rete e **Notifiche Reali**.

## Architettura del Sistema (v2.0)

Il sistema è containerizzato tramite Docker e orchestrato via Docker Compose. La topologia di rete è stata ridisegnata introducendo l'isolamento tra **Frontend Network** (pubblica) e **Backend Network** (privata).

### Core Services
1.  **API Gateway (Nginx)**
    * Unico punto di ingresso del sistema (Porta 80).
    * Gestisce il routing verso i microservizi (`/users`, `/flights`, `/interests`, `/statistics`) nascondendo la topologia interna.
2.  **User Manager Service**
    * Gestisce identità e sicurezza (Hashing SHA-256).
    * Implementa la politica **At-Most-Once**.
    * Espone interfacce gRPC per la manutenzione della consistenza dei dati.
    * **Database:** PostgreSQL.
3.  **Data Collector Service**
    * Interagisce con OpenSky tramite OAuth2.
    * Protegge le chiamate esterne tramite pattern **Circuit Breaker**.
    * Pubblica i dati grezzi su Kafka (Producer).
    * Gestisce le procedure di cleanup interessi via gRPC.
    * **Database:** MongoDB.

### Event-Driven & Notification Services (Novità)
4.  **Message Broker (Apache Kafka)**
    * Componente centrale per il disaccoppiamento asincrono tra raccolta dati, analisi e notifica.
5.  **Alert System**
    * Consuma i dati di volo da Kafka.
    * Recupera le regole utente da MongoDB (soglie `high_value` e `low_value`).
    * Produce eventi di allarme solo se le condizioni sono soddisfatte.
6.  **Notifier System**
    * Consuma gli eventi di allarme.
    * Invia **email reali** all'utente tramite protocollo SMTP (Gmail SSL).

## Prerequisiti

* **Docker Desktop**
* **IDE con supporto HTTP Client** (IntelliJ IDEA / PyCharm)
* **Account OpenSky** (per le credenziali API)
* **Account Gmail** con "Password per le App" attivata (necessaria per il servizio Notifier)

## Configurazione

### 1. Clonazione Repository (Branch Homework-2)
```bash
git clone -b Homework-2 [https://github.com/zChronoX/Homework_1_DSBD.git](https://github.com/zChronoX/Homework_1_DSBD.git)
