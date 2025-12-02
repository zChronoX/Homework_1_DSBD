## **Homework 1 per il corso di Sistemi Distribuiti e Big Data (A.A. 2025-2026)**

Questo repository contiene l'implementazione di un sistema distribuito a microservizi dockerizzato per la gestione di utenti e il monitoraggio di voli aerei, sfruttando le API di **OpenSky Network**.

## Architettura del Sistema

Il sistema è containerizzato tramite Docker e orchestrato via Docker Compose. Si compone di quattro unità funzionali indipendenti:

1.  **User Manager Service**
    * Gestisce la registrazione e cancellazione degli utenti.
    * Implementa la politica At-Most-Once tramite cache in-memory per evitare duplicazioni.
    * Gestisce la sicurezza dei dati sensibili tramite hashing SHA-256.
    * **Database:** PostgreSQL (Relazionale).

2.  **Data Collector Service**
    * Interagisce con le API esterne di OpenSky tramite autenticazione OAuth2.
    * Esegue job ciclici di background per scaricare voli in arrivo e partenza.
    * Fornisce statistiche e dati storici.
    * **Database:** MongoDB (NoSQL).

3.  **Database Services**
    * **PostgreSQL:** Per dati strutturati e integrità referenziale (Utenti).
    * **MongoDB:** Per l'ingestione flessibile di dati JSON complessi (Voli).

4.  **Shared Library**
    * Modulo condiviso per configurazioni, gestione errori centralizzata e connettori DB.

## Prerequisiti

Per eseguire il progetto in locale è necessario disporre di:

* **Docker Desktop** : Essenziale per la containerizzazione.
* **IDE con supporto HTTP Client**: Si consiglia IntelliJ IDEA (Ultimate) o PyCharm per eseguire i test definiti nel file `api.test.http`.
* **Account OpenSky**: Necessario per ottenere le credenziali API.

## Configurazione Iniziale

### Step Preliminare: Clonare la repository dal seguente link

```
https://github.com/zChronoX/Homework_1_DSBD.git
```

### 1. Configurazione Credenziali (Obbligatorio)
Il sistema necessita di credenziali valide per autenticarsi presso OpenSky tramite protocollo OAuth2.

1.  Registrati sul sito di OpenSky Network e ottieni un `Client ID` e un `Client Secret`.
2.  Crea un file denominato `credentials.json`.
3.  Posiziona il file all'interno della cartella: `data_collector/`.
4.  Il contenuto deve rispettare rigorosamente questo formato JSON:

```json
{
  "clientId": "tuo-client-id",
  "clientSecret": "tuo-client-secret"
}
```

### 2. Build

E' consigliato esseguire una pulizia d'ambiente prima di effettuare il build delle immagini dei servizi tramite i seguenti comandi:

```

docker-compose down -v

```

```

docker-compose up --build

```

```

docker ps

```

### 3. Accesso ai Database

Il sistema è preconfigurato per esporre le porte dei database su `localhost`. Ecco le credenziali di default definite nel `docker-compose.yml`:

| Servizio | DB Type | Porta | Database | User | Password |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **User DB** | PostgreSQL | `5432` | `user_db` | `postgres` | `postgrespassword` |
| **Flight DB** | MongoDB | `27017` | `data_db` | `admin` | `adminpassword` |



### 4. Esecuzione dei Test

Il progetto non prevede un'interfaccia grafica in quanto non necessaria. La validazione delle funzionalità avviene tramite chiamate REST; nella directory principale del progetto è fornito il file `api.test.http`, che contiene tutta la lista delle richieste API utilizzate per testare la corretta esecuzione del sistema.

### Procedura di Test:

1.  Aprire il progetto con l'IDE consigliato (IntelliJ o PyCharm).
2.  Aprire il file `api.test.http`.
3.  Eseguire le richieste sequenzialmente cliccando sull'icona "Run" (freccia verde accanto ad ogni definizione HTTP):

* **1. Registrazione:** Conferma la creazione dell'utente e la generazione dell'hash bancario.
* **2. Politica At-Most-Once:** Conferma che il rinvio dello stesso RequestID non genera errori né duplicati.
* **3. Aggiunta Interesse:** Attiva il monitoraggio per un aeroporto. Questo innesca la comunicazione gRPC interna.
* **4. Recupero Dati:** Verifica che il Data Collector abbia scaricato i voli (Arrivi/Partenze) e li restituisca correttamente.
* **5. Statistiche:** Valida il calcolo della media giornaliera.

## Autori

Progetto realizzato per il corso di Sistemi Distribuiti e Big Data.

* **Giovanni Maria Contarino** - Matricola: 1000007029 
* **Alessia Provvidenza Tomarchio** - Matricola: 1000005160 


## Tecnologie

* **Linguaggio:** Python 3.x
* **Containerizzazione:** Docker & Docker Compose
* **API Gateway / Comunicazione:** REST & gRPC 
* **Database:** PostegreSQL & MongoDB
* **Data Source:** [OpenSky Network API](https://opensky-network.org/)

