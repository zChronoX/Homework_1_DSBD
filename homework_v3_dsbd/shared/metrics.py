import os
import logging
from prometheus_client import start_http_server, Counter, Gauge
from shared.handlers import log_background_error


#File centralizzato che gestisce tutte le metriche di Prometheus associate
#ad ogni microservizio
#Abbiamo implementato le due tipologie di metriche richieste:
#Un contatore che viene incrementato ed un indicatore (gauge) che viene aggiornato
#quindi può sia essere incrementato che decrementato

#Qui recupero il nome del nodo di Kubernetes in modo da poter capire
#a livello di debugging se, nel caso in cui ci sia un picco di errori,
#è colpa del codice o di un nodo specifico del cluster che non funziona per
#motivi non di programmazione
NODE_NAME = os.getenv("K8S_NODE_NAME", "local_dev_node")



"Avvia un server HTTP leggero in un thread separato."
"Prometheus contatterà questo server per leggere i valori attuali delle metriche."
def start_metrics_server(port=8000):
    try:
        start_http_server(port)
        print(f"Prometheus server avviato correttamente su porta {port}")
    except Exception as e:
        # Usiamo il nostro gestore errori standardizzato che produce un JSON
        log_background_error("MetricsServer", "StartupError", e)

#Metriche associate al Data Collector

#Numero di fetch fatti verso OpenSky
DC_FETCH_TOTAL = Counter(
    'data_collector_fetch_total',
    'Totale richieste di fetch verso OpenSky',
    ['service', 'node', 'status']
)

#Indicatore del numero di voli scaricati nell'ultimo job
DC_LAST_FLIGHTS = Gauge(
    'data_collector_last_flights',
    'Numero di voli scaricati nell\'ultimo job',
    ['service', 'node']
)

#Indicatore del numero totale di aeroporti monitorati (cioè di interessi)
DC_TOTAL_INTERESTS = Gauge(
    'data_collector_total_interests',
    'Numero totale di aeroporti monitorati (interessi attivi)',
    ['service', 'node']
)

#Contatore delle richieste API verso le API di lettura dati
#quindi conta le letture degli ultimi voli in partenza/arrivo e media voli
DC_API_REQUESTS = Counter(
    'data_collector_api_requests_total',
    'Numero di richieste alle API di lettura dati',
    ['service', 'node', 'endpoint', 'status'] # endpoint: "average", "last_flight"
)

#Contatore del numero di registrazioni effettuate
UM_REGISTRATION_TOTAL = Counter(
    'user_manager_registration_total',
    'Totale richieste di registrazione utente',
    ['service', 'node', 'outcome']
)

#Contatore del numero di cancellazioni effettuate
UM_DELETION_TOTAL = Counter(
    'user_manager_deletion_total',
    'Totale richieste di cancellazione utente',
    ['service', 'node', 'outcome']
)


#Indicatore del numero di utenti registrati
UM_TOTAL_USERS = Gauge(
    'user_manager_total_users',
    'Numero attuale di utenti registrati',
    ['service', 'node']
)

#Metriche associate al Notifier

#Contatore delle email inviate con successo
NOTIFIER_EMAIL_TOTAL = Counter(
    'notifier_email_sent_total',
    'Totale email inviate con successo',
    ['service', 'node', 'type'] # type: "alert_email"
)

#Contatore delle email con errori di invio
NOTIFIER_ERRORS_TOTAL = Counter(
    'notifier_email_errors_total',
    'Totale errori invio email',
    ['service', 'node', 'error_type']
)