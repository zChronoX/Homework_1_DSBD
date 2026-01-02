from flask import Flask, request, jsonify, abort
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import requests
import json
from confluent_kafka import Producer
from shared.grpc_utils import check_user_grpc, verify_credentials_grpc

#Importiamo dai moduli condivisi dalla cartella shared
from shared.handlers import register_error_handlers
from shared.config import Config
from shared.database import MongoManager
from shared.opensky import get_token
from shared.grpc_utils import check_user_grpc
#Importiamo il Circuit Breaker
from shared.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from shared.kafka_utils import wait_for_kafka


#Importo le metriche del data_collector
from shared.metrics import (
    start_metrics_server,
    DC_FETCH_TOTAL, DC_LAST_FLIGHTS,
    DC_TOTAL_INTERESTS, DC_API_REQUESTS,
    NODE_NAME
)


app = Flask(__name__)
register_error_handlers(app)

#Definiamo il nome del servizio per le label di Prometheus
SERVICE_NAME = "data_collector"

#Configurazione Produttore Kafka
#serve per inviare i dati dei voli scaricari all'alert system
producer_conf = {
    'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'data-collector-producer'
}
producer = Producer(producer_conf)

#Configurazione Circuit Breaker
#protegge il sistema se le API di OpenSky non sono disponibili (falliscono) o sono lente.
#abbiamo deciso di impostare una soglia di 3 tentativi, dopo il quale il circuito si APRE
#e smette di inviare richieste ad OpenSky.
#Dopo 60 secondi il circuito riprovare a chiamare le API di OpenSky.
opensky_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=60, expected_exception=requests.RequestException)

#Setup connessione a MongoDB
db = MongoManager.get_client()["data_db"]
interests_col = db["interests"]
flights_col = db["flights"]

"Il servizio Data_Collector si occupa di scaricare i voli direttamente da OpenSky tramite l'API REST"
"I dati scaricati in formato json vengono salvati in un database MongoDB che è perfetto per questo scopo"
"Inoltre il Data_Collector si avvale di uno scheduler dinamico che esegue inizialmente il job velocemente"
"Mentre una volta arrivati a REGIME, diventa più lento per ridurre il numero di richieste"
"Implementa inoltre una logica di gestione dei dati grezzi dei voli, restituendo"
"gli ultimi voli in partenza e/o arrivo (tramite endpoints), e la media degli ultimi X giorni così come da specifiche"




"Funzione wrapper per la chiamata HTTP necessaria per il Circuit Breaker"
"Serve per far capire al Circuit Breaker quali risposte delle API generano errori critici"
"Un errore 404 non è critico perché non ho dati in quell'aeroporto"
"mentre se esagero con le richieste (es. scheduler impostato a 30 secondi)"
"dopo 10 richieste, ho l'errore 429 Too Many Requests, che significa che le API non saranno disponbibili per un breve periodo di tempo"

def http_get_request(url, headers, params):
    #Timeout impostato a 10 secondi per evitare blocchi
    response = requests.get(url, headers=headers, params=params, timeout=10)

    #Se lo status code e' 404, non lo consideriamo un errore critico per il Circuit Breaker
    #(significa solo nessun dato per questo aeroporto/periodo)
    if response.status_code == 404:
        return response

    #Per altri errori (es. 429 Too Many Requests, 500), solleviamo l'eccezione
    response.raise_for_status()
    return response

"Funzione che si occupa di popolare il database con i dati di voli in partenza e/o in arrivo"
def fetch_job():
    print("Fetch dei voli in corso (Arrivi e Partenze)")
    apts = interests_col.distinct("airport_code")

    #Usiamo la funzione importata dal file shared/opensky.py per ottenere il token di autenticazione
    token = get_token()

    #Se l'autenticazione fallisce, o non sono stati inseriti interessi dagli utenti, non fare nulla
    if not apts or not token:
        print("Skip Job: Nessun aeroporto o recupero del token fallito.")
        return

    headers = {"Authorization": f"Bearer {token}"}


    end_time = int(time.time())
    start_time = end_time - 28800 #Finestra di 8 ore (serve per avere un numero di voli sufficiente per triggerare l'alert system e notifier

    #Lista degli "endpoints (arrivi e partenze)"
    endpoints = [
        ("arrival", "https://opensky-network.org/api/flights/arrival"),
        ("departure", "https://opensky-network.org/api/flights/departure")
    ]


        #Endpoints falsi per triggerare il circuit breaker"
    """endpoints = [
        ("arrival", "https://fake_endpoint/fake/fake/fake"),
        ("departure", "https://endpoint_falso/falso/falso/falso")
    ]"""


    for apt in apts:
        #Ciclo su entrambi gli endpoint (arrivi e partenze)
        for direction, url in endpoints:
            flights = []
            try:
                #La richiesta è protetta dal circuit breaker
                #se è aperto, viene lanciata l'eccezione
                res = opensky_breaker.call(
                    http_get_request,
                    url=url,
                    headers=headers,
                    params={"airport": apt, "begin": start_time, "end": end_time}
                )

                if res.status_code == 200:
                    flights = res.json()
                elif res.status_code == 404:
                    #Se 404, significa semplicemente nessun volo trovato per questo intervallo
                    pass


                #Incremento il contatore delle fetch
                DC_FETCH_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, status="success").inc()

                if flights:
                    count = 0
                    kafka_messages = []

                    for f in flights:
                        f['fetched_at'] = datetime.now()
                        f['type'] = direction # Mi salvo la direzione del volo

                        #Inserisci il volo nel database se è nuovo
                        #altirmenti aggiornalo
                        flights_col.update_one(
                            {"icao24": f.get("icao24"), "firstSeen": f.get("firstSeen")},
                            {"$set": f}, upsert=True
                        )
                        count += 1

                        #Preparazione dati per Kafka
                        msg_payload = f.copy()
                        #rimuoviamo l'ID interno di mongo che non ci serve
                        msg_payload.pop('_id', None)
                        msg_payload['fetched_at'] = msg_payload['fetched_at'].isoformat()
                        kafka_messages.append(msg_payload)

                    #Aggiorno il gauge con il numero di voli trovati
                    DC_LAST_FLIGHTS.labels(service=SERVICE_NAME, node=NODE_NAME).set(count)

                    print(f"{apt} ({direction}): {count} voli salvati.")


                    #Inviamo i dati all'Alert System solo se abbiamo trovato voli
                    if kafka_messages:
                        try:
                            #Costruiamo un messaggio unico contenente la lista dei voli
                            message_body = {
                                "airport": apt,
                                "type": direction,
                                "flights": kafka_messages
                            }

                            producer.produce(
                                Config.KAFKA_TOPIC_ALERT,
                                key=apt,
                                value=json.dumps(message_body).encode('utf-8')
                            )
                            producer.flush()
                            print(f"Inviati dati {apt} ({direction}) al topic {Config.KAFKA_TOPIC_ALERT} Kafka")

                        except Exception as e:
                            print(f"Errore Kafka Producer: {e}")
                else:
                    #Se 0 voli, aggiorniamo comunque il gauge a 0 per correttezza
                    DC_LAST_FLIGHTS.labels(service=SERVICE_NAME, node=NODE_NAME).set(0)

                    #Se non ci sono voli (o 404), stampiamo solo un info
                    print(f"{apt} ({direction}): Nessun volo trovato.")

            except CircuitBreakerOpenException as e:
                #Incremento il contatore degli errori del Circuit Breaker
                DC_FETCH_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, status="circuit_open").inc()
                #Se entriamo qui, il Circuit Breaker è scattato per troppi errori.
                print(f"CIRCUIT BREAKER APERTO: Chiamata bloccata per {apt}. {e}")
            except Exception as e:
                #Incremento il contatore degli errori del fetch
                DC_FETCH_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, status="error").inc()
                print(f"Err {apt}: {e}")



"Aggiungi agli interessi di un utente un aeroporto solo nel caso in cui l'utente esista"
@app.route('/interests', methods=['POST'])
def add_interest():
    data = request.json or {}
    email, code = data.get('email'), data.get('airport_code')

    #Modifica necessaria nel secondo homework per le soglie
    high_val = data.get('high_value')
    low_val = data.get('low_value')

    if not email or not code: abort(400, description="Dati mancanti")

    #Validazione Soglie: come scritto nelle specifiche, high_value deve essere maggiore di low_value
    if high_val is not None and low_val is not None:
        try:
            if float(high_val) <= float(low_val):
                abort(400, description="Errore logico: high_value deve essere maggiore di low_value")
        except ValueError:
            abort(400, description="Le soglie devono essere numeri")

    #Usiamo il canale gRPC per verificare che l'utente esista tramite la funzione importata dal file shared/grpc_utils.py
    #Se l'utente non esiste, restituisci un errore 404
    if not check_user_grpc(email):
        abort(404, description="Utente inesistente")

    #Costruzione oggetto da salvare con le nuove soglie
    update_data = {
        "email": email,
        "airport_code": code,
        "high_value": float(high_val) if high_val is not None else None,
        "low_value": float(low_val) if low_val is not None else None
    }

    #Eseguiamo l'operazione e salviamo il risultato per capire se è un update o un insert
    result = interests_col.update_one(
        {"email": email, "airport_code": code},
        {"$set": update_data},
        upsert=True
    )

    #Logica di aggiornamento delle soglie già presenti
    #Se matched_count > 0, significa che ha trovato un interesse esistente, quindi aggiorno
    #il valore delle soglie con quelli nuovi
    if result.matched_count > 0:
        message = f"Soglie aggiornate per l'aeroporto d'interesse {code} per l'utente {email}"
        status_code = 200
    else:

        #Incremento il gauge degli interessi totali a seguito di aggiunta interesse
        DC_TOTAL_INTERESTS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()

        #Altrimenti è stato creato un nuovo interesse
        message = f"Nuovo interesse aggiunto: {code}"
        status_code = 201

    return jsonify({"message": message}), status_code

"Restituisce tutti gli interessi salvati nel sistema filtrati per utente"
@app.route('/interests', methods=['GET'])
def get_interests():
    email = request.args.get('email')

    query = {}
    if email:
        query = {"email": email}

    try:
        #Recupera gli interessi dal DB (escludendo l'ID di Mongo per pulizia)
        interests = list(interests_col.find(query, {"_id": 0}))
        return jsonify(interests), 200
    except Exception as e:
        abort(500, description=str(e))


"Cancella un interesse di un utente, verificando prima le credenziali via gRPC"
@app.route('/interests', methods=['DELETE'])
def delete_interest():
    data = request.json or {}
    email = data.get('email')
    password = data.get('password')
    airport_code = data.get('airport_code')

    if not email or not password or not airport_code:
        abort(400, description="Dati mancanti (email, password, airport_code)")

    #Verifica Credenziali via gRPC
    if not verify_credentials_grpc(email, password):
        abort(401, description="Credenziali utente non valide")

    #Cancellazione dal DB
    result = interests_col.delete_one({"email": email, "airport_code": airport_code})

    if result.deleted_count > 0:

        #Decremento il gauge degli interessi totali
        DC_TOTAL_INTERESTS.labels(service=SERVICE_NAME, node=NODE_NAME).dec()

        return jsonify({"message": f"Interesse per {airport_code} rimosso correttamente"}), 200
    else:
        return jsonify({"message": "Interesse non trovato"}), 404




"Mi ritorna la lista grezza degli ultimi 1000 voli in partenza o arrivo di un determinato aeroporto"
"Aggiornata al secondo homework per far si che mi torni solo i voli in partenza/arrivo oppure"
"il JSON completo di TUTTI i voli grezzi"
@app.route('/flights/<code>', methods=['GET'])
def get_flights(code):
    #Leggiamo il parametro opzionale dalla query string (es. ?type=arrival)
    direction = request.args.get('type')

    query = {}

    if direction == 'arrival':
        #Voglio SOLO gli arrivi
        query = {"estArrivalAirport": code}
    elif direction == 'departure':
        #Voglio SOLO le partenze
        query = {"estDepartureAirport": code}
    else:
        #Voglio TUTTO (sia arrivi che partenze)
        query = {"$or": [{"estArrivalAirport": code}, {"estDepartureAirport": code}]}

    try:
        #Eseguiamo la query costruita dinamicamente
        data = list(flights_col.find(query).limit(1000))

        for d in data:
            d['_id'] = str(d['_id'])
            if 'fetched_at' in d: d['fetched_at'] = d['fetched_at'].isoformat()

        #Incremento il contatore delle richieste API quando va tutto a buon fine
        DC_API_REQUESTS.labels(service=SERVICE_NAME, node=NODE_NAME, endpoint="raw_flights", status="success").inc()

        return jsonify(data), 200
    except Exception as e:
        DC_API_REQUESTS.labels(service=SERVICE_NAME, node=NODE_NAME, endpoint="raw_flights", status="error").inc()
        abort(500, description=str(e))


"Mi ritorna l'ultimo volo in partenza o arrivo di un determinato aeroporto"
@app.route('/flights/last', methods=['GET'])
def get_last_flight():
    code = request.args.get('code')
    direction = request.args.get('type', 'arrival') #'arrival' o 'departure'

    if not code: abort(400, description="Codice aeroporto mancante")

    #Seleziono il campo corretto nel DB in base al tipo di richiesta
    db_field = "estArrivalAirport" if direction == 'arrival' else "estDepartureAirport"
    #firstSeen corrisponde agli arrivi (come specificato sopra), mentre lastSeen corrisponde alle partenze
    time_field = "lastSeen" if direction == 'arrival' else "firstSeen"

    try:
        flight = flights_col.find_one(
            {db_field: code},
            sort=[(time_field, -1)] #Mi ritorna il primo volo trovato (in ordine decrescente)
        )

        #Incremento il contatore delle richieste API quando va tutto a buon fine
        DC_API_REQUESTS.labels(service=SERVICE_NAME, node=NODE_NAME, endpoint="last_flight", status="success").inc()

        if flight:
            flight['_id'] = str(flight['_id'])
            if 'fetched_at' in flight: flight['fetched_at'] = flight['fetched_at'].isoformat()
            return jsonify(flight), 200
        else:
            return jsonify({"message": "Nessun volo trovato"}), 404
    except Exception as e:
        #Incremento il contatore degli errori API
        DC_API_REQUESTS.labels(service=SERVICE_NAME, node=NODE_NAME, endpoint="last_flight", status="error").inc()
        abort(500, description=str(e))


"Funzione che mi calcola la media dei voli in arrivo in uno specifico aeroporto negli ultimi X giorni"
@app.route('/statistics/average', methods=['GET'])
def get_average():
    code = request.args.get('code')
    days = int(request.args.get('days', 7))
    direction = request.args.get('type', 'arrival')

    if not code: abort(400, description="Codice mancante")

    #Mi calcolo il timestamp degli ultimi X giorni
    cutoff = int(time.time()) - (days * 86400)

    db_field = "estArrivalAirport" if direction == 'arrival' else "estDepartureAirport"
    time_field = "lastSeen" if direction == 'arrival' else "firstSeen"

    try:
        #Tramite un semplice contatore, conto i voli che soddisfano la finestra temporale della richiesta
        count = flights_col.count_documents({
            db_field: code,
            time_field: {"$gte": cutoff}
        })

        avg = count / days if days > 0 else 0

        #Incremento il contatore delle richieste API quando va tutto a buon fine
        DC_API_REQUESTS.labels(service=SERVICE_NAME, node=NODE_NAME, endpoint="average", status="success").inc()

        #Mi ritorna un JSON con i dati calcolati
        return jsonify({
            "airport": code,
            "type": direction,
            "days_analyzed": days,
            "total_flights": count,
            "daily_average": round(avg, 2)
        }), 200
    except Exception as e:
        DC_API_REQUESTS.labels(service=SERVICE_NAME, node=NODE_NAME, endpoint="average", status="error").inc()
        abort(500, description=str(e))


"Endpoint interno per cancellare TUTTI gli interessi di un utente, chiamato dallo User Manager"
"quando l'utente viene cancellato dal sistema"
@app.route('/interests/cleanup', methods=['DELETE'])
def cleanup_user_interests():
    email = request.args.get('email')

    if not email:
        abort(400, description="Email mancante")

    try:
        #Cancelliamo tutti i documenti che hanno questa email
        result = interests_col.delete_many({"email": email})

        if result.deleted_count > 0:
            #Decrementiamo il gauge in base a quanti interessi rimossi
            DC_TOTAL_INTERESTS.labels(service=SERVICE_NAME, node=NODE_NAME).dec(result.deleted_count)
        print(f"Rimossi {result.deleted_count} interessi per l'utente {email} a seguito di cancellazione utente")
        return jsonify({"message": "Cleanup completato", "totale interessi cancellati": result.deleted_count}), 200
    except Exception as e:
        abort(500, description=str(e))

if __name__ == '__main__':

    #Delay iniziale per dare tempo a Kafka di avviarsi completamente
    #Kafka in Kubernetes è lento a partire.
    print("Sleep per Kafka")
    wait_for_kafka(Config.KAFKA_BOOTSTRAP_SERVERS)
    print("Sleep per Kafka completato")

    # Avviamo il server metriche Prometheus su porta 8000
    start_metrics_server(8000)
    print("Metriche Prometheus avviate sulla porta 8000")

    #Logica di implementazione dello scheduler "dinamico"
    scheduler = BackgroundScheduler()

    #Sto calcolando il timestamp di 12 ore dall'ora corrente
    now = datetime.now()
    switch_time = now + timedelta(hours=12)

    print(f"Avvio dello scheduler")
    #Nella fase "iniziale", il sistema esegue il job una volta ogni cinque minuti per popolare rapidamente
    #il database con i dati iniziali.
    #se facessi eseguire il job ogni 30 secondi andrei incontro all'errore 429 (Too many requests)
    #facendo scattare il Circuit Breaker.
    scheduler.add_job(fetch_job, 'interval', minutes=5, end_date=switch_time)
    #Superate le 12 ore iniziali, il sistema è a "regime" ed
    #esegue il job ogni 12 ore così come da specifiche
    scheduler.add_job(fetch_job, 'interval', hours=12, start_date=switch_time)

    scheduler.start()
    app.run(host='0.0.0.0', port=Config.FLASK_DATA_PORT)