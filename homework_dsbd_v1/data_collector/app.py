from flask import Flask, request, jsonify, abort
import time
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
import requests

#Importiamo dai moduli condivisi dalla cartella shared
from shared.handlers import register_error_handlers
from shared.config import Config
from shared.database import MongoManager
from shared.opensky import get_token
from shared.grpc_utils import check_user_grpc

app = Flask(__name__)
register_error_handlers(app)

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


"Funzione che si occupa di popolare il database con i dati di voli in partenza e/o in arrivo"
def fetch_job():
    print("Fetch dei voli in corso (Arrivi e Partenze)")
    apts = interests_col.distinct("airport_code")

    #Usiamo la funzione importata dal file shared/opensky.py per ottenere il token di autenticazione
    token = get_token()


    #Se l'autenticazione fallisce, o non sono stati inseriti interessi dagli utenti, non fare nulla
    if not apts or not token:
        print("Skip Job: Nessun aeroporto o Token fallito.")
        return

    headers = {"Authorization": f"Bearer {token}"}

    # Impostiamo l'intervallo temporale (ultimi 30 minuti)
    # end_time = int(time.time())
    # start_time = end_time - 1800

    #Questo trucco mi serve per fare il test con i dati di ieri, va commentato prima di consegnare
    #perché anche nel caso di un aeroporto particolarmente affollato, è probabile che non ci siano voli
    #nell'esatto momento in cui sto facendo la richiesta
    ieri = int(time.time()) - 86400  # 86400 secondi = 1 giorno fa
    end_time = ieri
    start_time = ieri - 7200  # Finestra di 2 ore per essere sicuri di beccare qualcosa

    #Lista degli "endpoints (arrivi e partenze)"
    endpoints = [
        ("arrival", "https://opensky-network.org/api/flights/arrival"),
        ("departure", "https://opensky-network.org/api/flights/departure")
    ]

    for apt in apts:
        #Ciclo su entrambi gli endpoint (arrivi e partenze)
        for direction, url in endpoints:
            try:
                res = requests.get(url, headers=headers, params={"airport": apt, "begin": start_time, "end": end_time})

                if res.status_code == 200:
                    flights = res.json()
                    count = 0
                    for f in flights:
                        f['fetched_at'] = datetime.now()
                        f['type'] = direction #Mi salvo la direzione del volo
                        #Inserisci il volo nel database se è nuovo
                        #altirmenti aggiornalo
                        #L'identificazione del volo viene fatto grazie a due parametri fondamentali
                        #che troviamo nel JSON: icao24 e firstSeen
                        #icao24 mi permette di identificare un aeroplano univoco (è tipo la targa delle macchine)
                        #firstSeen è letteralmente il timestamp del volo, che spesso coincide con la partenza del volo
                        #con questi due parametri posso distinguere, ad esempio, due voli con lo stesso icao24
                        #ma avvenuti in tempi diversi (ad esempio uno nel giorno corrente e l'altro nel giorno precedente)
                        flights_col.update_one(
                            {"icao24": f.get("icao24"), "firstSeen": f.get("firstSeen")},
                            {"$set": f}, upsert=True
                        )
                        count += 1
                    print(f"{apt} ({direction}): {count} voli salvati.")
                elif res.status_code == 404:
                    print(f"{apt} ({direction}): Nessun volo trovato.")
                else:
                    print(f"Err {apt} ({direction}): {res.status_code}")

            except Exception as e: print(f"Err {apt}: {e}")


"Aggiungi agli interessi di un utente un aeroporto solo nel caso in cui l'utente esista"
@app.route('/interests', methods=['POST'])
def add_interest():
    data = request.json or {}
    email, code = data.get('email'), data.get('airport_code')

    if not email or not code: abort(400, description="Dati mancanti")

    # Usiamo il canale gRPC per verificare che l'utente esista tramite la funzione importata dal file shared/grpc_utils.py
    # Se l'utente non esiste, restituisci un errore 404
    if not check_user_grpc(email):
        abort(404, description="Utente inesistente")

    interests_col.update_one({"email": email, "airport_code": code},
                             {"$set": data}, upsert=True)
    return jsonify({"message": f"Added: {code}"}), 201


"Mi ritorna la lista grezza degli ultimi 1000 voli in partenza o arrivo di un determinato aeroporto"
@app.route('/flights/<code>', methods=['GET'])
def get_flights(code):
    try:
        data = list(flights_col.find({"estArrivalAirport": code}).limit(1000))
        for d in data:
            d['_id'] = str(d['_id'])
            if 'fetched_at' in d: d['fetched_at'] = d['fetched_at'].isoformat()
        return jsonify(data), 200
    except Exception as e: abort(500, description=str(e))


"Mi ritorna l'ultimo volo in partenza o arrivo di un determinato aeroporto"
@app.route('/flights/last', methods=['GET'])
def get_last_flight():
    code = request.args.get('code')
    direction = request.args.get('type', 'arrival') # 'arrival' o 'departure'

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

        if flight:
            flight['_id'] = str(flight['_id'])
            if 'fetched_at' in flight: flight['fetched_at'] = flight['fetched_at'].isoformat()
            return jsonify(flight), 200
        else:
            return jsonify({"message": "Nessun volo trovato"}), 404
    except Exception as e:
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

        #Mi ritorna un JSON con i dati calcolati
        return jsonify({
            "airport": code,
            "type": direction,
            "days_analyzed": days,
            "total_flights": count,
            "daily_average": round(avg, 2)
        }), 200
    except Exception as e:
        abort(500, description=str(e))

if __name__ == '__main__':

    #Logica di implementazione dello scheduler "dinamico"
    scheduler = BackgroundScheduler()

    #Sto calcolando il timestamp di 12 ore dall'ora corrente
    now = datetime.now()
    switch_time = now + timedelta(hours=12)

    print(f"Avvio dello scheduler")
    #Nella fase "iniziale", il sistema esegue il job una volta al minuto per popolare rapidamente
    #il database con i dati iniziali.
    scheduler.add_job(fetch_job, 'interval', minutes=1, end_date=switch_time)
    #Superate le 12 ore iniziali, il sistema è a "regime" ed
    #esegue il job ogni 12 ore così come da specifiche
    scheduler.add_job(fetch_job, 'interval', hours=12, start_date=switch_time)

    scheduler.start()
    app.run(host='0.0.0.0', port=Config.FLASK_DATA_PORT)