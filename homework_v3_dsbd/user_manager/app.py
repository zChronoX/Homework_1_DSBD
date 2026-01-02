from flask import Flask, request, jsonify, abort
import threading
import hashlib
import sys
import os

from shared import user_pb2_grpc, user_pb2


sys.path.append(os.path.join(os.path.dirname(__file__), 'shared'))


from shared.handlers import register_error_handlers
from shared.config import Config
from shared.cache import request_cache
from shared.database import PostgresManager
from shared.grpc_utils import start_grpc
import requests


# --- NUOVI IMPORT PER LE METRICHE ---
from shared.metrics import (
    start_metrics_server,
    UM_REGISTRATION_TOTAL, UM_DELETION_TOTAL, UM_TOTAL_USERS,
    NODE_NAME
)


app = Flask(__name__)
register_error_handlers(app) #Gestione degli errori tramite il file condiviso handlers.py
PostgresManager.init_db() #Inizializzazione del DB utenti


# Definiamo il nome del servizio per Prometheus
SERVICE_NAME = "user_manager"

"Il servizio User_Manager gestisce la creazione e cancellazione degli utenti rispondendo tramite gRPC"



"Funzione interna per verificare se email e password corrispondono nel DB."
"Esegue l'hash della password e controlla la corrispondenza."
def verify_credentials_db(email, password):
    if not email or not password:
        return False

    try:
        conn = PostgresManager.get_connection()
        cur = conn.cursor()

        #Riusiamo la logica di hashing usata nella creazione
        hashed_pw = hashlib.sha256(password.encode()).hexdigest()

        #Cerchiamo un utente con quella email e quella password hashata
        cur.execute("SELECT 1 FROM users WHERE email = %s AND password = %s", (email, hashed_pw))
        result = cur.fetchone()

        cur.close(); conn.close()
        return result is not None
    except Exception as e:
        print(f"Errore DB durante verifica credenziali: {e}")
        return False



"Classe che implementa il servizio gRPC, rispondendo alle chiamate del Data Collector"
class UserServicer(user_pb2_grpc.UserManagerServicer):
    def CheckUserExists(self, request, context):
        try:
            conn = PostgresManager.get_connection()
            cur = conn.cursor()
            #Verifico semplicemente se la mail esiste nel DB
            cur.execute("SELECT email FROM users WHERE email = %s", (request.email,))
            exists = cur.fetchone() is not None
            cur.close(); conn.close()
            return user_pb2.UserResponse(exists=exists)
        except:
            return user_pb2.UserResponse(exists=False)

    #Verifica credenziali per il Data Collector (es. cancellazione interesse)
    def VerifyUserCredentials(self, request, context):
        # Utilizziamo la funzione helper condivisa
        is_valid = verify_credentials_db(request.email, request.password)
        return user_pb2.UserResponse(exists=is_valid)


"Questa funzione avvia il server gRPC ma in un thread separato"
"Il motivo è dovuto al fatto che utilizziamo Flask come server REST che insieme a gRPC sono"
"processi bloccanti che devono rimanere in ascolto di continuo"
"Se scrivessi il codice senza thread, mantenendo un comportamento sequenziale"
"il codice si bloccherebbe all'avvio del server gRPC non raggiungendo mai il punto in cui avvia Flask"
"Pertanto come vedremo sotto, è stato necessario creare un thread separato per avviare il server gRPC"
"Mentre il thread principale rimane in ascolto delle richieste REST"
def launch_grpc():
    start_grpc(UserServicer(), user_pb2_grpc.add_UserManagerServicer_to_server)



"Metodo che gestisce le richieste POST per la creazione di un nuovo utente"
"Implementa la politica di At-Most-Once con il supporto della cache"
@app.route('/users', methods=['POST'])
def create_user():
    data = request.get_json() or {}

    #Dati utente di base
    email = data.get('email')
    pwd = data.get('password')
    nome = data.get('nome', 'Sconosciuto')
    req_id = request.headers.get('RequestID') #Fondamentale per il funzionamento della cache
    cognome = data.get('cognome', '')
    cf = data.get('codice_fiscale', '')
    #Dati Bancari (le coordinate bancarie citate nelle specifiche sono state tradotte da noi come
    #le cifre della carta di credito, la scadenza e il CVV
    carta = data.get('carta_credito', '')
    scadenza = data.get('scadenza', '')
    cvv = data.get('cvv', '')
    #Queste non verranno salvate in chiaro per motivi di sicurezza (idem per la password)

    #Validazione di base
    if not email or not pwd or not req_id:
        abort(400, description="Dati obbligatori mancanti")

    # Gestione della logica "At-Most-Once"
    #Abbiamo due casi principali da gestire
    #Il primo consiste in un RETRY, vale a dire che l'ID esiste ed è associato alla stessa email
    #in questo caso abbiamo un "Hit" nella memoria cache
    #Il secondo consiste in un CREATE ma con un requestID già utilizzato
    #qui viene generato un "Conflict"
    if request_cache.is_processed(req_id):
        if request_cache.get_email(req_id) == email:
            return jsonify({"message": "La richiesta è già stata elaborata (Cache Hit)", "email": email}), 200

        # --- METRICA: Conflitto ID ---
        UM_REGISTRATION_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, outcome="conflict_req_id").inc()
        abort(409, description="Conflitto RequestID")

    #Connesione al DB
    conn = PostgresManager.get_connection()
    cur = conn.cursor()
    try:
        #Controllo dei duplicati utente (tramite chiave primaria email)
        #Questa è la "Business Logic" che mi garantisce l'unicità degli utenti
        cur.execute("SELECT email FROM users WHERE email = %s", (email,))
        # --- METRICA: Email già in uso ---
        UM_REGISTRATION_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, outcome="duplicate_email").inc()
        if cur.fetchone(): abort(409, description="Email occupata")

        #Per motivi di sicurezza abbiamo deciso di effettuare un hash della password
        #e dei dati bancari dell'utente
        pw_hash = hashlib.sha256(pwd.encode()).hexdigest()
        #Salvo l'hash dei dati bancari se presenti altrimenti salvo una stringa vuota
        banking_string = f"{carta}{scadenza}{cvv}"
        banking_hash = hashlib.sha256(banking_string.encode()).hexdigest() if banking_string else None

        #Inserimento nel DB
        query = """
                INSERT INTO users (email, password, nome, cognome, codice_fiscale, banking_hash)
                VALUES (%s, %s, %s, %s, %s, %s) \
                """
        cur.execute(query, (email, pw_hash, nome, cognome, cf, banking_hash))
        conn.commit()

        #Aggiorno la cache inserendo l'ID della richiesta e la email associata
        request_cache.add(req_id, email)

        # --- METRICA SUCCESSO: Incrementiamo Counter e Gauge ---
        UM_REGISTRATION_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, outcome="created").inc()
        UM_TOTAL_USERS.labels(service=SERVICE_NAME, node=NODE_NAME).inc()
        return jsonify({"message": "Utente creato con successo", "email": email}), 201
    except Exception as e:
        # --- METRICA ERRORE ---
        UM_REGISTRATION_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, outcome="error").inc()
        abort(500, description=str(e))
    finally: cur.close(); conn.close()


"Cancellazione di un utente; può essere fatta solo da un utente con email e password corrispondenti"
@app.route('/users', methods=['DELETE'])
def delete_user():
    data = request.get_json() or {}
    email, pwd = data.get('email'), data.get('password')
    if not email or not pwd: abort(400, description="Dati mancanti")

    #Verifichiamo prima le credenziali
    if not verify_credentials_db(email, pwd):
        abort(401, description="Credenziali errate")

    conn = PostgresManager.get_connection()
    cur = conn.cursor()
    try:
        #Procediamo con l'esecuzione della query di cancellazione
        cur.execute("DELETE FROM users WHERE email=%s", (email,))
        conn.commit()

        if cur.rowcount > 0:
            request_cache.remove_by_email(email)

            # --- METRICA SUCCESSO: Tracciamo cancellazione e decrementiamo utenti ---
            UM_DELETION_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, outcome="deleted").inc()
            UM_TOTAL_USERS.labels(service=SERVICE_NAME, node=NODE_NAME).dec()


            #Una volta cancellato l'utente, anche i suoi interessi devono essere cancellati dal Data Collector
            #Chiamiamo il microservizio Data Collector per dirgli di pulire
            try:
                # Usiamo l'URL definito in Config + l'endpoint che abbiamo appena creato
                cleanup_url = f"{Config.DATA_COLLECTOR_URL}/interests/cleanup"
                requests.delete(cleanup_url, params={"email": email}, timeout=5)
            except Exception as e:
                print(f"Attenzione: Impossibile contattare Data Collector per cleanup: {e}")

            return jsonify({"message": "Eliminato", "email": email}), 200

        abort(404, description="Utente non trovato")
    except Exception as e:
        UM_DELETION_TOTAL.labels(service=SERVICE_NAME, node=NODE_NAME, outcome="error").inc()
        abort(500, description=str(e))
    finally: cur.close(); conn.close()


if __name__ == '__main__':

    #Avvio server metriche Prometheus
    print("Avvio server metriche Prometheus su porta 8000")
    start_metrics_server(8000)


    #In questa riga avviamo il server gRPC in un thread separato (discorso fatto sopra)
    #Inoltre utilizziamo l'attributo "deamon" settato a True per far si che
    #nel caso in cui il thread principale sia interrotto, il server gRPC venga interrotto anche
    #altrimenti il server gRPC rimanerebbe in ascolto di continuo impedendo ai
    #container di arrestarsi correttamente
    threading.Thread(target=launch_grpc, daemon=True).start()

    #Flask viene avviato in "primo piano" cioè gestito dal thread principale
    app.run(host='0.0.0.0', port=Config.FLASK_USER_PORT)