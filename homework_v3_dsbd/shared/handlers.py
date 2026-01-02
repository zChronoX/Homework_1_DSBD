from flask import jsonify
# Importiamo l'eccezione custom del Circuit Breaker per poterla intercettare
from shared.circuit_breaker import CircuitBreakerOpenException
import json
import sys
from datetime import datetime

"File che gestisce in modo centralizzato gli eventuali errori delle richieste"
"La gestione centralizzata mi permette di garantire una forma di coerenza delle risposte"
"perchè se in ogni microservizio gestissi gli errori internamente, potrei avere situazioni"
"in cui le risposte vengano ricevuti in formati diversi (es. HTML o JSON)"
"In questo caso, ogni errore mi restituisce un JSON con codice e messaggio di errore"
def register_error_handlers(app):

    #Gestione dell'errore 400 che corrisponde a qualcosa di sbagliato nella richiesta
    #in particolar modo dati mancanti come nel caso in cui manca il codice aeroporto
    #o l'email/password/requestID
    @app.errorhandler(400)
    def bad_request(e):
        return jsonify({"Errore": "Richiesta Errata (400)", "details": e.description}), 400

    #Gestione dell'errore 401 che corrisponde a credenziali errate
    #(tipo quando metto una password sbagliata nella cancellazione utente)
    @app.errorhandler(401)
    def unauthorized(e):
        return jsonify({"Errore": "Non Autorizzato (401)", "details": e.description}), 401

    #Gestione dell'errore 404 che corrisponde a qualcosa di non trovato
    #come ad esempio un volo non trovato
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"Errore": "Risorsa Non Trovata (404)", "details": e.description}), 404

    #Gestione dell'errore 409 che corrisponde a qualcosa di duplicato/conflitto
    #utilizzato nel caso in cui stiamo passando una email già utilizzata
    #o, nel caso più importante, per un requestID già elaborato
    @app.errorhandler(409)
    def conflict(e):
        return jsonify({"Errore": "Conflitto (409)", "details": e.description}), 409
    #Gestione dell'errore 503 per il Circuit Breaker
    #Se il circuito è aperto, l'eccezione 'CircuitBreakerOpenException' viene catturata qui.
    #Restituiamo 503 Service Unavailable, indicando che OpenSky è temporaneamente giù.
    @app.errorhandler(CircuitBreakerOpenException)
    def service_unavailable(e):
        #Usiamo str(e) perché questa è un'eccezione Python custom, non HTTP di Flask
        return jsonify({"Errore": "Servizio Non Disponibile (503)", "details": str(e)}), 503

    #Gestione dell'errore 500 che corrisponde a errori interni del server
    #come crash, oppure a eccezioni non previste (inclusi errori Kafka generici)
    @app.errorhandler(500)
    def internal_error(e):
        return jsonify({"Errore": "Errore Interno (500)", "details": e.description}), 500

    #Catch-all per eccezioni generiche non HTTP
    #Utile se un errore imprevisto non solleva un errore HTTP esplicito
    @app.errorhandler(Exception)
    def handle_generic_exception(e):
        #Se l'errore è già stato gestito sopra Flask lo ignora qui.
        #Altrimenti, lo trattiamo come 500.
        if isinstance(e, CircuitBreakerOpenException):
            return service_unavailable(e)
        return jsonify({"Errore": "Errore Interno Generico (500)", "details": str(e)}), 500

#Funzione per i servizi di Background (Alert System, Notifier, Scheduler)
#Dato che non possono ritornare un JSON HTTP, stampano un log formattato JSON
def log_background_error(service_name, error_type, details):
    error_msg = {
        "timestamp": datetime.now().isoformat(),
        "service": service_name,
        "error_type": error_type,
        "details": str(details)
    }
    #Stampiamo su stderr così Docker lo segna come errore (rosso nei log)
    print(json.dumps(error_msg), file=sys.stderr, flush=True)