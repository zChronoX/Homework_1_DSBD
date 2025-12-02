from flask import jsonify

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
        return jsonify({"error": "Richiesta Errata (400)", "details": e.description}), 400

    #Gestione dell'errore 401 che corrisponde a credenziali errate
    #(tipo quando metto una password sbagliata nella cancellazione utente)
    @app.errorhandler(401)
    def unauthorized(e):
        return jsonify({"error": "Non Autorizzato (401)", "details": e.description}), 401

    #Gestione dell'errore 404 che corrisponde a qualcosa di non trovato
    #come ad esempio un volo non trovato
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "Risorsa Non Trovata (404)", "details": e.description}), 404

    #Gestione dell'errore 409 che corrisponde a qualcosa di duplicato/conflitto
    #utilizzato nel caso in cui stiamo passando una email già utilizzata
    #o, nel caso più importante, per un requestID già elaborato
    @app.errorhandler(409)
    def conflict(e):
        return jsonify({"error": "Conflitto (409)", "details": e.description}), 409
    #Gestione dell'errore 500 che corrisponde a errori interni del server
    #come crash, oppure a eccezioni non previste
    @app.errorhandler(500)
    def internal_error(e):
        return jsonify({"error": "Errore Interno (500)", "details": e.description}), 500