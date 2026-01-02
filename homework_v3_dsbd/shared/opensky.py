import requests

from .config import Config


"Gestione dell'autenticazione con OpenSky tramite OAuth2"
"Abbiamo rimosso la funzione load_creds() e la lettura del file json."
"Ora usiamo direttamente le variabili caricate in Config dalle Env Vars di K8s."



"Questa mi permette di ottenere un token di autenticazione per fare tutte le richieste API"
"Necessaria perché l'autenticazione di base è stata rimossa per tutti gli account creati"
"Dopo Marzo 2025"
def get_token():

    #Controlliamo se le credenziali esistono (se il Secret è stato caricato male)
    if not Config.OPENSKY_CLIENT_ID or not Config.OPENSKY_CLIENT_SECRET:
        print("Credenziali mancanti nelle variabili d'ambiente.")
        return None

    try:
        res = requests.post(Config.OPENSKY_TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": Config.OPENSKY_CLIENT_ID,
            "client_secret": Config.OPENSKY_CLIENT_SECRET
        })

        if res.status_code == 200:
            return res.json().get("access_token")
        else:
            print(f"Errore Autenticazione: {res.status_code} - {res.text}")
            return None

    except Exception as e:
        print(f"Eccezione durante richiesta token: {e}")
        return None