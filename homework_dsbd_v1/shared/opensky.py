import json
import requests
from .config import Config


"Gestione dell'autenticazione con OpenSky tramite OAuth2"
"Le due funzioni di seguito sono utilizzate per ottenere un token di autenticazione"
"La prima carica le credenziali dal file scaricato dal sito di OpenSky"
def load_creds():
    try:
        #Il file è nella root del container (/app/credentials.json)
        with open('credentials.json', 'r') as f:
            c = json.load(f)
            return c.get('clientId'), c.get('clientSecret')
    except: return None, None

#Eseguiamo il load appena questo modulo viene importato
CLIENT_ID, CLIENT_SECRET = load_creds()

"Questa mi permette di ottenere un token di autenticazione per fare tutte le richieste API"
"Necessaria perché l'autenticazione di base è stata rimossa per tutti gli account creati"
"Dopo Marzo 2025"
def get_token():
    if not CLIENT_ID: return None
    try:
        res = requests.post(Config.OPENSKY_TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET
        })
        return res.json().get("access_token") if res.status_code == 200 else None
    except: return None