import time

"Classe che gestisce la logica di salvataggio e recupero delle richieste tramite ID"
"Che implementa la politica di At-Most-Once"
#Tramite una memoria "cache" tutte le richieste elaborate vengono salvate via ID
#Nel caso in cui venga ripetuta la richiesta con lo stesso ID, questa viene "bloccata"
#Per far si che la cache non si riempia all'infinito, viene impostato un "Time to Live" (TTL)
#in modo tale che dopo 10 minuti le richieste precedenti vengano eliminate dal sistema

class Cache:
    def __init__(self, ttl_seconds=600):  # Default: 10 minuti
        #Struttura: { "req_id": ("email", timestamp_creazione) }
        self._data = {}
        #Tempo di vita della cache
        self.ttl = ttl_seconds


    "Controlla se l'ID esiste ed è ancora valido. Se è scaduto, lo cancella"
    def is_processed(self, req_id):

        if req_id in self._data:
            email, timestamp = self._data[req_id]

            #Controllo scadenza
            if time.time() - timestamp < self.ttl:
                return True  #È ancora valido
            else:
                #Se è scaduto viene cancellato
                del self._data[req_id]
                print(f"Cache: ID {req_id} scaduto e rimosso.")

        return False
    "Restituisce l'email se l'ID è valido"
    def get_email(self, req_id):

        #La pulizia qua non serve perché viene già fatta dalla funzione sopra
        if req_id in self._data:
            return self._data[req_id][0]
        return None

    "Aggiunge alla cache il nuovo ID appena elaborato (insieme all'email)"
    "Inoltre memorizza anche il tempo di creazione dell'entry per gestire la scadenza"
    def add(self, req_id, email):

        self._data[req_id] = (email, time.time())
        print(f"Cache: salvato {req_id}")

    "Rimuove dalla cache tutti gli ID associati a una specifica email"
    def remove_by_email(self, target_email):

        #Questo metodo è fondamentale per la gestione del caso limite della cache in cui
        #se io cancellassi un'utente e provassi a riaggiungerlo subito dopo
        #avrei un conflitto di ID perché la memoria cache viene svuotata ogni 10 minuti
        #con le richieste più vecchie, quindi dovrei aspettare 10 minuti
        #Ma anche perché permette di gestire in modo più corretto la cancellazione degli utenti
        #tenendo sempre a mente il principio dell'At-Most-Once
        keys_to_remove = [
            req_id for req_id, (email, _) in self._data.items()
            if email == target_email
        ]

        for req_id in keys_to_remove:
            del self._data[req_id]

        if keys_to_remove:
            print(f"Cache: Rimossi {len(keys_to_remove)} ID associati a {target_email}")


#TTL 10 minuti
request_cache = Cache(ttl_seconds=600)