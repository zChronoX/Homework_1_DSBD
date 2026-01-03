import os
import redis
import time

"Classe che gestisce la logica di salvataggio e recupero delle richieste tramite ID"
"Implementa la politica di At-Most-Once in un ambiente Distribuito (Kubernetes)"
# A differenza della versione precedente (in-memory), questa versione usa REDIS.
# Questo è fondamentale perché in Kubernetes potremmo avere più repliche dello User Manager:
# una cache locale (dizionario) non sarebbe condivisa, mentre Redis è unico per tutti.
# Il TTL (Time To Live) è gestito nativamente da Redis, che cancella i dati scaduti.

class Cache:
    def __init__(self, ttl_seconds=600):  # Default: 10 minuti
        self.ttl = ttl_seconds

        #Recuperiamo l'indirizzo di Redis dalle variabili d'ambiente di Kubernetes.
        #Se non le trova (es. test locale), usa 'localhost' come fallback.
        redis_host = os.getenv("REDIS_HOST", "localhost")
        redis_port = int(os.getenv("REDIS_PORT", 6379))

        #LOGICA DI WAIT-FOR-REDIS
        #Proviamo a connetterci per un massimo di 100 tentativi
        #Questo serve per dare il tempo al container Redis di avviarsi su Kubernetes
        max_retries = 100
        for i in range(max_retries):
            try:
                # Creiamo l'oggetto connessione
                client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
                # Il ping è fondamentale: forza una connessione reale immediata
                client.ping()

                # Se arriviamo qui, la connessione ha avuto successo
                self.r = client
                print(f"Connesso con successo a Redis su {redis_host}:{redis_port}")
                break # Usciamo dal ciclo

            except redis.ConnectionError:
                if i < max_retries - 1:
                    print(f"Redis non ancora pronto ({redis_host}:{redis_port}). Riprovo tra 2 secondi ({i+1}/{max_retries})")
                    time.sleep(2)
                else:
                    print(f"Impossibile connettersi a Redis dopo {max_retries} tentativi.")
                    # Lasciamo self.r a None, il sistema funzionerà ma senza cache (fallback)
                    self.r = None
            except Exception as e:
                print(f"Errore imprevisto durante la connessione a Redis: {e}")
                self.r = None
                break

    "Controlla se l'ID esiste in Redis (ed è quindi già stato processato)"
    def is_processed(self, req_id):
        if not self.r: return False # Fallback se Redis è giù

        # Non serve controllare il timestamp manualmente come facevamo col dizionario.
        # Se la chiave esiste in Redis, significa che non è ancora scaduta (TTL).
        # exists restituisce 1 se c'è, 0 se non c'è.
        if self.r.exists(req_id):
            return True

        return False

    "Restituisce l'email associata all'ID (se presente)"
    def get_email(self, req_id):
        if not self.r: return None
        return self.r.get(req_id)

    "Salva il nuovo ID su Redis impostando una scadenza automatica (TTL)"
    "Inoltre, aggiorna un indice secondario per poter trovare velocemente le richieste per email"
    def add(self, req_id, email):
        if not self.r: return

        # 1. Salvataggio Chiave Primaria: req_id -> email
        # 'ex' imposta la scadenza automatica (Expire) in secondi.
        self.r.set(req_id, email, ex=self.ttl)

        # 2. Aggiornamento Indice Secondario: index:email:<email> -> {id1, id2...}
        # Redis è un Key-Value store, non possiamo fare query tipo "SELECT WHERE email=...".
        # Per questo creiamo un SET (insieme) che raggruppa tutti gli ID di quell'utente.
        # Questo ci servirà per la funzione remove_by_email rendendola veloce.
        idx_key = f"index:email:{email}"
        self.r.sadd(idx_key, req_id)
        self.r.expire(idx_key, self.ttl) # Anche l'indice deve scadere

        print(f"Cache: Salvato ID {req_id} su Redis con TTL {self.ttl}s")

    "Rimuove dalla cache tutti gli ID associati a una specifica email"
    def remove_by_email(self, target_email):
        if not self.r: return

        # Questo metodo serve per gestire i conflitti in caso di cancellazione e re-iscrizione immediata.
        # Grazie all'indice secondario creato in 'add', non dobbiamo scansionare tutto il database.

        idx_key = f"index:email:{target_email}"

        # 1. Recuperiamo tutti gli ID dal nostro indice (Set)
        req_ids = self.r.smembers(idx_key)

        if req_ids:
            # 2. Cancelliamo le singole chiavi delle richieste (RequestID)
            self.r.delete(*req_ids)
            # 3. Cancelliamo l'indice stesso per fare pulizia
            self.r.delete(idx_key)
            print(f"Cache: Rimossi {len(req_ids)} ID associati a {target_email} da Redis.")
        else:
            print(f"Cache: Nessun ID trovato in memoria per {target_email}.")

# Istanza globale della cache (TTL 10 minuti)
request_cache = Cache(ttl_seconds=600)