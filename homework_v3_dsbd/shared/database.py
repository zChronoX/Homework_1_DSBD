import time
from .config import Config



"Classe per la gestione delle connessioni al database relazionale PostgreSQL"
"usata dallo User Manager"
class PostgresManager:
    @staticmethod
#Importiamo qui per evitare di avere problemi di import con il container
#del data_collector (non ha la libreria tra i requisiti, perché usa MongoDB (pymongo))
#Questa tecnica prende il nome di "Lazy Import"
    def get_connection():
        #Importiamo qui per evitare di avere problemi di import con il container
        #del data_collector (Lazy Import)
        import psycopg2


        #Impostiamo un timeout molto lungo (5 minuti totali)
        #per dare tempo al container Postgres di inizializzare il disco.
        #Modifica che abbiamo dovuto fare per il terzo homework in quanto
        #lo user manager si avvia molto più velocemente del container Postgres
        max_retries = 100
        retry_interval = 3
        current_retry = 0

        while current_retry < max_retries:
            try:
                # Tentativo di connessione
                conn = psycopg2.connect(
                    host=Config.PG_HOST, database=Config.PG_NAME,
                    user=Config.PG_USER, password=Config.PG_PASS
                )
                return conn

            except psycopg2.OperationalError as e:
                current_retry += 1
                #flush=True forza la stampa immediata nel log di Kubernetes
                print(f"[{current_retry}/{max_retries}] In attesa di Postgres. ({e})", flush=True)
                time.sleep(retry_interval)

        #Se esce dal ciclo while, abbiamo fallito per 5 minuti
        raise Exception(f"Impossibile connettersi a PostgreSQL dopo {max_retries} tentativi.")


    "Crea la tabella utenti se non esiste ancora all'avvio dello User Manager"
    @staticmethod
    def init_db():

        try:
            conn = PostgresManager.get_connection()
            cur = conn.cursor()
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS users (
                                                             email VARCHAR(255) PRIMARY KEY,
                            password VARCHAR(255) NOT NULL,
                            nome VARCHAR(255),
                            cognome VARCHAR(255),
                            codice_fiscale VARCHAR(255),
                            banking_hash VARCHAR(255) 
                            );
                        """)
            conn.commit()
            cur.close()
            conn.close()
            print("DB Postgres inizializzato.")
        except Exception as e:
            print(f"Errore Init Postgres: {e}")

"Classe per la gestione delle connessioni al database MongoDB"
class MongoManager:
    @staticmethod
    def get_client():
        import pymongo #Vale il discorso fatto sopra ma al contrario (lo user_manager non ha la libreria per il MongoDB, ma solo quella di PostgreSQL)

        return pymongo.MongoClient(Config.MONGO_URI)