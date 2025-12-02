import time
from .config import Config



"Classe per la gestione delle connessioni al database relazionale PostgreSQL"
"usata dallo User Manager"
class PostgresManager:
    @staticmethod
    def get_connection():
        #Importiamo qui per evitare di avere problemi di import con il container
        #del data_collector (non ha la libreria tra i requisiti, perché usa MongoDB (pymongo))
        #Questa tecnica prende il nome di "Lazy Import"
        import psycopg2
        retries = 5
        while retries > 0:
            try:
                return psycopg2.connect(
                    host=Config.PG_HOST, database=Config.PG_NAME,
                    user=Config.PG_USER, password=Config.PG_PASS
                )
            except psycopg2.OperationalError:
                print("In attesa di Postgres")
                time.sleep(3)
                retries -= 1
        raise Exception("Impossibile connettersi a PostgreSQL")


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
            print("DB Postgres inizializzato con nuovi campi.")
        except Exception as e:
            print(f"Errore Init Postgres: {e}")

"Classe per la gestione delle connessioni al database MongoDB"
class MongoManager:
    @staticmethod
    def get_client():
        import pymongo #Vale il discorso fatto sopra ma al contrario (lo user_manager non ha la libreria per il MongoDB, ma solo quella di PostgreSQL)

        return pymongo.MongoClient(Config.MONGO_URI)