import os

"In questo file gestisco la configurazione centralizzata del sistema, in cui"
"sono memorizzate password, porte e altre variabili essenziali per configurare"
"correttamente il sistema"
"Inserendole in un unico file centralizzato, posso cambiare la configurazione del sistema"
"senza dover ricercare e modificare tutte le posizioni in cui vengono utilizzate"
"e quindi senza toccare il codice sorgente principale"


class Config:
    #PostgreSQL (User Manager)
    #Se nel docker-compose dovessi cambiare il nome del container, posso tranquillamente
    #modificarlo qui
    PG_HOST = os.getenv("PG_HOST", "postgres_container")
    PG_NAME = os.getenv("PG_NAME", "user_db")
    PG_USER = os.getenv("PG_USER", "postgres")
    PG_PASS = os.getenv("PG_PASS", "postgrespassword")

    #Mongo (Data Collector)
    #Formato: mongodb://username:password@host:port/
    #Mi serve per autenticarmi al DB di Mongo
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpassword@mongo_container:27017/")

    #Porta interna per gRPC (per la comunicazione tra i container)
    GRPC_PORT = "[::]:50051"
    #Indirizzo completo per il CLIENT gRPC (Data Collector -> User Manager)
    #"user_manager" è il nome del servizio nel docker-compose
    GRPC_SERVER_ADDRESS = os.getenv("GRPC_SERVER_ADDRESS", "user_manager:50051")
    #Porte dei server Flask
    FLASK_USER_PORT = 5000
    FLASK_DATA_PORT = 5001


    #Indirizzo del Data Collector per chiamate HTTP interne (da User Manager)
    DATA_COLLECTOR_URL = "http://data_collector:5001"

    #Endpoint per ottenere il token di autenticazione OAuth2 di OpenSky
    OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"




    #KAFKA (Interno a Docker)
    #Nota: Usiamo 'kafka_broker:9092' perché siamo dentro la rete Docker
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka_broker:9092")
    #Topic dove il DataCollector scrive e l'AlertSystem legge
    KAFKA_TOPIC_ALERT = "to-alert-system"

    #Topic dove l'AlertSystem scrive e il Notifier legge (lo useremo dopo)
    KAFKA_TOPIC_NOTIFIER = "to-notifier"

    #Cambia il nome aggiungendo un suffisso (es. v2) per resettare la lettura
    KAFKA_GROUP_ID_ALERT = "alert-system-group-v2"

    #CONFIGURAZIONE EMAIL (Gmail)
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 465  # Porta sicura SSL
    SENDER_EMAIL = os.getenv("SENDER_EMAIL")     # Lo prenderà dal docker-compose
    SENDER_PASSWORD = os.getenv("SENDER_PASSWORD") # Lo prenderà dal docker-compose