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
    #Porte dei server Flask
    FLASK_USER_PORT = 5000
    FLASK_DATA_PORT = 5001

    #Endpoint per ottenere il token di autenticazione OAuth2 di OpenSky
    OPENSKY_TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"