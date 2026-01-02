import time
import sys
from confluent_kafka.admin import AdminClient



"Blocca l'esecuzione finché Kafka non è raggiungibile e pronto."
"Usa AdminClient per chiedere i metadati del cluster."


def wait_for_kafka(bootstrap_servers, max_retries=60):

    print(f"Inizio attesa Kafka su: {bootstrap_servers}", flush=True)

    conf = {'bootstrap.servers': bootstrap_servers}
    client = AdminClient(conf)

    for i in range(max_retries):
        try:
            #list_topics è il modo migliore per vedere se il broker è "sveglio"
            #Se il cluster non è pronto, questa chiamata lancia un'eccezione o va in timeout.
            cluster_metadata = client.list_topics(timeout=5.0)

            #Se siamo qui, Kafka ha risposto!
            print(f"Kafka è pronto.", flush=True)
            return

        except Exception as e:
            #flush=True è vitale per vedere i log in tempo reale su Kubernetes
            print(f"[{i+1}/{max_retries}] Kafka non risponde, riprovo tra 2s.", flush=True)
            time.sleep(2)

    print("Errore critico: Kafka non è diventato pronto nel tempo previsto.", flush=True)
    sys.exit(1) #Termina il pod per forzare un riavvio di Kubernetes