import json
import time
from confluent_kafka import Consumer, Producer, KafkaError
from shared.config import Config
from shared.database import MongoManager
from shared.handlers import log_background_error
from shared.kafka_utils import wait_for_kafka


#Configurazione Kafka iniziale: l'alert system ha una doppia natura
#funge da consumatore perché legge i dati grezzi del data collector
#(topic 'to-alert-system') e da produttore perché se trova "un'anomalia"
#scrive il messaggio al notifier (topic 'to-notifier')


# Configurazione Consumer (Legge dal Data Collector)
consumer_conf = {
    'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
    'group.id': Config.KAFKA_GROUP_ID_ALERT,
    'auto.offset.reset': 'earliest',
    #disattivo il salvataggio automatico della lettura, mi serve perché
    #viene fatto manualmente in seguito solo se tutto è andato
    #a buon fine per evitare di perdere dati in seguito ad un crash
    'enable.auto.commit': False
}

# Configurazione Producer (Scrive verso il Notifier)
producer_conf = {
    'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'alert-system-producer'
}



"Funzione principale che inizia il ciclo di vita dell'Alert System"

def start_alert_system():
    print(f"Configurazione Alert System")
    print(f"Server Kafka: {Config.KAFKA_BOOTSTRAP_SERVERS}")

    #Inizializziamo Consumer, Producer e DB
    consumer = Consumer(consumer_conf)
    producer = Producer(producer_conf)

    # Ci connettiamo a Mongo per leggere gli interessi degli utenti
    db = MongoManager.get_client()["data_db"]
    interests_col = db["interests"]

    #Ci iscriviamo al topic dove il Data Collector scrive i dati dei voli
    consumer.subscribe([Config.KAFKA_TOPIC_ALERT])
    print(f"Alert System avviato.", flush=True)

    try:
        #loop infinito che aspetta massimo 1 secondo per i nuovi messaggi
        #altrimenti ricomincia
        while True:
            #Chiediamo a Kafka se c'è un nuovo messaggio (timeout 1 secondo)
            msg = consumer.poll(1.0)

            if msg is None: continue
            # Gestione errore Kafka tramite handler
            if msg.error():
                #Gestione specifica per Topic non ancora creato
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    #quando avviamo il docker-compose, l'alert system potrebbe partire
                    #ancor prima che il data collector abbia creato il topic
                    #quindi andrebbe in errore, pertanto aspettiamo i secondi
                    #necessari per creare il topic e evitare crash
                    time.sleep(2)
                    continue
                log_background_error("AlertSystem", "KafkaConsumerError", msg.error())
                continue

            try:
                #Elaboriamo il messaggio
                val = msg.value()
                if not val: continue

                data = json.loads(val.decode('utf-8'))

                airport = data.get('airport')
                direction = data.get('type') # 'arrival' o 'departure' come tipi di "volo"
                flights_list = data.get('flights', [])
                current_count = len(flights_list)

                print(f"Sono stati ricevuti dall'aeroporto {airport} in ({direction}): {current_count} voli.", flush=True)


                #Cerchiamo tutti gli utenti interessati a questo aeroporto
                #tramite mongo
                interested_users = interests_col.find({"airport_code": airport})

                alerts_sent = 0
                #verifica delle soglie
                for rule in interested_users:
                    email = rule.get('email')
                    high = rule.get('high_value')
                    low = rule.get('low_value')

                    alert_reason = None

                    #Se i voli "violano" le soglie, generiamo un messaggio di allarme
                    if high is not None and current_count > high:
                        alert_reason = f"SOGLIA ALTA SUPERATA: {current_count} voli > {high}"

                    elif low is not None and current_count < low:
                        alert_reason = f"SOGLIA BASSA SUPERATA: {current_count} voli < {low}"

                    #Invio Notifica quando c'è una "violazione"
                    if alert_reason:
                        notification_payload = {
                            "email": email,
                            "airport": airport,
                            "direction": direction,
                            "current_value": current_count,
                            "message": alert_reason,
                            "timestamp": time.time()
                        }

                        #Scriviamo sul topic 'to-notifier'
                        producer.produce(
                            Config.KAFKA_TOPIC_NOTIFIER,
                            value=json.dumps(notification_payload).encode('utf-8')
                        )
                        alerts_sent += 1
                        print(f"Allarme per {email}: {alert_reason}", flush=True)

                if alerts_sent > 0:
                    producer.flush()
                    print(f"Inviati {alerts_sent} allarmi al topic {Config.KAFKA_TOPIC_NOTIFIER} da parte del Producer Kafka", flush=True)

                #Commit manuale
                consumer.commit(asynchronous=False)

            except Exception as e:
                # Gestione errore elaborazione logica alert
                log_background_error("AlertSystem", "ProcessingError", e)

    except KeyboardInterrupt:
        print("Stop manuale.", flush=True)
    except Exception as e:
        #Gestione crash critico che stopperebbe il container
        log_background_error("AlertSystem", "CriticalLoopError", e)
    finally:
        #Chiudiamo la connessione a Kafka in modo pulito
        consumer.close()

if __name__ == '__main__':
    #Delay iniziale per dare tempo a Kafka di avviarsi completamente
    #Kafka in Kubernetes è lento a partire.
    print("Sleep per Kafka")
    wait_for_kafka(Config.KAFKA_BOOTSTRAP_SERVERS)
    print("Sleep per Kafka completato")
    start_alert_system()