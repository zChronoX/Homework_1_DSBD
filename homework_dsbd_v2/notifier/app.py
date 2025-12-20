import json
import time
import sys
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from confluent_kafka import Consumer, KafkaError
from shared.config import Config
# Importiamo la gestione errori centralizzata
from shared.handlers import log_background_error

#Configurazione del Consumer
consumer_conf = {
    'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
    'group.id': "notifier-group", #Gruppo dedicato alle notifiche
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
}



"Dopo la stampa a video del log d'invio della email, invia realmente l'email tramite server SMTP Gmail"
def send_email_real(data):

    email_dest = data.get('email')
    airport = data.get('airport')
    condition = data.get('message')
    current_value = data.get('current_value')

    #Simulazione d'invio della email
    print("----------------------------------------------------------------")
    print(f" [EMAIL SENT]")
    print(f" TO:      {email_dest}")
    print(f" SUBJECT: Alert for Airport {airport}")
    print(f" BODY:    {condition}")
    print(f"          (Valore rilevato: {current_value})")
    print("----------------------------------------------------------------")
    sys.stdout.flush()

    #Invio email reale
    sender_email = Config.SENDER_EMAIL
    sender_password = Config.SENDER_PASSWORD
    #Controllo di sicurezza: se mancano le credenziali nel docker-compose, non si va avanti.
    if not sender_email or not sender_password:
        print("Credenziali email mancanti in Config.")
        return

    try:
        #Costruzione Messaggio MIME
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = email_dest
        msg['Subject'] = f"Allerta per l'aeroporto {airport}"

        #Corpo della mail (uguale a quello stampato a video)
        body_text =f"""Ciao {email_dest}, è stato rilevato un superamento nelle soglie da te impostate per l'aeroporto {airport}, il valore registrato è {current_value}.
                   \n{condition}\n Buona giornata \n Homework 2 Distributed System and Big Data 2025/2026"""

        msg.attach(MIMEText(body_text, 'plain'))

        #Connessione SSL a Gmail
        #Usiamo SMTP_SSL (porta 465) che è più sicuro e standard per Gmail
        server = smtplib.SMTP_SSL(Config.SMTP_SERVER, Config.SMTP_PORT)
        server.login(sender_email, sender_password) #Login con la "Password per le app"
        server.send_message(msg)
        server.quit()#Chiudiamo la connessione
        print("Email inviata correttamente al server SMTP.")

    except Exception as e:
        print(f"Errore invio SMTP: {e}")
        #Non blocchiamo il programma, logghiamo solo l'errore dell'email

def start_notifier():
    print(f"Notifier avviato: in ascolto su: {Config.KAFKA_TOPIC_NOTIFIER}")

    consumer = Consumer(consumer_conf)
    consumer.subscribe([Config.KAFKA_TOPIC_NOTIFIER])

    try:
        while True:
            #Polling: controlliamo se ci sono messaggi ogni secondo
            msg = consumer.poll(1.0)

            if msg is None: continue
            if msg.error():
                #Gestione specifica per Topic non ancora creato
                if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    #Se il topic non esiste ancora, aspettiamo un attimo senza loggare errore
                    #E' normale all'avvio finche' non viene mandato il primo allarme
                    time.sleep(2)
                    continue
                log_background_error("NotifierSystem", "KafkaConsumerError", msg.error())
                continue

            try:
                #Decodifica messaggio
                val = msg.value()
                if not val: continue

                data = json.loads(val.decode('utf-8'))

                #Invio email
                send_email_real(data)

                #Commit manuale
                consumer.commit(asynchronous=False)

            except Exception as e:
                #Gestione errore elaborazione messaggio
                log_background_error("NotifierSystem", "ProcessingError", e)

    except KeyboardInterrupt:
        print("Stop manuale.")
    except Exception as e:
        #Gestione crash critico del loop principale
        log_background_error("NotifierSystem", "CriticalLoopError", e)
    finally:
        consumer.close()

if __name__ == '__main__':
    #Attesa strategica all'avvio per assicurarsi che il broker Kafka sia pronto
    time.sleep(10)
    start_notifier()