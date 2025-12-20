import time
import threading

class CircuitBreakerOpenException(Exception):
    """
    Eccezione personalizzata che viene lanciata quando il circuito è APERTO.
    """
    pass

class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=30, expected_exception=Exception):
        """
        Inizializza il Circuit Breaker.
        :param failure_threshold: Numero di fallimenti consecutivi prima di aprire il circuito.
        :param recovery_timeout: Secondi di attesa prima di tentare il ripristino (Half-Open).
        :param expected_exception: Tipo di eccezione da considerare come fallimento.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time = None
        # Stato iniziale: CLOSED (Il traffico passa)
        self.state = 'CLOSED'
        self.lock = threading.Lock()

    def call(self, func, *args, **kwargs):
        """
        Esegue la funzione fornita proteggendola col Circuit Breaker.
        """
        #Usiamo il lock per assicurarci che il controllo e il cambio di stato siano atomici (sicuri)
        with self.lock:
            #Controlliamo lo stato
            if self.state == 'OPEN':
                #Se il circuito è aperto, controlliamo se è passato abbastanza tempo
                time_since_failure = time.time() - self.last_failure_time
                if time_since_failure > self.recovery_timeout:
                    #se il tempo di attesa passa, passiamo in modalità prova
                    self.state = 'HALF_OPEN'
                    print(f"Circuit Breaker: Passaggio a HALF_OPEN (dopo {time_since_failure:.1f}s)")
                else:
                    #Non è ancora passato il tempo. Blocchiamo subito la richiesta.
                    raise CircuitBreakerOpenException(f"Circuito APERTO. Riprova tra {self.recovery_timeout - time_since_failure:.1f}s")

            try:
                #Esegue la funzione
                result = func(*args, **kwargs)
            except self.expected_exception as e:
                #Se fallisce, incrementa contatore
                self.failure_count += 1
                self.last_failure_time = time.time()
                print(f"Errore rilevato ({self.failure_count}/{self.failure_threshold}): {e}")
                #Se abbiamo raggiunto la soglia di errori consecutivi APRIAMO IL CIRCUITO
                if self.failure_count >= self.failure_threshold:
                    self.state = 'OPEN'
                    print("Circuit Breaker: SOGLIA RAGGIUNTA -> CIRCUITO APERTO!")
                raise e
            else:
                 #Se la funzione viene eseguita senza errori (entriamo qui se non scatta except)
                if self.state == 'HALF_OPEN':
                    #Eravamo in prova e ha funzionato quindi il sistema torna a funzionare.
                    self.state = 'CLOSED'
                    self.failure_count = 0
                    print("Circuit Breaker: Successo in Half-Open -> CIRCUITO CHIUSO (Ripristinato)")
                elif self.state == 'CLOSED':
                    #Eravamo già chiusi, ma resetto il contatore per sicurezza.
                    #(Se avevamo fatto 1 errore su 3, e ora funziona, resettiamo a 0/3)
                    self.failure_count = 0

                return result