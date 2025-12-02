import grpc
from concurrent import futures
from .config import Config
#Import dei file generati dal proto, altrimenti non funziona niente
import user_pb2
import user_pb2_grpc


"Questa funzione è usata dal Data Collector per verificare se un utente esiste nel sistema"
"Nel caso in cui l'utente non esista, restituisce False, altrimenti True"
"Se torna False, il Data Collector non aggiunge l'utente agli interessi"
def check_user_grpc(email):
    try:
        #Apro un canale di comunicazione insicuro verso il container dello user manager
        chan = grpc.insecure_channel('user_manager:50051')
        stub = user_pb2_grpc.UserManagerStub(chan)
        #Effua la chiamata di procedura remota (RPC) passando l'email
        return stub.CheckUserExists(user_pb2.UserRequest(email=email)).exists
    except: return False


"Funzione che avvia il server gRPC, usata dallo User Manager"

def start_grpc(servicer, add_to_server_func):
    #Creo il server gRPC con il supporto di 10 thread per gestire le richieste in parallelo
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    #Registro lo User Service dentro il server gRPC
    add_to_server_func(servicer, server)
    #Il server si mette in ascolto sulla porta configurata nel file di configurazione
    server.add_insecure_port(Config.GRPC_PORT)
    print(f"Server gRPC attivo su {Config.GRPC_PORT}")
    #Avvia il server
    server.start()
    server.wait_for_termination()