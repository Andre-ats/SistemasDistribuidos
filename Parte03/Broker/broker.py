import zmq

contexto = zmq.Context()
clientes = contexto.socket(zmq.ROUTER)
servidores = contexto.socket(zmq.DEALER)

clientes.bind("tcp://*:5555")
servidores.bind("tcp://*:5556")

print("Broker iniciado: ROUTER 5555 (clientes) / DEALER 5556 (servidores, round-robin)", flush=True)
try:
    zmq.proxy(clientes, servidores)
finally:
    clientes.close()
    servidores.close()
    contexto.term()
