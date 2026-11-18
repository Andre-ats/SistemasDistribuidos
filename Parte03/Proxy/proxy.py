import zmq

contexto = zmq.Context()
entrada = contexto.socket(zmq.XSUB)
saida = contexto.socket(zmq.XPUB)

entrada.bind("tcp://*:5557")
saida.bind("tcp://*:5558")

print("Proxy Pub/Sub iniciado: XSUB 5557 / XPUB 5558", flush=True)
try:
    zmq.proxy(entrada, saida)
finally:
    entrada.close()
    saida.close()
    contexto.term()
