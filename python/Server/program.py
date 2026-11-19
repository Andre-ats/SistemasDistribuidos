
import os

from servidor import Servidor


porta = int(os.environ.get("SERVER_PORT") or "6000")

porta_pub = int(os.environ.get("PUB_PORT") or "6001")

servidor_id = os.environ.get("SERVER_ID") or "server1"

servidor = Servidor(porta, porta_pub, servidor_id)
servidor.iniciar()
