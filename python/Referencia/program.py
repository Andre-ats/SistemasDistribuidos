
import os

from servico_referencia import ServicoReferencia


porta = int(os.environ.get("REFERENCE_PORT") or "5000")

tempo_limite = int(os.environ.get("HEARTBEAT_TIMEOUT") or "30")

referencia = ServicoReferencia(porta, tempo_limite)
referencia.iniciar()
