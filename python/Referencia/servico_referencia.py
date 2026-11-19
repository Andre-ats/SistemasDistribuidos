
import time

import zmq

from Common import log_mensagem
from Common.envelope import Envelope
from Common.servidor_info import ServidorInfo


def agora_ms() -> int:
    return int(time.time() * 1000)


class ServicoReferencia:
    def __init__(self, porta: int, tempo_limite: int):
        self.servidores: dict[str, ServidorInfo] = {}
        self.ultimo_heartbeat: dict[str, int] = {}
        self.disponiveis: set[str] = set()
        self.proximo_rank = 1
        self.relogio_logico = 0

        self.porta = porta
        self.tempo_limite = tempo_limite

    def iniciar(self) -> None:
        contexto = zmq.Context()
        socket = contexto.socket(zmq.REP)

        socket.bind(f"tcp://*:{self.porta}")

        print(f"Serviço de referência iniciado na porta {self.porta}!", flush=True)
        print(f"Servidores sem heartbeat por {self.tempo_limite}s serão removidos.", flush=True)

        while True:
            # Timeout curto permite verificar os heartbeats mesmo sem requisições.
            if socket.poll(1000):
                dados = socket.recv()

                mensagem = Envelope.desserializar(dados)
                log_mensagem.exibir("REFERENCIA", "RECEBENDO REQ", mensagem)

                self.relogio_logico = max(self.relogio_logico, mensagem.clock)

                print(f"Relógio lógico da referência: {self.relogio_logico}", flush=True)

                tipo = mensagem.type.lower()

                if tipo == "rank":
                    resposta = self.rank(mensagem)
                elif tipo == "list":
                    resposta = self.listar()
                elif tipo == "heartbeat":
                    resposta = self.heartbeat(mensagem)
                else:
                    resposta = Envelope(
                        type="error",
                        status="error",
                        error="Tipo de mensagem inválido"
                    )

                resposta.ts = agora_ms()
                self.relogio_logico += 1
                resposta.clock = self.relogio_logico
                resposta.from_ = "Referencia"

                log_mensagem.exibir("REFERENCIA", "ENVIANDO REP", resposta)

                socket.send(resposta.serializar())

            self.verificar_heartbeats()

    def chave(self, nome: str) -> str:
        return nome.lower()

    def rank(self, mensagem: Envelope) -> Envelope:
        nome = mensagem.server or ""

        if not nome.strip():
            return Envelope(
                type="rank_reply",
                status="error",
                error="Nome do servidor inválido"
            )

        chave = self.chave(nome)

        # Sem repetição de nomes: um servidor que reinicia recebe o mesmo rank.
        info = self.servidores.get(chave)

        if info is None:
            info = ServidorInfo(name=nome, rank=self.proximo_rank)

            self.proximo_rank += 1
            self.servidores[chave] = info

            print(f"Servidor cadastrado: {nome} (rank {info.rank})", flush=True)
        else:
            print(f"Servidor já cadastrado: {nome} (rank {info.rank})", flush=True)

        info.address = mensagem.address or info.address

        self.disponiveis.add(chave)
        self.ultimo_heartbeat[chave] = agora_ms()

        return Envelope(
            type="rank_reply",
            status="ok",
            server=nome,
            rank=info.rank
        )

    def listar(self) -> Envelope:
        return Envelope(
            type="list_reply",
            status="ok",
            servers=sorted(
                [info for chave, info in self.servidores.items() if chave in self.disponiveis],
                key=lambda info: info.rank
            )
        )

    def heartbeat(self, mensagem: Envelope) -> Envelope:
        nome = mensagem.server or ""
        chave = self.chave(nome)

        if chave not in self.servidores:
            return Envelope(
                type="heartbeat_reply",
                status="error",
                error="Servidor não registrado"
            )

        if chave not in self.disponiveis:
            self.disponiveis.add(chave)
            print(f"Servidor voltou para a lista: {nome}", flush=True)

        self.ultimo_heartbeat[chave] = agora_ms()

        print(f"Heartbeat recebido: {nome}", flush=True)

        # A hora correta agora é fornecida pelo coordenador eleito (Berkeley).
        return Envelope(
            type="heartbeat_reply",
            status="ok",
            server=nome,
            text="OK"
        )

    def verificar_heartbeats(self) -> None:
        agora = agora_ms()

        for chave in list(self.disponiveis):
            if agora - self.ultimo_heartbeat[chave] > self.tempo_limite * 1000:
                self.disponiveis.remove(chave)

                nomes = [self.servidores[item].name for item in self.disponiveis]

                print(f"Servidor removido por falta de heartbeat: {self.servidores[chave].name}", flush=True)
                print(f"Servidores disponíveis: [{', '.join(nomes)}]", flush=True)
