
import os
import re
import threading
import time
import uuid

import zmq

from Common import log_mensagem
from Common.envelope import Envelope
from Common.login_registro import LoginRegistro
from Common.servidor_info import ServidorInfo
from persistencia import Persistencia


def agora_ms() -> int:
    return int(time.time() * 1000)


def iniciar_thread(funcao) -> None:
    thread = threading.Thread(target=funcao, daemon=True)
    thread.start()


class Servidor:
    def __init__(self, porta: int, porta_pub: int, servidor_id: str):
        # Usuários e participantes ignoram maiúsculas/minúsculas (guardados em minúsculo).
        self.usuarios: set[str] = set()
        self.canais: set[str] = set()
        self.participantes: dict[str, set[str]] = {}
        self.mensagens: dict[str, list[Envelope]] = {}
        self.relogio_logico = 0

        # Relógio físico: hora local = hora do sistema + desvio (em ms).
        self.desvio_relogio = 0
        self.rank = 0
        self.mensagens_clientes = 0

        # Eleição (algoritmo do valentão): o servidor disponível de maior rank vira coordenador.
        self.coordenador: str | None = None
        self.endereco_coordenador: str | None = None
        self.eleicao_em_andamento = False
        self.anuncios_recebidos = 0

        # Berkeley: diferença (ms) de cada servidor em relação ao coordenador.
        self.diferencas_relogio: dict[str, int] = {}

        self.trava_relogio = threading.RLock()
        self.trava_publisher = threading.RLock()
        self.trava_estado = threading.RLock()
        self.trava_eleicao = threading.Lock()

        self.historico_logins: list[LoginRegistro] = []

        self.porta = porta
        self.porta_pub = porta_pub
        self.servidor_id = servidor_id
        self.porta_sincronizacao = int(os.environ.get("SYNC_PORT") or str(porta + 2))
        self.proxy_host = os.environ.get("PROXY_HOST") or "localhost"
        self.referencia_host = os.environ.get("REFERENCE_HOST") or "localhost"
        self.referencia_porta = int(os.environ.get("REFERENCE_PORT") or "5000")

        # Desvio inicial opcional para simular um relógio físico atrasado/adiantado.
        self.desvio_relogio = int(os.environ.get("CLOCK_DRIFT_MS") or "0")

        self.persistencia = Persistencia(servidor_id)

        self.contexto = zmq.Context()
        self.publisher = None

    # ===== Relógios =====

    def hora_atual(self) -> int:
        with self.trava_relogio:
            return agora_ms() + self.desvio_relogio

    def incrementar_relogio(self) -> int:
        with self.trava_relogio:
            self.relogio_logico += 1
            return self.relogio_logico

    def atualizar_relogio(self, recebido: int) -> int:
        with self.trava_relogio:
            self.relogio_logico = max(self.relogio_logico, recebido)
            return self.relogio_logico

    @staticmethod
    def formatar_hora(milissegundos: int) -> str:
        # Mesmo formato do C# (":O"): 2026-11-18T06:38:57.5330000+00:00
        return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(milissegundos // 1000)) + f".{milissegundos % 1000:03d}0000+00:00"

    def iniciar(self) -> None:
        self.historico_logins.extend(self.persistencia.carregar_logins())

        for canal in self.persistencia.carregar_canais():
            self.canais.add(canal)
            self.participantes[canal] = set()
            self.mensagens[canal] = self.persistencia.carregar_mensagens(canal)

        # Regrava os arquivos na ordem padrão (dados antigos podem estar em outra ordem).
        for canal in self.canais:
            self.ordenar_mensagens(canal)
            self.persistencia.salvar_mensagens(canal, self.mensagens[canal])

        self.persistencia.salvar_canais(self.canais)
        self.persistencia.salvar_logins(self.historico_logins)

        print(f"Logins registrados: {len(self.historico_logins)}", flush=True)
        print(f"Canais carregados: {len(self.canais)}", flush=True)

        socket = self.contexto.socket(zmq.REP)

        socket.bind(f"tcp://*:{self.porta}")

        self.publisher = self.contexto.socket(zmq.PUB)

        self.publisher.connect(f"tcp://{self.proxy_host}:5557")

        print(f"Servidor iniciado na porta {self.porta}!", flush=True)
        print(f"Publicações enviadas ao proxy {self.proxy_host}:5557!", flush=True)
        print(f"Relógio físico inicial: {self.formatar_hora(self.hora_atual())} (desvio {self.desvio_relogio} ms)", flush=True)

        self.pedir_rank()
        self.listar_servidores()

        iniciar_thread(self.atender_servidores)
        iniciar_thread(self.ouvir_servidores)

        # O SUB de "replication" já está ativo; então busca o que foi perdido.
        time.sleep(1)
        self.sincronizar_dados(self.listar_servidores())

        # Também envia aos outros o que só este servidor tem (ex.: dados de antes de cair).
        # Espera um pouco para os outros servidores já estarem assinando "replication".
        def divulgar_depois():
            time.sleep(10)
            self.divulgar_estado()

        iniciar_thread(divulgar_depois)

        # Aguarda o SUB conectar ao proxy antes da primeira eleição.
        def eleger_depois():
            time.sleep(3)
            self.eleicao()

        iniciar_thread(eleger_depois)

        while True:
            dados = socket.recv()

            mensagem = Envelope.desserializar(dados)
            log_mensagem.exibir("SERVIDOR", "RECEBENDO REQ", mensagem)

            print(f"Relógio do servidor: {self.atualizar_relogio(mensagem.clock)}", flush=True)

            print(f"Mensagem recebida: {mensagem.type}", flush=True)

            with self.trava_estado:
                tipo = mensagem.type.lower()

                if tipo == "login":
                    resposta = self.login(mensagem)
                elif tipo == "create_channel":
                    resposta = self.criar_canal(mensagem)
                elif tipo == "list_channels":
                    resposta = self.listar_canais()
                elif tipo == "join_channel":
                    resposta = self.entrar_canal(mensagem)
                elif tipo == "leave_channel":
                    resposta = self.sair_canal(mensagem)
                elif tipo == "post":
                    resposta = self.publicar_mensagem(mensagem)
                elif tipo == "get_messages":
                    resposta = self.consultar_mensagens(mensagem)
                elif tipo == "logout":
                    resposta = self.logout(mensagem)
                else:
                    resposta = Envelope(
                        type="error",
                        status="error",
                        error="Tipo de mensagem inválido"
                    )

            resposta.ts = self.hora_atual()
            resposta.clock = self.incrementar_relogio()
            resposta.from_ = "Server"

            log_mensagem.exibir("SERVIDOR", "ENVIANDO REP", resposta)

            socket.send(resposta.serializar())

            self.mensagens_clientes += 1

            # Heartbeat a cada 10 mensagens de clientes recebidas.
            if self.mensagens_clientes % 10 == 0:
                self.enviar_heartbeat()

            # Sincronização do relógio físico com o coordenador a cada 15 mensagens.
            if self.mensagens_clientes % 15 == 0:
                self.sincronizar_relogio()

    # ===== Serviço de referência =====

    def enviar_referencia(self, requisicao: Envelope) -> Envelope | None:
        for tentativa in range(1, 4):
            requisicao_socket = self.contexto.socket(zmq.REQ)
            requisicao_socket.setsockopt(zmq.LINGER, 0)

            try:
                requisicao_socket.connect(f"tcp://{self.referencia_host}:{self.referencia_porta}")

                requisicao.ts = self.hora_atual()
                requisicao.clock = self.incrementar_relogio()
                requisicao.from_ = self.servidor_id

                log_mensagem.exibir("SERVIDOR", "ENVIANDO REQ", requisicao)

                requisicao_socket.send(requisicao.serializar())

                if not requisicao_socket.poll(3000):
                    print(f"[{self.servidor_id}] Referência sem resposta. Tentativa {tentativa}/3", flush=True)

                    time.sleep(1)
                    continue

                resposta = Envelope.desserializar(requisicao_socket.recv())

                log_mensagem.exibir("SERVIDOR", "RECEBENDO REP", resposta)

                print(f"Relógio do servidor: {self.atualizar_relogio(resposta.clock)}", flush=True)

                return resposta
            except Exception as ex:
                print(f"[{self.servidor_id}] Erro com a referência: {ex}", flush=True)
                time.sleep(1)
            finally:
                requisicao_socket.close()

        return None

    def pedir_rank(self) -> None:
        while True:
            resposta = self.enviar_referencia(Envelope(
                type="rank",
                server=self.servidor_id,
                address=f"{self.servidor_id}:{self.porta_sincronizacao}"
            ))

            if resposta is not None and resposta.status == "ok" and resposta.rank is not None:
                self.rank = resposta.rank
                print(f"[{self.servidor_id}] Rank recebido: {self.rank}", flush=True)
                return

            print(f"[{self.servidor_id}] Não foi possível obter o rank. Tentando novamente...", flush=True)
            time.sleep(2)

    def listar_servidores(self) -> list[ServidorInfo]:
        resposta = self.enviar_referencia(Envelope(
            type="list"
        ))

        lista = resposta.servers if resposta is not None and resposta.servers is not None else []

        print(f"[{self.servidor_id}] Servidores disponíveis:", flush=True)

        for info in lista:
            print(f"- {info.name} (rank {info.rank}) em {info.address}", flush=True)

        return lista

    def enviar_heartbeat(self) -> None:
        resposta = self.enviar_referencia(Envelope(
            type="heartbeat",
            server=self.servidor_id
        ))

        if resposta is not None and resposta.status == "error" and resposta.error == "Servidor não registrado":
            self.pedir_rank()
            return

        if resposta is None or resposta.status != "ok":
            print(f"[{self.servidor_id}] Heartbeat sem resposta.", flush=True)
            return

        print(f"[{self.servidor_id}] Heartbeat confirmado: {resposta.text}", flush=True)

    def ajustar_relogio_fisico(self, hora_correta: int, enviado_em: int, recebido_em: int) -> None:
        # Compensa metade do tempo de ida e volta (algoritmo de Cristian).
        hora_estimada = hora_correta + int((recebido_em - enviado_em) / 2)
        antes = self.hora_atual()

        with self.trava_relogio:
            self.desvio_relogio = hora_estimada - recebido_em

        depois = self.hora_atual()

        print(f"[{self.servidor_id}] Relógio físico ajustado:", flush=True)
        print(f"  Antes: {self.formatar_hora(antes)}", flush=True)
        print(f"  Depois: {self.formatar_hora(depois)}", flush=True)
        print(f"  Correção: {depois - antes} ms", flush=True)

    # ===== Comunicação entre servidores =====

    # Atende as requisições de outros servidores (relógio, eleição e sync) em uma porta própria,
    # para não misturar com as requisições dos clientes.
    def atender_servidores(self) -> None:
        socket_servidores = self.contexto.socket(zmq.REP)

        socket_servidores.bind(f"tcp://*:{self.porta_sincronizacao}")

        print(f"[{self.servidor_id}] Atendendo servidores na porta {self.porta_sincronizacao}!", flush=True)

        while True:
            dados = socket_servidores.recv()

            mensagem = Envelope.desserializar(dados)
            log_mensagem.exibir("SERVIDOR", "RECEBENDO REQ", mensagem)

            print(f"Relógio do servidor: {self.atualizar_relogio(mensagem.clock)}", flush=True)

            tipo = mensagem.type.lower()

            if tipo == "clock":
                resposta = self.relogio_coordenador(mensagem)
            elif tipo == "sync":
                resposta = self.estado_completo()
            elif tipo == "election":
                resposta = Envelope(
                    type="election_reply",
                    status="ok",
                    server=self.servidor_id,
                    rank=self.rank,
                    text="OK"
                )

                # Responde OK e assume a eleição a partir deste servidor.
                iniciar_thread(self.eleicao)
            else:
                resposta = Envelope(
                    type="error",
                    status="error",
                    error="Tipo de mensagem inválido"
                )

            resposta.ts = self.hora_atual()
            resposta.clock = self.incrementar_relogio()
            resposta.from_ = self.servidor_id

            log_mensagem.exibir("SERVIDOR", "ENVIANDO REP", resposta)

            socket_servidores.send(resposta.serializar())

    # Recebe pelo tópico "servers" o nome do servidor eleito como coordenador
    # e pelo tópico "replication" as escritas feitas nos outros servidores.
    def ouvir_servidores(self) -> None:
        subscriber = self.contexto.socket(zmq.SUB)

        subscriber.connect(f"tcp://{self.proxy_host}:5558")
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "servers")
        subscriber.setsockopt_string(zmq.SUBSCRIBE, "replication")

        while True:
            topico, dados = subscriber.recv_multipart()
            topico = topico.decode()

            if topico == "replication":
                operacao = Envelope.desserializar(dados)

                if operacao.server == self.servidor_id:
                    continue

                log_mensagem.exibir("SERVIDOR", "RECEBENDO PUB", operacao)

                print(f"Relógio do servidor: {self.atualizar_relogio(operacao.clock)}", flush=True)

                self.aplicar_replicacao(operacao)
                continue

            if topico != "servers":
                continue

            anuncio = Envelope.desserializar(dados)
            log_mensagem.exibir("SERVIDOR", "RECEBENDO PUB", anuncio)

            print(f"Relógio do servidor: {self.atualizar_relogio(anuncio.clock)}", flush=True)

            if anuncio.rank is not None and anuncio.rank < self.rank:
                # Valentão: um servidor de rank menor se anunciou, então este servidor assume.
                print(f"[{self.servidor_id}] Coordenador {anuncio.server} tem rank menor; iniciando eleição.", flush=True)
                iniciar_thread(self.eleicao)
                continue

            self.coordenador = anuncio.server
            self.endereco_coordenador = anuncio.address
            self.anuncios_recebidos += 1

            with self.trava_relogio:
                self.diferencas_relogio.clear()

            print(f"[{self.servidor_id}] Coordenador atual: {self.coordenador} ({self.endereco_coordenador})", flush=True)

    def enviar_servidor(self, endereco: str, requisicao: Envelope) -> Envelope | None:
        requisicao_socket = self.contexto.socket(zmq.REQ)
        requisicao_socket.setsockopt(zmq.LINGER, 0)

        try:
            requisicao_socket.connect(f"tcp://{endereco}")

            requisicao.ts = self.hora_atual()
            requisicao.clock = self.incrementar_relogio()
            requisicao.from_ = self.servidor_id
            requisicao.server = requisicao.server or self.servidor_id

            log_mensagem.exibir("SERVIDOR", "ENVIANDO REQ", requisicao)

            requisicao_socket.send(requisicao.serializar())

            if not requisicao_socket.poll(2000):
                print(f"[{self.servidor_id}] Servidor {endereco} sem resposta.", flush=True)
                return None

            resposta = Envelope.desserializar(requisicao_socket.recv())

            log_mensagem.exibir("SERVIDOR", "RECEBENDO REP", resposta)

            print(f"Relógio do servidor: {self.atualizar_relogio(resposta.clock)}", flush=True)

            return resposta
        except Exception as ex:
            print(f"[{self.servidor_id}] Erro com o servidor {endereco}: {ex}", flush=True)
            return None
        finally:
            requisicao_socket.close()

    # ===== Eleição =====

    def eleicao(self) -> None:
        with self.trava_eleicao:
            if self.eleicao_em_andamento:
                print(f"[{self.servidor_id}] Eleição já em andamento.", flush=True)
                return

            self.eleicao_em_andamento = True

        try:
            while True:
                print(f"[{self.servidor_id}] Iniciando eleição (rank {self.rank})", flush=True)

                lista = self.listar_servidores()
                recebeu_ok = False

                for info in lista:
                    if info.rank <= self.rank or info.name == self.servidor_id:
                        continue

                    resposta = self.enviar_servidor(info.address, Envelope(
                        type="election",
                        rank=self.rank
                    ))

                    if resposta is not None and resposta.status == "ok":
                        recebeu_ok = True

                if not recebeu_ok:
                    self.tornar_coordenador()
                    return

                # Um servidor de rank maior respondeu: aguarda o anúncio dele no tópico "servers".
                print(f"[{self.servidor_id}] Servidor de rank maior respondeu; aguardando anúncio do coordenador...", flush=True)

                antes = self.anuncios_recebidos

                for _ in range(20):
                    if self.anuncios_recebidos != antes:
                        break

                    time.sleep(0.5)

                if self.anuncios_recebidos != antes:
                    return

                print(f"[{self.servidor_id}] Nenhum coordenador anunciado; repetindo eleição.", flush=True)
        finally:
            with self.trava_eleicao:
                self.eleicao_em_andamento = False

    def tornar_coordenador(self) -> None:
        self.coordenador = self.servidor_id
        self.endereco_coordenador = f"{self.servidor_id}:{self.porta_sincronizacao}"

        anuncio = Envelope(
            type="coordinator",
            server=self.servidor_id,
            rank=self.rank,
            address=self.endereco_coordenador,
            from_=self.servidor_id,
            ts=self.hora_atual(),
            clock=self.incrementar_relogio()
        )

        log_mensagem.exibir("SERVIDOR", "ENVIANDO PUB", anuncio)

        with self.trava_publisher:
            self.publisher.send_multipart([b"servers", anuncio.serializar()])

        print(f"[{self.servidor_id}] Eleito como coordenador!", flush=True)

    def sincronizar_relogio(self) -> None:
        atual = self.coordenador
        endereco = self.endereco_coordenador

        if atual is None or endereco is None:
            print(f"[{self.servidor_id}] Nenhum coordenador conhecido; iniciando eleição.", flush=True)
            iniciar_thread(self.eleicao)
            return

        if atual == self.servidor_id:
            print(f"[{self.servidor_id}] Este servidor é o coordenador; relógio usado como referência.", flush=True)
            return

        enviado_em = agora_ms()

        resposta = self.enviar_servidor(endereco, Envelope(
            type="clock"
        ))

        recebido_em = agora_ms()

        if resposta is None or resposta.status != "ok" or resposta.time is None:
            print(f"[{self.servidor_id}] Coordenador {atual} indisponível; iniciando eleição.", flush=True)
            self.coordenador = None
            self.endereco_coordenador = None
            iniciar_thread(self.eleicao)
            return

        print(f"[{self.servidor_id}] {resposta.text}", flush=True)

        self.ajustar_relogio_fisico(resposta.time, enviado_em, recebido_em)

    # Berkeley no coordenador: guarda a diferença de cada servidor, calcula a média
    # (incluindo o próprio coordenador com diferença 0), ajusta o próprio relógio
    # pela média e devolve a hora correta para o servidor que pediu.
    def relogio_coordenador(self, mensagem: Envelope) -> Envelope:
        if self.coordenador != self.servidor_id:
            return Envelope(
                type="clock_reply",
                status="error",
                error="Servidor não é o coordenador"
            )

        nome = mensagem.server or mensagem.from_

        with self.trava_relogio:
            agora = agora_ms() + self.desvio_relogio

            self.diferencas_relogio[nome] = mensagem.ts - agora

            # Divisão inteira truncando em direção ao zero, como no C#.
            media = int(sum(self.diferencas_relogio.values()) / (len(self.diferencas_relogio) + 1))

            self.desvio_relogio += media

            for chave in list(self.diferencas_relogio.keys()):
                self.diferencas_relogio[chave] -= media

            # Quem pediu passa a usar a hora correta, então sua diferença vira zero.
            self.diferencas_relogio[nome] = 0

            hora_correta = agora_ms() + self.desvio_relogio
            quantidade = len(self.diferencas_relogio) + 1

        print(f"[{self.servidor_id}] Berkeley: {quantidade} relógios, média {media} ms", flush=True)

        return Envelope(
            type="clock_reply",
            status="ok",
            server=self.servidor_id,
            time=hora_correta,
            text=f"Hora correta do coordenador {self.servidor_id} (média Berkeley {media} ms)"
        )

    # ===== Replicação =====
    # Replicação ativa: toda escrita aceita por um servidor (login, canal e mensagem)
    # é publicada no tópico "replication" e aplicada por todos os outros servidores.

    def replicar(self, operacao: Envelope) -> None:
        operacao.server = self.servidor_id
        operacao.from_ = self.servidor_id
        operacao.ts = self.hora_atual()
        operacao.clock = self.incrementar_relogio()

        log_mensagem.exibir("SERVIDOR", "ENVIANDO PUB", operacao)

        with self.trava_publisher:
            self.publisher.send_multipart([b"replication", operacao.serializar()])

    def aplicar_replicacao(self, operacao: Envelope) -> None:
        if operacao.server == self.servidor_id:
            return

        with self.trava_estado:
            if operacao.type == "replicate_login":
                self.aplicar_login(LoginRegistro(
                    user=operacao.user or "",
                    ts=operacao.time if operacao.time is not None else operacao.ts
                ))

            elif operacao.type == "replicate_channel":
                self.aplicar_canal(operacao.channel or "")

            elif operacao.type == "replicate_state":
                for registro in operacao.logins or []:
                    self.aplicar_login(registro)

                for canal in operacao.channels or []:
                    self.aplicar_canal(canal)

                for item in operacao.messages or []:
                    self.aplicar_mensagem(item)

            elif operacao.type == "replicate_post":
                for item in operacao.messages or []:
                    self.aplicar_mensagem(item)

            else:
                print(f"[{self.servidor_id}] Réplica com tipo desconhecido: {operacao.type}", flush=True)

    # As funções "aplicar" são idempotentes: aplicar duas vezes a mesma operação
    # não duplica dados (importante para a transferência de estado na entrada).

    def aplicar_login(self, registro: LoginRegistro) -> bool:
        if any(login.user == registro.user and login.ts == registro.ts for login in self.historico_logins):
            return False

        self.historico_logins.append(registro)
        self.historico_logins.sort(key=lambda login: login.ts)

        self.persistencia.salvar_logins(self.historico_logins)

        print(f"[{self.servidor_id}] Réplica aplicada: login de {registro.user}", flush=True)

        return True

    def aplicar_canal(self, canal: str) -> bool:
        if not canal.strip() or canal in self.canais:
            return False

        self.canais.add(canal)
        self.participantes[canal] = set()
        self.mensagens[canal] = []

        self.persistencia.salvar_canais(self.canais)
        self.persistencia.salvar_mensagens(canal, self.mensagens[canal])

        print(f"[{self.servidor_id}] Réplica aplicada: canal {canal}", flush=True)

        return True

    def aplicar_mensagem(self, mensagem: Envelope) -> bool:
        canal = mensagem.channel or ""

        # Garante o canal caso a mensagem chegue antes da criação replicada.
        self.aplicar_canal(canal)

        if canal not in self.mensagens or any(item.id == mensagem.id for item in self.mensagens[canal]):
            return False

        self.mensagens[canal].append(mensagem)
        self.ordenar_mensagens(canal)

        self.persistencia.salvar_mensagens(canal, self.mensagens[canal])

        print(f"[{self.servidor_id}] Réplica aplicada: [{canal}] {mensagem.user}: {mensagem.text}", flush=True)

        return True

    # Ordem total igual em todas as réplicas: relógio lógico, depois timestamp e Id.
    def ordenar_mensagens(self, canal: str) -> None:
        self.mensagens[canal].sort(key=lambda item: (item.clock, item.ts, item.id or ""))

    # Transferência de estado: ao iniciar, o servidor pede a outro servidor disponível
    # tudo o que perdeu enquanto estava fora (ou tudo, se for um servidor novo).
    def sincronizar_dados(self, lista: list[ServidorInfo]) -> None:
        for info in lista:
            if info.name == self.servidor_id:
                continue

            resposta = self.enviar_servidor(info.address, Envelope(
                type="sync"
            ))

            if resposta is None or resposta.status != "ok":
                continue

            novos = 0

            with self.trava_estado:
                for registro in resposta.logins or []:
                    if self.aplicar_login(registro):
                        novos += 1

                for canal in resposta.channels or []:
                    if self.aplicar_canal(canal):
                        novos += 1

                for item in resposta.messages or []:
                    if self.aplicar_mensagem(item):
                        novos += 1

            print(f"[{self.servidor_id}] Dados sincronizados com {info.name}: {novos} itens novos.", flush=True)
            return

        print(f"[{self.servidor_id}] Nenhum outro servidor disponível para sincronizar dados.", flush=True)

    def divulgar_estado(self) -> None:
        estado = self.estado_completo()

        estado.type = "replicate_state"
        estado.status = None

        print(f"[{self.servidor_id}] Enviando estado completo aos outros servidores.", flush=True)

        self.replicar(estado)

    def estado_completo(self) -> Envelope:
        with self.trava_estado:
            return Envelope(
                type="sync_reply",
                status="ok",
                server=self.servidor_id,
                channels=sorted(self.canais),
                messages=[item for lista in self.mensagens.values() for item in lista],
                logins=list(self.historico_logins)
            )

    # ===== Operações dos clientes =====

    def login(self, mensagem: Envelope) -> Envelope:
        usuario = mensagem.user or ""

        if not re.fullmatch(r"[A-Za-z0-9_-]{3,32}", usuario):
            return Envelope(
                type="login_reply",
                status="error",
                error="Nome de usuário inválido"
            )

        if usuario.lower() in self.usuarios:
            return Envelope(
                type="login_reply",
                status="error",
                error="Usuário já está registrado"
            )

        self.usuarios.add(usuario.lower())

        registro = LoginRegistro(
            user=usuario,
            ts=self.hora_atual()
        )

        self.historico_logins.append(registro)

        self.persistencia.salvar_logins(self.historico_logins)

        self.replicar(Envelope(
            type="replicate_login",
            user=usuario,
            time=registro.ts
        ))

        print(f"Usuário conectado: {usuario}", flush=True)

        return Envelope(
            type="login_reply",
            status="ok",
            user=usuario
        )

    def criar_canal(self, mensagem: Envelope) -> Envelope:
        canal = mensagem.channel or ""

        if not re.fullmatch(r"[a-z0-9_-]{3,24}", canal):
            return Envelope(
                type="create_channel_reply",
                status="error",
                error="Nome de canal inválido"
            )

        if canal == "servers" or canal == "replication":
            return Envelope(
                type="create_channel_reply",
                status="error",
                error="Nome de canal reservado"
            )

        if canal in self.canais:
            return Envelope(
                type="create_channel_reply",
                status="error",
                error="Canal já existe"
            )

        self.canais.add(canal)
        self.participantes[canal] = set()
        self.mensagens[canal] = []

        self.persistencia.salvar_canais(self.canais)
        self.persistencia.salvar_mensagens(canal, self.mensagens[canal])

        self.replicar(Envelope(
            type="replicate_channel",
            channel=canal
        ))

        print(f"Canal criado: {canal}", flush=True)

        return Envelope(
            type="create_channel_reply",
            status="ok",
            channel=canal
        )

    def listar_canais(self) -> Envelope:
        return Envelope(
            type="list_channels_reply",
            status="ok",
            channels=sorted(self.canais)
        )

    def entrar_canal(self, mensagem: Envelope) -> Envelope:
        usuario = mensagem.user or ""
        canal = mensagem.channel or ""

        if usuario.lower() not in self.usuarios:
            return Envelope(
                type="join_channel_reply",
                status="error",
                error="Usuário não está registrado"
            )

        if canal not in self.canais:
            return Envelope(
                type="join_channel_reply",
                status="error",
                error="Canal não encontrado"
            )

        if usuario.lower() in self.participantes[canal]:
            return Envelope(
                type="join_channel_reply",
                status="error",
                error="Usuário já está no canal"
            )

        self.participantes[canal].add(usuario.lower())

        print(f"Usuário {usuario} entrou no canal {canal}", flush=True)

        return Envelope(
            type="join_channel_reply",
            status="ok",
            user=usuario,
            channel=canal
        )

    def sair_canal(self, mensagem: Envelope) -> Envelope:
        usuario = mensagem.user or ""
        canal = mensagem.channel or ""

        if usuario.lower() not in self.usuarios:
            return Envelope(
                type="leave_channel_reply",
                status="error",
                error="Usuário não está registrado"
            )

        if canal not in self.canais:
            return Envelope(
                type="leave_channel_reply",
                status="error",
                error="Canal não encontrado"
            )

        if usuario.lower() not in self.participantes[canal]:
            return Envelope(
                type="leave_channel_reply",
                status="error",
                error="Usuário não está no canal"
            )

        self.participantes[canal].remove(usuario.lower())

        print(f"Usuário {usuario} saiu do canal {canal}", flush=True)

        return Envelope(
            type="leave_channel_reply",
            status="ok",
            user=usuario,
            channel=canal
        )

    def publicar_mensagem(self, mensagem: Envelope) -> Envelope:
        usuario = mensagem.user or ""
        canal = mensagem.channel or ""
        texto = mensagem.text or ""

        if usuario.lower() not in self.usuarios:
            return Envelope(
                type="post_reply",
                status="error",
                error="Usuário não está registrado"
            )

        if canal not in self.canais:
            return Envelope(
                type="post_reply",
                status="error",
                error="Canal não encontrado"
            )

        if usuario.lower() not in self.participantes[canal]:
            return Envelope(
                type="post_reply",
                status="error",
                error="Usuário não participa do canal"
            )

        if not texto.strip():
            return Envelope(
                type="post_reply",
                status="error",
                error="Mensagem não pode estar vazia"
            )

        nova_mensagem = Envelope(
            id=str(uuid.uuid4()),
            type="post",
            user=usuario,
            channel=canal,
            text=texto,
            from_=usuario,
            ts=self.hora_atual(),
            clock=self.incrementar_relogio()
        )

        self.mensagens[canal].append(nova_mensagem)
        self.ordenar_mensagens(canal)
        self.persistencia.salvar_mensagens(canal, self.mensagens[canal])
        log_mensagem.exibir("SERVIDOR", "ENVIANDO PUB", nova_mensagem)

        with self.trava_publisher:
            self.publisher.send_multipart([canal.encode(), nova_mensagem.serializar()])

        print(f"[{canal}] {usuario}: {texto}", flush=True)

        self.replicar(Envelope(
            type="replicate_post",
            channel=canal,
            messages=[nova_mensagem]
        ))

        return Envelope(
            type="post_reply",
            status="ok",
            channel=canal
        )

    def consultar_mensagens(self, mensagem: Envelope) -> Envelope:
        usuario = mensagem.user or ""
        canal = mensagem.channel or ""

        if usuario.lower() not in self.usuarios:
            return Envelope(
                type="get_messages_reply",
                status="error",
                error="Usuário não está registrado"
            )

        if canal not in self.canais:
            return Envelope(
                type="get_messages_reply",
                status="error",
                error="Canal não encontrado"
            )

        if usuario.lower() not in self.participantes[canal]:
            return Envelope(
                type="get_messages_reply",
                status="error",
                error="Usuário não participa do canal"
            )

        print(f"Usuário {usuario} consultou as mensagens do canal {canal}", flush=True)

        return Envelope(
            type="get_messages_reply",
            status="ok",
            channel=canal,
            messages=list(self.mensagens[canal])
        )

    def logout(self, mensagem: Envelope) -> Envelope:
        usuario = mensagem.user or ""

        if usuario.lower() not in self.usuarios:
            return Envelope(
                type="logout_reply",
                status="error",
                error="Usuário não está registrado"
            )

        self.usuarios.remove(usuario.lower())

        for usuarios_canal in self.participantes.values():
            usuarios_canal.discard(usuario.lower())

        print(f"Usuário desconectado: {usuario}", flush=True)

        return Envelope(
            type="logout_reply",
            status="ok",
            user=usuario
        )
