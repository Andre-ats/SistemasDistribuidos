
import os
import queue
import random
import sys
import threading
import time
import uuid

import zmq

from Common import log_mensagem
from Common.envelope import Envelope


def agora_ms() -> int:
    return int(time.time() * 1000)


def formatar_hora(milissegundos: int) -> str:
    # Mesmo formato do C# (":O"): 2026-11-18T06:38:57.5330000+00:00
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(milissegundos // 1000)) + f".{milissegundos % 1000:03d}0000+00:00"


servidor_host = os.environ.get("SERVER_HOST") or "localhost"

porta_servidor = int(os.environ.get("SERVER_PORT") or "6000")

proxy_host = os.environ.get("PROXY_HOST") or "localhost"

contexto = zmq.Context()

socket = contexto.socket(zmq.REQ)

socket.connect(f"tcp://{servidor_host}:{porta_servidor}")

print(f"Conectado ao servidor na porta {porta_servidor}", flush=True)

usuario_logado: str | None = None
relogio_logico = 0
trava_relogio = threading.Lock()

canais_inscritos: set[str] = set()
trava_canais = threading.Lock()

# No modo interativo, enquanto o usuário digita, as publicações recebidas ficam
# guardadas e só são exibidas depois, para não misturar com o que está sendo digitado.
pausar_mensagens = threading.Event()
mensagens_pendentes: queue.Queue[str] = queue.Queue()


def incrementar_relogio() -> int:
    global relogio_logico

    with trava_relogio:
        relogio_logico += 1
        return relogio_logico


def atualizar_relogio(recebido: int) -> int:
    global relogio_logico

    with trava_relogio:
        relogio_logico = max(relogio_logico, recebido)
        return relogio_logico


def copiar_canais() -> list[str]:
    with trava_canais:
        return list(canais_inscritos)


def receber_mensagens() -> None:
    subscriber = contexto.socket(zmq.SUB)

    subscriber.connect(f"tcp://{proxy_host}:5558")
    # O socket SUB pertence exclusivamente a esta thread.
    # As inscrições desejadas são compartilhadas pelo conjunto canais_inscritos.
    topicos_ativos: set[str] = set()

    while True:
        desejados = set(copiar_canais())

        # Sincroniza inscrições reais do ZeroMQ com os canais do cliente.
        for topico in list(topicos_ativos):
            if topico not in desejados:
                subscriber.setsockopt_string(zmq.UNSUBSCRIBE, topico)
                topicos_ativos.remove(topico)

        for topico in desejados:
            if topico not in topicos_ativos:
                subscriber.setsockopt_string(zmq.SUBSCRIBE, topico)
                topicos_ativos.add(topico)

        # Timeout curto permite atualizar inscrições sem bloquear a thread.
        if not subscriber.poll(100):
            continue

        canal_bytes, dados_mensagem = subscriber.recv_multipart()
        canal = canal_bytes.decode()

        if canal not in desejados:
            continue

        mensagem_recebida = Envelope.desserializar(dados_mensagem)
        recebido_em = agora_ms()

        novo = atualizar_relogio(mensagem_recebida.clock)

        texto = []

        texto.append(log_mensagem.formatar("CLIENTE", "RECEBENDO PUB", mensagem_recebida))
        texto.append(f"Relógio lógico do Cliente: {novo}")
        texto.append("")
        texto.append(f"[{canal}] {mensagem_recebida.user}: {mensagem_recebida.text}")
        texto.append(f"Enviada em: {formatar_hora(mensagem_recebida.ts)}")
        texto.append(f"Recebida em: {formatar_hora(recebido_em)}")

        saida = "\n".join(texto) + "\n"

        if pausar_mensagens.is_set():
            mensagens_pendentes.put(saida)
            continue

        print(saida, end="", flush=True)
        print("Escolha uma opção: ", end="", flush=True)


thread_mensagens = threading.Thread(target=receber_mensagens, daemon=True)
thread_mensagens.start()

modo_cliente = os.environ.get("CLIENT_MODE") or "interactive"


# ===== Modo automático (bots) =====

def executar_bot() -> None:
    usuario = os.environ.get("CLIENT_USER") or "cliente1"

    print(f"Modo automático iniciado: {usuario}", flush=True)

    def enviar_automatico(tipo: str, nome_canal: str | None = None, texto: str | None = None) -> Envelope | None:
        for tentativa in range(1, 6):
            requisicao_socket = contexto.socket(zmq.REQ)
            requisicao_socket.setsockopt(zmq.LINGER, 0)

            try:
                requisicao_socket.connect(f"tcp://{servidor_host}:{porta_servidor}")

                requisicao = Envelope(
                    type=tipo,
                    ts=agora_ms(),
                    clock=incrementar_relogio(),
                    from_="Cliente",
                    user=usuario,
                    channel=nome_canal,
                    text=texto
                )

                log_mensagem.exibir("CLIENTE", "ENVIANDO REQ", requisicao)

                requisicao_socket.send(requisicao.serializar())

                if not requisicao_socket.poll(3000):
                    print(f"[{usuario}] Sem resposta. Tentativa {tentativa}/5", flush=True)

                    time.sleep(2)
                    continue

                resposta = Envelope.desserializar(requisicao_socket.recv())

                log_mensagem.exibir("CLIENTE", "RECEBENDO REP", resposta)

                novo = atualizar_relogio(resposta.clock)

                print(f"[{usuario}] Relógio lógico do Cliente: {novo}", flush=True)

                return resposta
            except Exception as ex:
                print(f"[{usuario}] Erro: {ex}", flush=True)
                time.sleep(2)
            finally:
                requisicao_socket.close()

        return None

    # O servidor persiste o histórico, mas as sessões e participantes são voláteis.
    # Após reinício, refazemos login e reinscrevemos o bot nos canais anteriores.
    def recuperar_sessao() -> bool:
        print(f"[{usuario}] RECUPERACAO: servidor perdeu a sessão; refazendo login...", flush=True)
        novo_login = enviar_automatico("login")
        if novo_login is None or novo_login.status != "ok":
            erro = novo_login.error if novo_login is not None else "sem resposta"
            print(f"[{usuario}] RECUPERACAO: login não realizado: {erro}", flush=True)
            return False

        listagem_atual = enviar_automatico("list_channels")
        if listagem_atual is None or listagem_atual.status != "ok":
            print(f"[{usuario}] RECUPERACAO: falha ao consultar canais.", flush=True)
            return False

        existentes = set(listagem_atual.channels or [])
        desejados = copiar_canais()

        with trava_canais:
            canais_inscritos.clear()

        for nome in desejados:
            if nome not in existentes:
                continue
            entrada = enviar_automatico("join_channel", nome)
            if entrada is not None and (entrada.status == "ok" or entrada.error == "Usuário já está no canal"):
                with trava_canais:
                    canais_inscritos.add(nome)
                print(f"[{usuario}] RECUPERACAO: inscrito novamente em '{nome}'", flush=True)

        print(f"[{usuario}] RECUPERACAO CONCLUIDA: {len(copiar_canais())} canais.", flush=True)
        return True

    def enviar_com_recuperacao(tipo: str, nome_canal: str | None = None, texto: str | None = None) -> Envelope | None:
        resposta = enviar_automatico(tipo, nome_canal, texto)
        # Não repetir uma publicação que apenas perdeu a resposta: isso poderia duplicá-la.
        # Só repetimos quando o servidor confirmou que a sessão não existe.
        if resposta is not None and resposta.status == "error" and resposta.error == "Usuário não está registrado":
            if recuperar_sessao():
                return enviar_automatico(tipo, nome_canal, texto)
        return resposta

    # Em caso de erro, o login é repetido até ser aceito.
    login = enviar_automatico("login")

    while login is None or login.status != "ok":
        erro = login.error if login is not None else "Servidor indisponível"
        print(f"[{usuario}] Login falhou: {erro}", flush=True)

        # O bot reiniciou, mas o servidor ainda guarda a sessão antiga: encerra e entra de novo.
        if login is not None and login.error == "Usuário já está registrado":
            enviar_automatico("logout")
        else:
            time.sleep(2)

        print(f"[{usuario}] Repetindo login...", flush=True)
        login = enviar_automatico("login")

    print(f"[{usuario}] Login realizado!", flush=True)

    textos = ["Bom dia!", "Tudo certo?", "Alguém por aqui?", "Mensagem de teste", "Olá pessoal!", "Mais uma mensagem", "Teste Pub/Sub"]

    while True:
        listagem = enviar_com_recuperacao("list_channels")
        if listagem is None or listagem.status != "ok":
            print(f"[{usuario}] Não foi possível listar canais.", flush=True)
            time.sleep(2)
            continue

        disponiveis = list(listagem.channels or [])
        if len(disponiveis) < 5:
            novo_canal = f"canal_{usuario}_{uuid.uuid4().hex[:6]}"
            criacao = enviar_com_recuperacao("create_channel", novo_canal)
            if criacao is not None and criacao.status == "ok":
                disponiveis.append(novo_canal)
                print(f"[{usuario}] Canal '{novo_canal}' criado!", flush=True)

        if len(copiar_canais()) < 3:
            inscritos_agora = set(copiar_canais())
            candidatos = [canal for canal in disponiveis if canal not in inscritos_agora]
            if candidatos:
                escolhido = random.choice(candidatos)
                entrada = enviar_com_recuperacao("join_channel", escolhido)
                if entrada is not None and entrada.status == "ok":
                    with trava_canais:
                        canais_inscritos.add(escolhido)
                    print(f"[{usuario}] Inscrito no canal '{escolhido}'", flush=True)

        inscritos = copiar_canais()
        if not inscritos:
            time.sleep(1)
            continue

        canal_escolhido = random.choice(inscritos)
        for _ in range(10):
            texto = random.choice(textos)
            resposta = enviar_com_recuperacao("post", canal_escolhido, texto)
            status = resposta.status if resposta is not None else "sem resposta"
            print(f"[{usuario}] Publicação em {canal_escolhido}: {status}", flush=True)
            time.sleep(1)


# ===== Modo interativo =====

def ler_linha(rotulo: str | None = None) -> str | None:
    if rotulo is not None:
        print(rotulo, end="", flush=True)

    linha = sys.stdin.readline()

    if linha == "":
        return None

    return linha.rstrip("\n")


def executar_interativo() -> None:
    global usuario_logado

    while True:
        # Libera a exibição e mostra o que chegou enquanto o usuário digitava.
        pausar_mensagens.clear()

        if not mensagens_pendentes.empty():
            print(flush=True)
            print(f"===== Mensagens recebidas enquanto você digitava: {mensagens_pendentes.qsize()} =====", flush=True)

            while not mensagens_pendentes.empty():
                print(mensagens_pendentes.get(), end="", flush=True)

        print()
        print("===== BBS =====")

        if usuario_logado is not None:
            print(f"Usuário logado: {usuario_logado}")

        print("1 - Login")
        print("2 - Criar canal")
        print("3 - Listar canais")
        print("4 - Entrar em um canal")
        print("5 - Sair de um canal")
        print("6 - Enviar mensagem")
        print("7 - Consultar mensagens")
        print("8 - Logout")
        print("0 - Encerrar")

        opcao = ler_linha("Escolha uma opção: ")

        # A partir daqui o usuário vai digitar: segura as publicações recebidas.
        pausar_mensagens.set()

        if opcao is None:
            print("Entrada encerrada. Finalizando cliente.", flush=True)
            break

        opcao = opcao.strip()

        if opcao == "0":
            if usuario_logado is not None:
                logout = Envelope(
                    type="logout",
                    user=usuario_logado,
                    from_="Cliente",
                    ts=agora_ms(),
                    clock=incrementar_relogio()
                )

                log_mensagem.exibir("CLIENTE", "ENVIANDO REQ", logout)
                socket.send(logout.serializar())

                resultado = Envelope.desserializar(socket.recv())

                log_mensagem.exibir("CLIENTE", "RECEBENDO REP", resultado)

                if resultado.status == "ok":
                    print("Logout realizado com sucesso.", flush=True)
                else:
                    print(f"Erro ao realizar logout: {resultado.error}", flush=True)

            print("Cliente encerrado.", flush=True)
            break

        mensagem = Envelope(
            from_="Cliente",
            user=usuario_logado
        )

        if opcao == "1":
            if usuario_logado is not None:
                print("Você já está logado.")
                continue

            mensagem.type = "login"
            mensagem.user = ler_linha("Nome do usuário: ")

        elif opcao == "2":
            mensagem.type = "create_channel"
            mensagem.channel = ler_linha("Nome do canal: ")

        elif opcao == "3":
            mensagem.type = "list_channels"

        elif opcao == "4":
            mensagem.type = "join_channel"
            mensagem.channel = ler_linha("Nome do canal: ")

        elif opcao == "5":
            mensagem.type = "leave_channel"
            mensagem.channel = ler_linha("Nome do canal: ")

        elif opcao == "6":
            mensagem.channel = ler_linha("Nome do canal: ")
            mensagem.text = ler_linha("Mensagem: ")
            mensagem.type = "post"

        elif opcao == "7":
            mensagem.type = "get_messages"
            mensagem.channel = ler_linha("Nome do canal: ")

        elif opcao == "8":
            mensagem.type = "logout"

        else:
            print("Opção inválida.")
            continue

        if usuario_logado is None and opcao != "1" and opcao != "3":
            print("Você precisa fazer login primeiro.")
            continue

        # Timestamp do envio (e não de quando o menu montou a mensagem).
        mensagem.ts = agora_ms()
        mensagem.clock = incrementar_relogio()

        log_mensagem.exibir("CLIENTE", "ENVIANDO REQ", mensagem)
        socket.send(mensagem.serializar())

        resposta = Envelope.desserializar(socket.recv())
        log_mensagem.exibir("CLIENTE", "RECEBENDO REP", resposta)

        novo = atualizar_relogio(resposta.clock)

        print(f"Relógio lógico do Cliente: {novo}")

        print(f"Resposta do servidor: {resposta.status}")

        if resposta.status == "error":
            print(f"Erro: {resposta.error}")

        if opcao == "1" and resposta.status == "ok":
            usuario_logado = resposta.user
            print(f"Login realizado: {usuario_logado}")

        if opcao == "4" and resposta.status == "ok" and mensagem.channel is not None:
            with trava_canais:
                canais_inscritos.add(mensagem.channel)

        if opcao == "5" and resposta.status == "ok" and mensagem.channel is not None:
            with trava_canais:
                canais_inscritos.discard(mensagem.channel)

        if opcao == "8" and resposta.status == "ok":
            usuario_logado = None
            with trava_canais:
                canais_inscritos.clear()
            print("Logout realizado com sucesso.")

        if resposta.channels is not None:
            print("Canais cadastrados:")

            for canal in resposta.channels:
                print(f"- {canal}")

        if resposta.messages is not None:
            print("Mensagens do canal:")

            if len(resposta.messages) == 0:
                print("Nenhuma mensagem encontrada.")

            for msg in resposta.messages:
                print(f"ID: {msg.id}")
                print(f"[{msg.user}] {msg.text}")

        sys.stdout.flush()


if modo_cliente.lower() == "auto":
    executar_bot()
else:
    executar_interativo()
