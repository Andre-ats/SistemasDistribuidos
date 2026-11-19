
package server;

import common.Envelope;
import common.LogMensagem;
import common.LoginRegistro;
import common.ServidorInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Servidor {

    // Usuários e participantes ignoram maiúsculas/minúsculas (guardados em minúsculo).
    private final Set<String> usuarios = new HashSet<>();
    private final Set<String> canais = new HashSet<>();
    private final Map<String, Set<String>> participantes = new HashMap<>();
    private final Map<String, List<Envelope>> mensagens = new HashMap<>();
    private long relogioLogico = 0;

    // Relógio físico: hora local = hora do sistema + desvio (em ms).
    private long desvioRelogio = 0;
    private int rank = 0;
    private int mensagensClientes = 0;

    // Eleição (algoritmo do valentão): o servidor disponível de maior rank vira coordenador.
    private volatile String coordenador = null;
    private volatile String enderecoCoordenador = null;
    private final AtomicBoolean eleicaoEmAndamento = new AtomicBoolean(false);
    private final AtomicLong anunciosRecebidos = new AtomicLong(0);

    // Berkeley: diferença (ms) de cada servidor em relação ao coordenador.
    private final Map<String, Long> diferencasRelogio = new HashMap<>();

    private final Object travaRelogio = new Object();
    private final Object travaPublisher = new Object();
    private final Object travaEstado = new Object();

    private final Persistencia persistencia;
    private final List<LoginRegistro> historicoLogins = new ArrayList<>();

    private final int porta;
    private final int portaPub;
    private final int portaSincronizacao;
    private final String servidorId;
    private final String proxyHost = variavel("PROXY_HOST", "localhost");
    private final String referenciaHost = variavel("REFERENCE_HOST", "localhost");
    private final int referenciaPorta = Integer.parseInt(variavel("REFERENCE_PORT", "5000"));

    private final ZContext contexto = new ZContext();
    private ZMQ.Socket publisher;

    // Sockets REQ reaproveitados (referência e cada servidor), recriados só quando a
    // resposta não chega (padrão "Lazy Pirate" do ZeroMQ). Cada trava garante que
    // apenas uma thread usa o socket por vez.
    private ZMQ.Socket socketReferencia;
    private final Map<String, ZMQ.Socket> socketsServidores = new HashMap<>();
    private final Object travaReferencia = new Object();
    private final Object travaServidores = new Object();

    private static final Pattern NOME_USUARIO = Pattern.compile("^[A-Za-z0-9_-]{3,32}$");
    private static final Pattern NOME_CANAL = Pattern.compile("^[a-z0-9_-]{3,24}$");

    public Servidor(int porta, int portaPub, String servidorId) {
        this.porta = porta;
        this.portaPub = portaPub;
        this.servidorId = servidorId;

        portaSincronizacao = Integer.parseInt(variavel("SYNC_PORT", String.valueOf(porta + 2)));

        // Desvio inicial opcional para simular um relógio físico atrasado/adiantado.
        desvioRelogio = Long.parseLong(variavel("CLOCK_DRIFT_MS", "0"));

        persistencia = new Persistencia(servidorId);
    }

    private static String variavel(String nome, String padrao) {
        String valor = System.getenv(nome);
        return valor != null ? valor : padrao;
    }

    private static void iniciarThread(Runnable tarefa) {
        Thread thread = new Thread(tarefa);
        thread.setDaemon(true);
        thread.start();
    }

    private static void esperar(long milissegundos) {
        try {
            Thread.sleep(milissegundos);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    // ===== Relógios =====

    private long horaAtual() {
        synchronized (travaRelogio) {
            return System.currentTimeMillis() + desvioRelogio;
        }
    }

    private long incrementarRelogio() {
        synchronized (travaRelogio) {
            relogioLogico++;
            return relogioLogico;
        }
    }

    private long atualizarRelogio(long recebido) {
        synchronized (travaRelogio) {
            relogioLogico = Math.max(relogioLogico, recebido);
            return relogioLogico;
        }
    }

    public void iniciar() {
        historicoLogins.addAll(persistencia.carregarLogins());

        for (String canal : persistencia.carregarCanais()) {
            canais.add(canal);
            participantes.put(canal, new HashSet<>());
            mensagens.put(canal, persistencia.carregarMensagens(canal));
        }

        // Regrava os arquivos na ordem padrão (dados antigos podem estar em outra ordem).
        for (String canal : canais) {
            ordenarMensagens(canal);
            persistencia.salvarMensagens(canal, mensagens.get(canal));
        }

        persistencia.salvarCanais(canais);
        persistencia.salvarLogins(historicoLogins);

        System.out.println("Logins registrados: " + historicoLogins.size());
        System.out.println("Canais carregados: " + canais.size());

        ZMQ.Socket socket = contexto.createSocket(SocketType.REP);

        socket.bind("tcp://*:" + porta);

        publisher = contexto.createSocket(SocketType.PUB);

        publisher.connect("tcp://" + proxyHost + ":5557");

        System.out.println("Servidor iniciado na porta " + porta + "!");
        System.out.println("Publicações enviadas ao proxy " + proxyHost + ":5557!");
        System.out.println("Relógio físico inicial: " + LogMensagem.formatarHora(horaAtual()) + " (desvio " + desvioRelogio + " ms)");

        pedirRank();
        listarServidores();

        iniciarThread(this::atenderServidores);
        iniciarThread(this::ouvirServidores);

        // O SUB de "replication" já está ativo; então busca o que foi perdido.
        esperar(1000);
        sincronizarDados(listarServidores());

        // Também envia aos outros o que só este servidor tem (ex.: dados de antes de cair).
        // Espera um pouco para os outros servidores já estarem assinando "replication".
        iniciarThread(() -> {
            esperar(10000);
            divulgarEstado();
        });

        // Aguarda o SUB conectar ao proxy antes da primeira eleição.
        iniciarThread(() -> {
            esperar(3000);
            eleicao();
        });

        while (true) {
            byte[] dados = socket.recv(0);

            Envelope mensagem = Envelope.desserializar(dados);
            LogMensagem.exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            System.out.println("Relógio do servidor: " + atualizarRelogio(mensagem.clock));

            System.out.println("Mensagem recebida: " + mensagem.type);

            Envelope resposta;

            synchronized (travaEstado) {
                resposta = switch (mensagem.type.toLowerCase()) {
                    case "login" -> login(mensagem);
                    case "create_channel" -> criarCanal(mensagem);
                    case "list_channels" -> listarCanais();
                    case "join_channel" -> entrarCanal(mensagem);
                    case "leave_channel" -> sairCanal(mensagem);
                    case "post" -> publicarMensagem(mensagem);
                    case "get_messages" -> consultarMensagens(mensagem);
                    case "logout" -> logout(mensagem);
                    default -> new Envelope()
                        .type("error")
                        .status("error")
                        .error("Tipo de mensagem inválido");
                };
            }

            resposta.ts = horaAtual();
            resposta.clock = incrementarRelogio();
            resposta.from = "Server";

            LogMensagem.exibir("SERVIDOR", "ENVIANDO REP", resposta);

            socket.send(resposta.serializar(), 0);

            mensagensClientes++;

            // Heartbeat a cada 10 mensagens de clientes recebidas.
            if (mensagensClientes % 10 == 0) {
                enviarHeartbeat();
            }

            // Sincronização do relógio físico com o coordenador a cada 15 mensagens.
            if (mensagensClientes % 15 == 0) {
                sincronizarRelogio();
            }
        }
    }

    // ===== Serviço de referência =====

    private ZMQ.Socket criarSocketRequisicao(String endereco, int timeout) {
        ZMQ.Socket requisicaoSocket = contexto.createSocket(SocketType.REQ);
        requisicaoSocket.setLinger(0);
        requisicaoSocket.setReceiveTimeOut(timeout);
        requisicaoSocket.connect("tcp://" + endereco);

        return requisicaoSocket;
    }

    private void descartarSocketReferencia() {
        if (socketReferencia != null) {
            contexto.destroySocket(socketReferencia);
            socketReferencia = null;
        }
    }

    private Envelope enviarReferencia(Envelope requisicao) {
        synchronized (travaReferencia) {
            return enviarReferenciaComTentativas(requisicao);
        }
    }

    private Envelope enviarReferenciaComTentativas(Envelope requisicao) {
        for (int tentativa = 1; tentativa <= 3; tentativa++) {
            try {
                if (socketReferencia == null) {
                    socketReferencia = criarSocketRequisicao(referenciaHost + ":" + referenciaPorta, 3000);
                }

                ZMQ.Socket requisicaoSocket = socketReferencia;

                requisicao.ts = horaAtual();
                requisicao.clock = incrementarRelogio();
                requisicao.from = servidorId;

                LogMensagem.exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

                requisicaoSocket.send(requisicao.serializar(), 0);

                byte[] respostaBytes = requisicaoSocket.recv(0);

                if (respostaBytes == null) {
                    System.out.println("[" + servidorId + "] Referência sem resposta. Tentativa " + tentativa + "/3");

                    descartarSocketReferencia();
                    esperar(1000);
                    continue;
                }

                Envelope resposta = Envelope.desserializar(respostaBytes);

                LogMensagem.exibir("SERVIDOR", "RECEBENDO REP", resposta);

                System.out.println("Relógio do servidor: " + atualizarRelogio(resposta.clock));

                return resposta;
            } catch (Exception ex) {
                System.out.println("[" + servidorId + "] Erro com a referência: " + ex.getMessage());
                descartarSocketReferencia();
                esperar(1000);
            }
        }

        return null;
    }

    private void pedirRank() {
        while (true) {
            Envelope resposta = enviarReferencia(new Envelope()
                .type("rank")
                .server(servidorId)
                .address(servidorId + ":" + portaSincronizacao));

            if (resposta != null && "ok".equals(resposta.status) && resposta.rank != null) {
                rank = resposta.rank;
                System.out.println("[" + servidorId + "] Rank recebido: " + rank);
                return;
            }

            System.out.println("[" + servidorId + "] Não foi possível obter o rank. Tentando novamente...");
            esperar(2000);
        }
    }

    private List<ServidorInfo> listarServidores() {
        Envelope resposta = enviarReferencia(new Envelope()
            .type("list"));

        List<ServidorInfo> lista = resposta != null && resposta.servers != null
            ? resposta.servers
            : new ArrayList<>();

        System.out.println("[" + servidorId + "] Servidores disponíveis:");

        for (ServidorInfo info : lista) {
            System.out.println("- " + info.name + " (rank " + info.rank + ") em " + info.address);
        }

        return lista;
    }

    private void enviarHeartbeat() {
        Envelope resposta = enviarReferencia(new Envelope()
            .type("heartbeat")
            .server(servidorId));

        if (resposta != null && "error".equals(resposta.status) && "Servidor não registrado".equals(resposta.error)) {
            pedirRank();
            return;
        }

        if (resposta == null || !"ok".equals(resposta.status)) {
            System.out.println("[" + servidorId + "] Heartbeat sem resposta.");
            return;
        }

        System.out.println("[" + servidorId + "] Heartbeat confirmado: " + resposta.text);
    }

    private void ajustarRelogioFisico(long horaCorreta, long enviadoEm, long recebidoEm) {
        // Compensa metade do tempo de ida e volta (algoritmo de Cristian).
        long horaEstimada = horaCorreta + (recebidoEm - enviadoEm) / 2;
        long antes = horaAtual();

        synchronized (travaRelogio) {
            desvioRelogio = horaEstimada - recebidoEm;
        }

        long depois = horaAtual();

        System.out.println("[" + servidorId + "] Relógio físico ajustado:");
        System.out.println("  Antes: " + LogMensagem.formatarHora(antes));
        System.out.println("  Depois: " + LogMensagem.formatarHora(depois));
        System.out.println("  Correção: " + (depois - antes) + " ms");
    }

    // ===== Comunicação entre servidores =====

    // Atende as requisições de outros servidores (relógio, eleição e sync) em uma porta própria,
    // para não misturar com as requisições dos clientes.
    private void atenderServidores() {
        ZMQ.Socket socketServidores = contexto.createSocket(SocketType.REP);

        socketServidores.bind("tcp://*:" + portaSincronizacao);

        System.out.println("[" + servidorId + "] Atendendo servidores na porta " + portaSincronizacao + "!");

        while (true) {
            byte[] dados = socketServidores.recv(0);

            Envelope mensagem = Envelope.desserializar(dados);
            LogMensagem.exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            System.out.println("Relógio do servidor: " + atualizarRelogio(mensagem.clock));

            Envelope resposta;

            switch (mensagem.type.toLowerCase()) {
                case "clock" -> resposta = relogioCoordenador(mensagem);

                case "sync" -> resposta = estadoCompleto();

                case "election" -> {
                    resposta = new Envelope()
                        .type("election_reply")
                        .status("ok")
                        .server(servidorId)
                        .rank(rank)
                        .text("OK");

                    // Responde OK e assume a eleição a partir deste servidor.
                    iniciarThread(this::eleicao);
                }

                default -> resposta = new Envelope()
                    .type("error")
                    .status("error")
                    .error("Tipo de mensagem inválido");
            }

            resposta.ts = horaAtual();
            resposta.clock = incrementarRelogio();
            resposta.from = servidorId;

            LogMensagem.exibir("SERVIDOR", "ENVIANDO REP", resposta);

            socketServidores.send(resposta.serializar(), 0);
        }
    }

    // Recebe pelo tópico "servers" o nome do servidor eleito como coordenador
    // e pelo tópico "replication" as escritas feitas nos outros servidores.
    private void ouvirServidores() {
        ZMQ.Socket subscriber = contexto.createSocket(SocketType.SUB);

        subscriber.connect("tcp://" + proxyHost + ":5558");
        subscriber.subscribe("servers".getBytes(ZMQ.CHARSET));
        subscriber.subscribe("replication".getBytes(ZMQ.CHARSET));

        while (true) {
            String topico = subscriber.recvStr(0);
            byte[] dados = subscriber.recv(0);

            if ("replication".equals(topico)) {
                Envelope operacao = Envelope.desserializar(dados);

                if (servidorId.equals(operacao.server)) {
                    continue;
                }

                LogMensagem.exibir("SERVIDOR", "RECEBENDO PUB", operacao);

                System.out.println("Relógio do servidor: " + atualizarRelogio(operacao.clock));

                aplicarReplicacao(operacao);
                continue;
            }

            if (!"servers".equals(topico)) {
                continue;
            }

            Envelope anuncio = Envelope.desserializar(dados);
            LogMensagem.exibir("SERVIDOR", "RECEBENDO PUB", anuncio);

            System.out.println("Relógio do servidor: " + atualizarRelogio(anuncio.clock));

            if (anuncio.rank != null && anuncio.rank < rank) {
                // Valentão: um servidor de rank menor se anunciou, então este servidor assume.
                System.out.println("[" + servidorId + "] Coordenador " + anuncio.server + " tem rank menor; iniciando eleição.");
                iniciarThread(this::eleicao);
                continue;
            }

            coordenador = anuncio.server;
            enderecoCoordenador = anuncio.address;
            anunciosRecebidos.incrementAndGet();

            synchronized (travaRelogio) {
                diferencasRelogio.clear();
            }

            System.out.println("[" + servidorId + "] Coordenador atual: " + coordenador + " (" + enderecoCoordenador + ")");
        }
    }

    private void descartarSocketServidor(String endereco) {
        ZMQ.Socket requisicaoSocket = socketsServidores.remove(endereco);

        if (requisicaoSocket != null) {
            contexto.destroySocket(requisicaoSocket);
        }
    }

    private Envelope enviarServidor(String endereco, Envelope requisicao) {
        synchronized (travaServidores) {
            return enviarServidorUmaVez(endereco, requisicao);
        }
    }

    private Envelope enviarServidorUmaVez(String endereco, Envelope requisicao) {
        try {
            ZMQ.Socket requisicaoSocket = socketsServidores.computeIfAbsent(
                endereco, destino -> criarSocketRequisicao(destino, 2000));

            requisicao.ts = horaAtual();
            requisicao.clock = incrementarRelogio();
            requisicao.from = servidorId;

            if (requisicao.server == null) {
                requisicao.server = servidorId;
            }

            LogMensagem.exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

            requisicaoSocket.send(requisicao.serializar(), 0);

            byte[] respostaBytes = requisicaoSocket.recv(0);

            if (respostaBytes == null) {
                System.out.println("[" + servidorId + "] Servidor " + endereco + " sem resposta.");
                descartarSocketServidor(endereco);
                return null;
            }

            Envelope resposta = Envelope.desserializar(respostaBytes);

            LogMensagem.exibir("SERVIDOR", "RECEBENDO REP", resposta);

            System.out.println("Relógio do servidor: " + atualizarRelogio(resposta.clock));

            return resposta;
        } catch (Exception ex) {
            System.out.println("[" + servidorId + "] Erro com o servidor " + endereco + ": " + ex.getMessage());
            descartarSocketServidor(endereco);
            return null;
        }
    }

    // ===== Eleição =====

    private void eleicao() {
        if (!eleicaoEmAndamento.compareAndSet(false, true)) {
            System.out.println("[" + servidorId + "] Eleição já em andamento.");
            return;
        }

        try {
            while (true) {
                System.out.println("[" + servidorId + "] Iniciando eleição (rank " + rank + ")");

                List<ServidorInfo> lista = listarServidores();
                boolean recebeuOk = false;

                for (ServidorInfo info : lista) {
                    if (info.rank <= rank || info.name.equals(servidorId)) {
                        continue;
                    }

                    Envelope resposta = enviarServidor(info.address, new Envelope()
                        .type("election")
                        .rank(rank));

                    if (resposta != null && "ok".equals(resposta.status)) {
                        recebeuOk = true;
                    }
                }

                if (!recebeuOk) {
                    tornarCoordenador();
                    return;
                }

                // Um servidor de rank maior respondeu: aguarda o anúncio dele no tópico "servers".
                System.out.println("[" + servidorId + "] Servidor de rank maior respondeu; aguardando anúncio do coordenador...");

                long antes = anunciosRecebidos.get();

                for (int i = 0; i < 20 && anunciosRecebidos.get() == antes; i++) {
                    esperar(500);
                }

                if (anunciosRecebidos.get() != antes) {
                    return;
                }

                System.out.println("[" + servidorId + "] Nenhum coordenador anunciado; repetindo eleição.");
            }
        } finally {
            eleicaoEmAndamento.set(false);
        }
    }

    private void tornarCoordenador() {
        coordenador = servidorId;
        enderecoCoordenador = servidorId + ":" + portaSincronizacao;

        Envelope anuncio = new Envelope()
            .type("coordinator")
            .server(servidorId)
            .rank(rank)
            .address(enderecoCoordenador)
            .from(servidorId)
            .ts(horaAtual())
            .clock(incrementarRelogio());

        LogMensagem.exibir("SERVIDOR", "ENVIANDO PUB", anuncio);

        synchronized (travaPublisher) {
            publisher.sendMore("servers");
            publisher.send(anuncio.serializar(), 0);
        }

        System.out.println("[" + servidorId + "] Eleito como coordenador!");
    }

    private void sincronizarRelogio() {
        String atual = coordenador;
        String endereco = enderecoCoordenador;

        if (atual == null || endereco == null) {
            System.out.println("[" + servidorId + "] Nenhum coordenador conhecido; iniciando eleição.");
            iniciarThread(this::eleicao);
            return;
        }

        if (atual.equals(servidorId)) {
            System.out.println("[" + servidorId + "] Este servidor é o coordenador; relógio usado como referência.");
            return;
        }

        long enviadoEm = System.currentTimeMillis();

        Envelope resposta = enviarServidor(endereco, new Envelope()
            .type("clock"));

        long recebidoEm = System.currentTimeMillis();

        if (resposta == null || !"ok".equals(resposta.status) || resposta.time == null) {
            System.out.println("[" + servidorId + "] Coordenador " + atual + " indisponível; iniciando eleição.");
            coordenador = null;
            enderecoCoordenador = null;
            iniciarThread(this::eleicao);
            return;
        }

        System.out.println("[" + servidorId + "] " + resposta.text);

        ajustarRelogioFisico(resposta.time, enviadoEm, recebidoEm);
    }

    // Berkeley no coordenador: guarda a diferença de cada servidor, calcula a média
    // (incluindo o próprio coordenador com diferença 0), ajusta o próprio relógio
    // pela média e devolve a hora correta para o servidor que pediu.
    private Envelope relogioCoordenador(Envelope mensagem) {
        if (!servidorId.equals(coordenador)) {
            return new Envelope()
                .type("clock_reply")
                .status("error")
                .error("Servidor não é o coordenador");
        }

        String nome = mensagem.server != null ? mensagem.server : mensagem.from;
        long media;
        long horaCorreta;
        int quantidade;

        synchronized (travaRelogio) {
            long agora = System.currentTimeMillis() + desvioRelogio;

            diferencasRelogio.put(nome, mensagem.ts - agora);

            long soma = 0;

            for (long diferenca : diferencasRelogio.values()) {
                soma += diferenca;
            }

            media = soma / (diferencasRelogio.size() + 1);

            desvioRelogio += media;

            for (Map.Entry<String, Long> item : diferencasRelogio.entrySet()) {
                item.setValue(item.getValue() - media);
            }

            // Quem pediu passa a usar a hora correta, então sua diferença vira zero.
            diferencasRelogio.put(nome, 0L);

            horaCorreta = System.currentTimeMillis() + desvioRelogio;
            quantidade = diferencasRelogio.size() + 1;
        }

        System.out.println("[" + servidorId + "] Berkeley: " + quantidade + " relógios, média " + media + " ms");

        return new Envelope()
            .type("clock_reply")
            .status("ok")
            .server(servidorId)
            .time(horaCorreta)
            .text("Hora correta do coordenador " + servidorId + " (média Berkeley " + media + " ms)");
    }

    // ===== Replicação =====
    // Replicação ativa: toda escrita aceita por um servidor (login, canal e mensagem)
    // é publicada no tópico "replication" e aplicada por todos os outros servidores.

    private void replicar(Envelope operacao) {
        operacao.server = servidorId;
        operacao.from = servidorId;
        operacao.ts = horaAtual();
        operacao.clock = incrementarRelogio();

        LogMensagem.exibir("SERVIDOR", "ENVIANDO PUB", operacao);

        synchronized (travaPublisher) {
            publisher.sendMore("replication");
            publisher.send(operacao.serializar(), 0);
        }
    }

    private void aplicarReplicacao(Envelope operacao) {
        if (servidorId.equals(operacao.server)) {
            return;
        }

        synchronized (travaEstado) {
            switch (operacao.type) {
                case "replicate_login" -> aplicarLogin(new LoginRegistro(
                    operacao.user != null ? operacao.user : "",
                    operacao.time != null ? operacao.time : operacao.ts));

                case "replicate_channel" -> aplicarCanal(operacao.channel != null ? operacao.channel : "");

                case "replicate_state" -> {
                    for (LoginRegistro registro : naoNulo(operacao.logins)) {
                        aplicarLogin(registro);
                    }

                    for (String canal : naoNulo(operacao.channels)) {
                        aplicarCanal(canal);
                    }

                    for (Envelope item : naoNulo(operacao.messages)) {
                        aplicarMensagem(item);
                    }
                }

                case "replicate_post" -> {
                    for (Envelope item : naoNulo(operacao.messages)) {
                        aplicarMensagem(item);
                    }
                }

                default -> System.out.println("[" + servidorId + "] Réplica com tipo desconhecido: " + operacao.type);
            }
        }
    }

    private static <T> List<T> naoNulo(List<T> lista) {
        return lista != null ? lista : new ArrayList<>();
    }

    // As funções "aplicar" são idempotentes: aplicar duas vezes a mesma operação
    // não duplica dados (importante para a transferência de estado na entrada).

    private boolean aplicarLogin(LoginRegistro registro) {
        for (LoginRegistro login : historicoLogins) {
            if (login.user.equals(registro.user) && login.ts == registro.ts) {
                return false;
            }
        }

        historicoLogins.add(registro);
        historicoLogins.sort(Comparator.comparingLong(login -> login.ts));

        persistencia.salvarLogins(historicoLogins);

        System.out.println("[" + servidorId + "] Réplica aplicada: login de " + registro.user);

        return true;
    }

    private boolean aplicarCanal(String canal) {
        if (canal.isBlank() || canais.contains(canal)) {
            return false;
        }

        canais.add(canal);
        participantes.put(canal, new HashSet<>());
        mensagens.put(canal, new ArrayList<>());

        persistencia.salvarCanais(canais);
        persistencia.salvarMensagens(canal, mensagens.get(canal));

        System.out.println("[" + servidorId + "] Réplica aplicada: canal " + canal);

        return true;
    }

    private boolean aplicarMensagem(Envelope mensagem) {
        String canal = mensagem.channel != null ? mensagem.channel : "";

        // Garante o canal caso a mensagem chegue antes da criação replicada.
        aplicarCanal(canal);

        if (!mensagens.containsKey(canal)) {
            return false;
        }

        for (Envelope item : mensagens.get(canal)) {
            if (item.id != null && item.id.equals(mensagem.id)) {
                return false;
            }
        }

        mensagens.get(canal).add(mensagem);
        ordenarMensagens(canal);

        persistencia.salvarMensagens(canal, mensagens.get(canal));

        System.out.println("[" + servidorId + "] Réplica aplicada: [" + canal + "] " + mensagem.user + ": " + mensagem.text);

        return true;
    }

    // Ordem total igual em todas as réplicas: relógio lógico, depois timestamp e Id.
    private void ordenarMensagens(String canal) {
        mensagens.get(canal).sort(
            Comparator.comparingLong((Envelope item) -> item.clock)
                .thenComparingLong(item -> item.ts)
                .thenComparing(item -> item.id != null ? item.id : "")
        );
    }

    // Transferência de estado: ao iniciar, o servidor pede a outro servidor disponível
    // tudo o que perdeu enquanto estava fora (ou tudo, se for um servidor novo).
    private void sincronizarDados(List<ServidorInfo> lista) {
        for (ServidorInfo info : lista) {
            if (info.name.equals(servidorId)) {
                continue;
            }

            Envelope resposta = enviarServidor(info.address, new Envelope()
                .type("sync"));

            if (resposta == null || !"ok".equals(resposta.status)) {
                continue;
            }

            int novos = 0;

            synchronized (travaEstado) {
                for (LoginRegistro registro : naoNulo(resposta.logins)) {
                    if (aplicarLogin(registro)) novos++;
                }

                for (String canal : naoNulo(resposta.channels)) {
                    if (aplicarCanal(canal)) novos++;
                }

                for (Envelope item : naoNulo(resposta.messages)) {
                    if (aplicarMensagem(item)) novos++;
                }
            }

            System.out.println("[" + servidorId + "] Dados sincronizados com " + info.name + ": " + novos + " itens novos.");
            return;
        }

        System.out.println("[" + servidorId + "] Nenhum outro servidor disponível para sincronizar dados.");
    }

    private void divulgarEstado() {
        Envelope estado = estadoCompleto();

        estado.type = "replicate_state";
        estado.status = null;

        System.out.println("[" + servidorId + "] Enviando estado completo aos outros servidores.");

        replicar(estado);
    }

    private Envelope estadoCompleto() {
        synchronized (travaEstado) {
            List<String> listaCanais = new ArrayList<>(canais);
            listaCanais.sort(null);

            List<Envelope> todasMensagens = new ArrayList<>();

            for (List<Envelope> lista : mensagens.values()) {
                todasMensagens.addAll(lista);
            }

            return new Envelope()
                .type("sync_reply")
                .status("ok")
                .server(servidorId)
                .channels(listaCanais)
                .messages(todasMensagens)
                .logins(new ArrayList<>(historicoLogins));
        }
    }

    // ===== Operações dos clientes =====

    private Envelope login(Envelope mensagem) {
        String usuario = mensagem.user != null ? mensagem.user : "";

        if (!NOME_USUARIO.matcher(usuario).matches()) {
            return new Envelope()
                .type("login_reply")
                .status("error")
                .error("Nome de usuário inválido");
        }

        if (usuarios.contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("login_reply")
                .status("error")
                .error("Usuário já está registrado");
        }

        usuarios.add(usuario.toLowerCase());

        LoginRegistro registro = new LoginRegistro(usuario, horaAtual());

        historicoLogins.add(registro);

        persistencia.salvarLogins(historicoLogins);

        replicar(new Envelope()
            .type("replicate_login")
            .user(usuario)
            .time(registro.ts));

        System.out.println("Usuário conectado: " + usuario);

        return new Envelope()
            .type("login_reply")
            .status("ok")
            .user(usuario);
    }

    private Envelope criarCanal(Envelope mensagem) {
        String canal = mensagem.channel != null ? mensagem.channel : "";

        if (!NOME_CANAL.matcher(canal).matches()) {
            return new Envelope()
                .type("create_channel_reply")
                .status("error")
                .error("Nome de canal inválido");
        }

        if (canal.equals("servers") || canal.equals("replication")) {
            return new Envelope()
                .type("create_channel_reply")
                .status("error")
                .error("Nome de canal reservado");
        }

        if (canais.contains(canal)) {
            return new Envelope()
                .type("create_channel_reply")
                .status("error")
                .error("Canal já existe");
        }

        canais.add(canal);
        participantes.put(canal, new HashSet<>());
        mensagens.put(canal, new ArrayList<>());

        persistencia.salvarCanais(canais);
        persistencia.salvarMensagens(canal, mensagens.get(canal));

        replicar(new Envelope()
            .type("replicate_channel")
            .channel(canal));

        System.out.println("Canal criado: " + canal);

        return new Envelope()
            .type("create_channel_reply")
            .status("ok")
            .channel(canal);
    }

    private Envelope listarCanais() {
        List<String> lista = new ArrayList<>(canais);
        lista.sort(null);

        return new Envelope()
            .type("list_channels_reply")
            .status("ok")
            .channels(lista);
    }

    private Envelope entrarCanal(Envelope mensagem) {
        String usuario = mensagem.user != null ? mensagem.user : "";
        String canal = mensagem.channel != null ? mensagem.channel : "";

        if (!usuarios.contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("join_channel_reply")
                .status("error")
                .error("Usuário não está registrado");
        }

        if (!canais.contains(canal)) {
            return new Envelope()
                .type("join_channel_reply")
                .status("error")
                .error("Canal não encontrado");
        }

        if (participantes.get(canal).contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("join_channel_reply")
                .status("error")
                .error("Usuário já está no canal");
        }

        participantes.get(canal).add(usuario.toLowerCase());

        System.out.println("Usuário " + usuario + " entrou no canal " + canal);

        return new Envelope()
            .type("join_channel_reply")
            .status("ok")
            .user(usuario)
            .channel(canal);
    }

    private Envelope sairCanal(Envelope mensagem) {
        String usuario = mensagem.user != null ? mensagem.user : "";
        String canal = mensagem.channel != null ? mensagem.channel : "";

        if (!usuarios.contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("leave_channel_reply")
                .status("error")
                .error("Usuário não está registrado");
        }

        if (!canais.contains(canal)) {
            return new Envelope()
                .type("leave_channel_reply")
                .status("error")
                .error("Canal não encontrado");
        }

        if (!participantes.get(canal).contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("leave_channel_reply")
                .status("error")
                .error("Usuário não está no canal");
        }

        participantes.get(canal).remove(usuario.toLowerCase());

        System.out.println("Usuário " + usuario + " saiu do canal " + canal);

        return new Envelope()
            .type("leave_channel_reply")
            .status("ok")
            .user(usuario)
            .channel(canal);
    }

    private Envelope publicarMensagem(Envelope mensagem) {
        String usuario = mensagem.user != null ? mensagem.user : "";
        String canal = mensagem.channel != null ? mensagem.channel : "";
        String texto = mensagem.text != null ? mensagem.text : "";

        if (!usuarios.contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("post_reply")
                .status("error")
                .error("Usuário não está registrado");
        }

        if (!canais.contains(canal)) {
            return new Envelope()
                .type("post_reply")
                .status("error")
                .error("Canal não encontrado");
        }

        if (!participantes.get(canal).contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("post_reply")
                .status("error")
                .error("Usuário não participa do canal");
        }

        if (texto.isBlank()) {
            return new Envelope()
                .type("post_reply")
                .status("error")
                .error("Mensagem não pode estar vazia");
        }

        Envelope novaMensagem = new Envelope()
            .id(UUID.randomUUID().toString())
            .type("post")
            .user(usuario)
            .channel(canal)
            .text(texto)
            .from(usuario)
            .ts(horaAtual())
            .clock(incrementarRelogio());

        mensagens.get(canal).add(novaMensagem);
        ordenarMensagens(canal);
        persistencia.salvarMensagens(canal, mensagens.get(canal));
        LogMensagem.exibir("SERVIDOR", "ENVIANDO PUB", novaMensagem);

        synchronized (travaPublisher) {
            publisher.sendMore(canal);
            publisher.send(novaMensagem.serializar(), 0);
        }

        System.out.println("[" + canal + "] " + usuario + ": " + texto);

        replicar(new Envelope()
            .type("replicate_post")
            .channel(canal)
            .messages(new ArrayList<>(List.of(novaMensagem))));

        return new Envelope()
            .type("post_reply")
            .status("ok")
            .channel(canal);
    }

    private Envelope consultarMensagens(Envelope mensagem) {
        String usuario = mensagem.user != null ? mensagem.user : "";
        String canal = mensagem.channel != null ? mensagem.channel : "";

        if (!usuarios.contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("get_messages_reply")
                .status("error")
                .error("Usuário não está registrado");
        }

        if (!canais.contains(canal)) {
            return new Envelope()
                .type("get_messages_reply")
                .status("error")
                .error("Canal não encontrado");
        }

        if (!participantes.get(canal).contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("get_messages_reply")
                .status("error")
                .error("Usuário não participa do canal");
        }

        System.out.println("Usuário " + usuario + " consultou as mensagens do canal " + canal);

        return new Envelope()
            .type("get_messages_reply")
            .status("ok")
            .channel(canal)
            .messages(new ArrayList<>(mensagens.get(canal)));
    }

    private Envelope logout(Envelope mensagem) {
        String usuario = mensagem.user != null ? mensagem.user : "";

        if (!usuarios.contains(usuario.toLowerCase())) {
            return new Envelope()
                .type("logout_reply")
                .status("error")
                .error("Usuário não está registrado");
        }

        usuarios.remove(usuario.toLowerCase());

        for (Set<String> usuariosCanal : participantes.values()) {
            usuariosCanal.remove(usuario.toLowerCase());
        }

        System.out.println("Usuário desconectado: " + usuario);

        return new Envelope()
            .type("logout_reply")
            .status("ok")
            .user(usuario);
    }
}
