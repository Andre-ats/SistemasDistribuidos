
package cliente;

import common.Envelope;
import common.LogMensagem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class Program {

    private static final String servidorHost = variavel("SERVER_HOST", "localhost");

    private static final int portaServidor = Integer.parseInt(variavel("SERVER_PORT", "6000"));

    private static final String proxyHost = variavel("PROXY_HOST", "localhost");

    private static final ZContext contexto = new ZContext();

    private static final AtomicLong relogioLogico = new AtomicLong(0);

    private static final Set<String> canaisInscritos = ConcurrentHashMap.newKeySet();

    // No modo interativo, enquanto o usuário digita, as publicações recebidas ficam
    // guardadas e só são exibidas depois, para não misturar com o que está sendo digitado.
    private static final AtomicBoolean pausarMensagens = new AtomicBoolean(false);
    private static final ConcurrentLinkedQueue<String> mensagensPendentes = new ConcurrentLinkedQueue<>();

    private static String variavel(String nome, String padrao) {
        String valor = System.getenv(nome);
        return valor != null ? valor : padrao;
    }

    private static void esperar(long milissegundos) {
        try {
            Thread.sleep(milissegundos);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static long atualizarRelogio(long recebido) {
        return relogioLogico.accumulateAndGet(recebido, Math::max);
    }

    public static void main(String[] args) {
        Thread threadMensagens = new Thread(Program::receberMensagens);
        threadMensagens.setDaemon(true);
        threadMensagens.start();

        String modoCliente = variavel("CLIENT_MODE", "interactive");

        if (modoCliente.equalsIgnoreCase("auto")) {
            executarBot();
        } else {
            executarInterativo();
        }
    }

    private static void receberMensagens() {
        ZMQ.Socket subscriber = contexto.createSocket(SocketType.SUB);

        subscriber.connect("tcp://" + proxyHost + ":5558");

        // Timeout curto permite atualizar inscrições sem bloquear a thread.
        subscriber.setReceiveTimeOut(100);

        // O socket SUB pertence exclusivamente a esta thread.
        // As inscrições desejadas são compartilhadas pelo conjunto canaisInscritos.
        Set<String> topicosAtivos = new HashSet<>();

        while (true) {
            // Sincroniza inscrições reais do ZeroMQ com os canais do cliente.
            for (String topico : new ArrayList<>(topicosAtivos)) {
                if (!canaisInscritos.contains(topico)) {
                    subscriber.unsubscribe(topico.getBytes(ZMQ.CHARSET));
                    topicosAtivos.remove(topico);
                }
            }

            for (String topico : canaisInscritos) {
                if (topicosAtivos.add(topico)) {
                    subscriber.subscribe(topico.getBytes(ZMQ.CHARSET));
                }
            }

            String canal = subscriber.recvStr(0);

            if (canal == null) {
                continue;
            }

            byte[] dadosMensagem = subscriber.recv(0);

            if (dadosMensagem == null || !canaisInscritos.contains(canal)) {
                continue;
            }

            Envelope mensagemRecebida = Envelope.desserializar(dadosMensagem);
            long recebidoEm = System.currentTimeMillis();

            long novo = atualizarRelogio(mensagemRecebida.clock);

            StringBuilder texto = new StringBuilder();

            texto.append(LogMensagem.formatar("CLIENTE", "RECEBENDO PUB", mensagemRecebida)).append('\n');
            texto.append("Relógio lógico do Cliente: ").append(novo).append('\n');
            texto.append('\n');
            texto.append("[").append(canal).append("] ").append(mensagemRecebida.user).append(": ").append(mensagemRecebida.text).append('\n');
            texto.append("Enviada em: ").append(LogMensagem.formatarHora(mensagemRecebida.ts)).append('\n');
            texto.append("Recebida em: ").append(LogMensagem.formatarHora(recebidoEm)).append('\n');

            if (pausarMensagens.get()) {
                mensagensPendentes.add(texto.toString());
                continue;
            }

            System.out.print(texto);
            System.out.print("Escolha uma opção: ");
            System.out.flush();
        }
    }

    // ===== Modo automático (bots) =====

    private static String usuario;

    // O socket REQ é reaproveitado entre as requisições e só é recriado quando a
    // resposta não chega (padrão "Lazy Pirate" do ZeroMQ): um REQ que perdeu a
    // resposta fica travado esperando e não pode enviar outra mensagem.
    private static ZMQ.Socket requisicaoSocket;

    private static ZMQ.Socket socketRequisicao() {
        if (requisicaoSocket == null) {
            requisicaoSocket = contexto.createSocket(SocketType.REQ);
            requisicaoSocket.setLinger(0);
            requisicaoSocket.setReceiveTimeOut(3000);
            requisicaoSocket.connect("tcp://" + servidorHost + ":" + portaServidor);
        }

        return requisicaoSocket;
    }

    private static void descartarSocket() {
        if (requisicaoSocket != null) {
            contexto.destroySocket(requisicaoSocket);
            requisicaoSocket = null;
        }
    }

    private static Envelope enviarAutomatico(String tipo, String nomeCanal, String texto) {
        for (int tentativa = 1; tentativa <= 5; tentativa++) {
            try {
                ZMQ.Socket requisicaoSocket = socketRequisicao();

                Envelope requisicao = new Envelope()
                    .type(tipo)
                    .ts(System.currentTimeMillis())
                    .clock(relogioLogico.incrementAndGet())
                    .from("Cliente")
                    .user(usuario)
                    .channel(nomeCanal)
                    .text(texto);

                LogMensagem.exibir("CLIENTE", "ENVIANDO REQ", requisicao);

                requisicaoSocket.send(requisicao.serializar(), 0);

                byte[] respostaBytes = requisicaoSocket.recv(0);

                if (respostaBytes == null) {
                    System.out.println("[" + usuario + "] Sem resposta. Tentativa " + tentativa + "/5");

                    descartarSocket();
                    esperar(2000);
                    continue;
                }

                Envelope resposta = Envelope.desserializar(respostaBytes);

                LogMensagem.exibir("CLIENTE", "RECEBENDO REP", resposta);

                long novo = atualizarRelogio(resposta.clock);

                System.out.println("[" + usuario + "] Relógio lógico do Cliente: " + novo);

                return resposta;
            } catch (Exception ex) {
                System.out.println("[" + usuario + "] Erro: " + ex.getMessage());
                descartarSocket();
                esperar(2000);
            }
        }

        return null;
    }

    private static Envelope enviarAutomatico(String tipo) {
        return enviarAutomatico(tipo, null, null);
    }

    // O servidor persiste o histórico, mas as sessões e participantes são voláteis.
    // Após reinício, refazemos login e reinscrevemos o bot nos canais anteriores.
    private static boolean recuperarSessao() {
        System.out.println("[" + usuario + "] RECUPERACAO: servidor perdeu a sessão; refazendo login...");
        Envelope novoLogin = enviarAutomatico("login");
        if (novoLogin == null || !"ok".equals(novoLogin.status)) {
            String erro = novoLogin != null ? novoLogin.error : "sem resposta";
            System.out.println("[" + usuario + "] RECUPERACAO: login não realizado: " + erro);
            return false;
        }

        Envelope listagemAtual = enviarAutomatico("list_channels");
        if (listagemAtual == null || !"ok".equals(listagemAtual.status)) {
            System.out.println("[" + usuario + "] RECUPERACAO: falha ao consultar canais.");
            return false;
        }

        Set<String> existentes = new HashSet<>(listagemAtual.channels != null ? listagemAtual.channels : List.of());
        List<String> desejados = new ArrayList<>(canaisInscritos);

        canaisInscritos.clear();
        for (String nome : desejados) {
            if (!existentes.contains(nome)) continue;
            Envelope entrada = enviarAutomatico("join_channel", nome, null);
            if (entrada != null && ("ok".equals(entrada.status) || "Usuário já está no canal".equals(entrada.error))) {
                canaisInscritos.add(nome);
                System.out.println("[" + usuario + "] RECUPERACAO: inscrito novamente em '" + nome + "'");
            }
        }
        System.out.println("[" + usuario + "] RECUPERACAO CONCLUIDA: " + canaisInscritos.size() + " canais.");
        return true;
    }

    private static Envelope enviarComRecuperacao(String tipo, String nomeCanal, String texto) {
        Envelope resposta = enviarAutomatico(tipo, nomeCanal, texto);
        // Não repetir uma publicação que apenas perdeu a resposta: isso poderia duplicá-la.
        // Só repetimos quando o servidor confirmou que a sessão não existe.
        if (resposta != null && "error".equals(resposta.status) && "Usuário não está registrado".equals(resposta.error)) {
            if (recuperarSessao())
                return enviarAutomatico(tipo, nomeCanal, texto);
        }
        return resposta;
    }

    private static void executarBot() {
        usuario = variavel("CLIENT_USER", "cliente1");

        System.out.println("Modo automático iniciado: " + usuario);

        // Em caso de erro, o login é repetido até ser aceito.
        Envelope login = enviarAutomatico("login");

        while (login == null || !"ok".equals(login.status)) {
            String erro = login != null ? login.error : "Servidor indisponível";
            System.out.println("[" + usuario + "] Login falhou: " + erro);

            // O bot reiniciou, mas o servidor ainda guarda a sessão antiga: encerra e entra de novo.
            if (login != null && "Usuário já está registrado".equals(login.error)) {
                enviarAutomatico("logout");
            } else {
                esperar(2000);
            }

            System.out.println("[" + usuario + "] Repetindo login...");
            login = enviarAutomatico("login");
        }

        System.out.println("[" + usuario + "] Login realizado!");

        Random aleatorio = new Random();
        String[] textos = { "Bom dia!", "Tudo certo?", "Alguém por aqui?", "Mensagem de teste", "Olá pessoal!", "Mais uma mensagem", "Teste Pub/Sub" };

        while (true) {
            Envelope listagem = enviarComRecuperacao("list_channels", null, null);
            if (listagem == null || !"ok".equals(listagem.status)) {
                System.out.println("[" + usuario + "] Não foi possível listar canais.");
                esperar(2000);
                continue;
            }

            List<String> disponiveis = new ArrayList<>(listagem.channels != null ? listagem.channels : List.of());
            if (disponiveis.size() < 5) {
                String novoCanal = "canal_" + usuario + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 6);
                Envelope criacao = enviarComRecuperacao("create_channel", novoCanal, null);
                if (criacao != null && "ok".equals(criacao.status)) {
                    disponiveis.add(novoCanal);
                    System.out.println("[" + usuario + "] Canal '" + novoCanal + "' criado!");
                }
            }

            if (canaisInscritos.size() < 3) {
                List<String> candidatos = new ArrayList<>();
                for (String canal : disponiveis) {
                    if (!canaisInscritos.contains(canal)) candidatos.add(canal);
                }
                if (!candidatos.isEmpty()) {
                    String escolhido = candidatos.get(aleatorio.nextInt(candidatos.size()));
                    Envelope entrada = enviarComRecuperacao("join_channel", escolhido, null);
                    if (entrada != null && "ok".equals(entrada.status)) {
                        canaisInscritos.add(escolhido);
                        System.out.println("[" + usuario + "] Inscrito no canal '" + escolhido + "'");
                    }
                }
            }

            List<String> inscritos = new ArrayList<>(canaisInscritos);
            if (inscritos.isEmpty()) {
                esperar(1000);
                continue;
            }

            String canalEscolhido = inscritos.get(aleatorio.nextInt(inscritos.size()));
            for (int i = 0; i < 10; i++) {
                String texto = textos[aleatorio.nextInt(textos.length)];
                Envelope resposta = enviarComRecuperacao("post", canalEscolhido, texto);
                String status = resposta != null ? resposta.status : "sem resposta";
                System.out.println("[" + usuario + "] Publicação em " + canalEscolhido + ": " + status);
                esperar(1000);
            }
        }
    }

    // ===== Modo interativo =====

    private static final BufferedReader entrada =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

    private static String lerLinha(String rotulo) {
        if (rotulo != null) {
            System.out.print(rotulo);
            System.out.flush();
        }

        try {
            return entrada.readLine();
        } catch (IOException ex) {
            return null;
        }
    }

    private static void executarInterativo() {
        ZMQ.Socket socket = contexto.createSocket(SocketType.REQ);

        socket.connect("tcp://" + servidorHost + ":" + portaServidor);

        System.out.println("Conectado ao servidor na porta " + portaServidor);

        String usuarioLogado = null;

        while (true) {
            // Libera a exibição e mostra o que chegou enquanto o usuário digitava.
            pausarMensagens.set(false);

            if (!mensagensPendentes.isEmpty()) {
                System.out.println();
                System.out.println("===== Mensagens recebidas enquanto você digitava: " + mensagensPendentes.size() + " =====");

                String pendente;

                while ((pendente = mensagensPendentes.poll()) != null) {
                    System.out.print(pendente);
                }
            }

            System.out.println();
            System.out.println("===== BBS =====");

            if (usuarioLogado != null) {
                System.out.println("Usuário logado: " + usuarioLogado);
            }

            System.out.println("1 - Login");
            System.out.println("2 - Criar canal");
            System.out.println("3 - Listar canais");
            System.out.println("4 - Entrar em um canal");
            System.out.println("5 - Sair de um canal");
            System.out.println("6 - Enviar mensagem");
            System.out.println("7 - Consultar mensagens");
            System.out.println("8 - Logout");
            System.out.println("0 - Encerrar");

            String opcao = lerLinha("Escolha uma opção: ");

            // A partir daqui o usuário vai digitar: segura as publicações recebidas.
            pausarMensagens.set(true);

            if (opcao == null) {
                System.out.println("Entrada encerrada. Finalizando cliente.");
                break;
            }

            opcao = opcao.trim();

            if (opcao.equals("0")) {
                if (usuarioLogado != null) {
                    Envelope logout = new Envelope()
                        .type("logout")
                        .user(usuarioLogado)
                        .from("Cliente")
                        .ts(System.currentTimeMillis())
                        .clock(relogioLogico.incrementAndGet());

                    LogMensagem.exibir("CLIENTE", "ENVIANDO REQ", logout);
                    socket.send(logout.serializar(), 0);

                    Envelope resultado = Envelope.desserializar(socket.recv(0));

                    LogMensagem.exibir("CLIENTE", "RECEBENDO REP", resultado);

                    if ("ok".equals(resultado.status)) {
                        System.out.println("Logout realizado com sucesso.");
                    } else {
                        System.out.println("Erro ao realizar logout: " + resultado.error);
                    }
                }

                System.out.println("Cliente encerrado.");
                break;
            }

            Envelope mensagem = new Envelope()
                .from("Cliente")
                .user(usuarioLogado);

            switch (opcao) {
                case "1" -> {
                    if (usuarioLogado != null) {
                        System.out.println("Você já está logado.");
                        continue;
                    }

                    mensagem.type = "login";
                    mensagem.user = lerLinha("Nome do usuário: ");
                }

                case "2" -> {
                    mensagem.type = "create_channel";
                    mensagem.channel = lerLinha("Nome do canal: ");
                }

                case "3" -> mensagem.type = "list_channels";

                case "4" -> {
                    mensagem.type = "join_channel";
                    mensagem.channel = lerLinha("Nome do canal: ");
                }

                case "5" -> {
                    mensagem.type = "leave_channel";
                    mensagem.channel = lerLinha("Nome do canal: ");
                }

                case "6" -> {
                    mensagem.channel = lerLinha("Nome do canal: ");
                    mensagem.text = lerLinha("Mensagem: ");
                    mensagem.type = "post";
                }

                case "7" -> {
                    mensagem.type = "get_messages";
                    mensagem.channel = lerLinha("Nome do canal: ");
                }

                case "8" -> mensagem.type = "logout";

                default -> {
                    System.out.println("Opção inválida.");
                    continue;
                }
            }

            if (usuarioLogado == null && !opcao.equals("1") && !opcao.equals("3")) {
                System.out.println("Você precisa fazer login primeiro.");
                continue;
            }

            // Timestamp do envio (e não de quando o menu montou a mensagem).
            mensagem.ts = System.currentTimeMillis();
            mensagem.clock = relogioLogico.incrementAndGet();

            LogMensagem.exibir("CLIENTE", "ENVIANDO REQ", mensagem);
            socket.send(mensagem.serializar(), 0);

            Envelope resposta = Envelope.desserializar(socket.recv(0));
            LogMensagem.exibir("CLIENTE", "RECEBENDO REP", resposta);

            long novo = atualizarRelogio(resposta.clock);

            System.out.println("Relógio lógico do Cliente: " + novo);

            System.out.println("Resposta do servidor: " + resposta.status);

            if ("error".equals(resposta.status)) {
                System.out.println("Erro: " + resposta.error);
            }

            if (opcao.equals("1") && "ok".equals(resposta.status)) {
                usuarioLogado = resposta.user;
                System.out.println("Login realizado: " + usuarioLogado);
            }

            if (opcao.equals("4") && "ok".equals(resposta.status) && mensagem.channel != null) {
                canaisInscritos.add(mensagem.channel);
            }

            if (opcao.equals("5") && "ok".equals(resposta.status) && mensagem.channel != null) {
                canaisInscritos.remove(mensagem.channel);
            }

            if (opcao.equals("8") && "ok".equals(resposta.status)) {
                usuarioLogado = null;
                canaisInscritos.clear();
                System.out.println("Logout realizado com sucesso.");
            }

            if (resposta.channels != null) {
                System.out.println("Canais cadastrados:");

                for (String canal : resposta.channels) {
                    System.out.println("- " + canal);
                }
            }

            if (resposta.messages != null) {
                System.out.println("Mensagens do canal:");

                if (resposta.messages.isEmpty()) {
                    System.out.println("Nenhuma mensagem encontrada.");
                }

                for (Envelope msg : resposta.messages) {
                    System.out.println("ID: " + msg.id);
                    System.out.println("[" + msg.user + "] " + msg.text);
                }
            }
        }
    }
}
