
package referencia;

import common.Envelope;
import common.LogMensagem;
import common.ServidorInfo;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

public class ServicoReferencia {

    // Chaves em minúsculo: nomes de servidores ignoram maiúsculas/minúsculas.
    private final Map<String, ServidorInfo> servidores = new HashMap<>();
    private final Map<String, Long> ultimoHeartbeat = new HashMap<>();
    private final Set<String> disponiveis = new HashSet<>();
    private int proximoRank = 1;
    private long relogioLogico = 0;

    private final int porta;
    private final int tempoLimite;

    public ServicoReferencia(int porta, int tempoLimite) {
        this.porta = porta;
        this.tempoLimite = tempoLimite;
    }

    public void iniciar() {
        try (ZContext contexto = new ZContext()) {
            ZMQ.Socket socket = contexto.createSocket(SocketType.REP);

            socket.bind("tcp://*:" + porta);

            // Timeout curto permite verificar os heartbeats mesmo sem requisições.
            socket.setReceiveTimeOut(1000);

            System.out.println("Serviço de referência iniciado na porta " + porta + "!");
            System.out.println("Servidores sem heartbeat por " + tempoLimite + "s serão removidos.");

            while (true) {
                byte[] dados = socket.recv(0);

                if (dados != null) {
                    Envelope mensagem = Envelope.desserializar(dados);
                    LogMensagem.exibir("REFERENCIA", "RECEBENDO REQ", mensagem);

                    relogioLogico = Math.max(relogioLogico, mensagem.clock);

                    System.out.println("Relógio lógico da referência: " + relogioLogico);

                    Envelope resposta = switch (mensagem.type.toLowerCase()) {
                        case "rank" -> rank(mensagem);
                        case "list" -> listar();
                        case "heartbeat" -> heartbeat(mensagem);
                        default -> new Envelope()
                            .type("error")
                            .status("error")
                            .error("Tipo de mensagem inválido");
                    };

                    resposta.ts = System.currentTimeMillis();
                    relogioLogico++;
                    resposta.clock = relogioLogico;
                    resposta.from = "Referencia";

                    LogMensagem.exibir("REFERENCIA", "ENVIANDO REP", resposta);

                    socket.send(resposta.serializar(), 0);
                }

                verificarHeartbeats();
            }
        }
    }

    private Envelope rank(Envelope mensagem) {
        String nome = mensagem.server != null ? mensagem.server : "";

        if (nome.isBlank()) {
            return new Envelope()
                .type("rank_reply")
                .status("error")
                .error("Nome do servidor inválido");
        }

        String chave = nome.toLowerCase();

        // Sem repetição de nomes: um servidor que reinicia recebe o mesmo rank.
        ServidorInfo info = servidores.get(chave);

        if (info == null) {
            info = new ServidorInfo();
            info.name = nome;
            info.rank = proximoRank;

            proximoRank++;
            servidores.put(chave, info);

            System.out.println("Servidor cadastrado: " + nome + " (rank " + info.rank + ")");
        } else {
            System.out.println("Servidor já cadastrado: " + nome + " (rank " + info.rank + ")");
        }

        if (mensagem.address != null) {
            info.address = mensagem.address;
        }

        disponiveis.add(chave);
        ultimoHeartbeat.put(chave, System.currentTimeMillis());

        return new Envelope()
            .type("rank_reply")
            .status("ok")
            .server(nome)
            .rank(info.rank);
    }

    private Envelope listar() {
        List<ServidorInfo> lista = new ArrayList<>();

        for (Map.Entry<String, ServidorInfo> item : servidores.entrySet()) {
            if (disponiveis.contains(item.getKey())) {
                lista.add(item.getValue());
            }
        }

        lista.sort(Comparator.comparingInt(info -> info.rank));

        return new Envelope()
            .type("list_reply")
            .status("ok")
            .servers(lista);
    }

    private Envelope heartbeat(Envelope mensagem) {
        String nome = mensagem.server != null ? mensagem.server : "";
        String chave = nome.toLowerCase();

        if (!servidores.containsKey(chave)) {
            return new Envelope()
                .type("heartbeat_reply")
                .status("error")
                .error("Servidor não registrado");
        }

        if (disponiveis.add(chave)) {
            System.out.println("Servidor voltou para a lista: " + nome);
        }

        ultimoHeartbeat.put(chave, System.currentTimeMillis());

        System.out.println("Heartbeat recebido: " + nome);

        // A hora correta agora é fornecida pelo coordenador eleito (Berkeley).
        return new Envelope()
            .type("heartbeat_reply")
            .status("ok")
            .server(nome)
            .text("OK");
    }

    private void verificarHeartbeats() {
        long agora = System.currentTimeMillis();

        for (String chave : new ArrayList<>(disponiveis)) {
            if (agora - ultimoHeartbeat.get(chave) > tempoLimite * 1000L) {
                disponiveis.remove(chave);

                List<String> nomes = new ArrayList<>();

                for (String item : disponiveis) {
                    nomes.add(servidores.get(item).name);
                }

                System.out.println("Servidor removido por falta de heartbeat: " + servidores.get(chave).name);
                System.out.println("Servidores disponíveis: [" + String.join(", ", nomes) + "]");
            }
        }
    }
}
