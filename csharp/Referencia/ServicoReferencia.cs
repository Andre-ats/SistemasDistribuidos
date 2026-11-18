
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

namespace Referencia;

public class ServicoReferencia
{
    private readonly Dictionary<string, ServidorInfo> servidores = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, long> ultimoHeartbeat = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> disponiveis = new(StringComparer.OrdinalIgnoreCase);
    private int proximoRank = 1;
    private long relogioLogico = 0;

    private readonly int porta;
    private readonly int tempoLimite;

    public ServicoReferencia(int porta, int tempoLimite)
    {
        this.porta = porta;
        this.tempoLimite = tempoLimite;
    }

    public void Iniciar()
    {
        using var socket = new ResponseSocket();

        socket.Bind($"tcp://*:{porta}");

        Console.WriteLine($"Serviço de referência iniciado na porta {porta}!");
        Console.WriteLine($"Servidores sem heartbeat por {tempoLimite}s serão removidos.");

        while (true)
        {
            // Timeout curto permite verificar os heartbeats mesmo sem requisições.
            if (socket.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out byte[]? dados))
            {
                Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);
                LogMensagem.Exibir("REFERENCIA", "RECEBENDO REQ", mensagem);

                relogioLogico = Math.Max(relogioLogico, mensagem.Clock);

                Console.WriteLine($"Relógio lógico da referência: {relogioLogico}");

                Envelope resposta;

                switch (mensagem.Type.ToLowerInvariant())
                {
                    case "rank":
                        resposta = Rank(mensagem);
                        break;

                    case "list":
                        resposta = Listar();
                        break;

                    case "heartbeat":
                        resposta = Heartbeat(mensagem);
                        break;

                    default:
                        resposta = new Envelope
                        {
                            Type = "error",
                            Status = "error",
                            Error = "Tipo de mensagem inválido"
                        };
                        break;
                }

                resposta.Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                relogioLogico++;
                resposta.Clock = relogioLogico;
                resposta.From = "Referencia";

                LogMensagem.Exibir("REFERENCIA", "ENVIANDO REP", resposta);
                byte[] respostaBytes = MessagePackSerializer.Serialize(resposta);

                socket.SendFrame(respostaBytes);
            }

            VerificarHeartbeats();
        }
    }

    private Envelope Rank(Envelope mensagem)
    {
        string nome = mensagem.Server ?? "";

        if (string.IsNullOrWhiteSpace(nome))
        {
            return new Envelope
            {
                Type = "rank_reply",
                Status = "error",
                Error = "Nome do servidor inválido"
            };
        }

        // Sem repetição de nomes: um servidor que reinicia recebe o mesmo rank.
        if (!servidores.TryGetValue(nome, out ServidorInfo? info))
        {
            info = new ServidorInfo
            {
                Name = nome,
                Rank = proximoRank
            };

            proximoRank++;
            servidores.Add(nome, info);

            Console.WriteLine($"Servidor cadastrado: {nome} (rank {info.Rank})");
        }
        else
        {
            Console.WriteLine($"Servidor já cadastrado: {nome} (rank {info.Rank})");
        }

        info.Address = mensagem.Address ?? info.Address;

        disponiveis.Add(nome);
        ultimoHeartbeat[nome] = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        return new Envelope
        {
            Type = "rank_reply",
            Status = "ok",
            Server = nome,
            Rank = info.Rank
        };
    }

    private Envelope Listar()
    {
        return new Envelope
        {
            Type = "list_reply",
            Status = "ok",
            Servers = servidores.Values
                .Where(info => disponiveis.Contains(info.Name))
                .OrderBy(info => info.Rank)
                .ToList()
        };
    }

    private Envelope Heartbeat(Envelope mensagem)
    {
        string nome = mensagem.Server ?? "";

        if (!servidores.ContainsKey(nome))
        {
            return new Envelope
            {
                Type = "heartbeat_reply",
                Status = "error",
                Error = "Servidor não registrado"
            };
        }

        if (disponiveis.Add(nome))
        {
            Console.WriteLine($"Servidor voltou para a lista: {nome}");
        }

        long agora = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        ultimoHeartbeat[nome] = agora;

        Console.WriteLine($"Heartbeat recebido: {nome}");

        // A hora correta agora é fornecida pelo coordenador eleito (Berkeley).
        return new Envelope
        {
            Type = "heartbeat_reply",
            Status = "ok",
            Server = nome,
            Text = "OK"
        };
    }

    private void VerificarHeartbeats()
    {
        long agora = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        foreach (string nome in disponiveis.ToList())
        {
            if (agora - ultimoHeartbeat[nome] > tempoLimite * 1000L)
            {
                disponiveis.Remove(nome);

                Console.WriteLine($"Servidor removido por falta de heartbeat: {nome}");
                Console.WriteLine($"Servidores disponíveis: [{string.Join(", ", disponiveis)}]");
            }
        }
    }
}
