
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;
using System.Text.RegularExpressions;

namespace Server;

public class Servidor
{
    private readonly HashSet<string> usuarios = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> canais = new();
    private readonly Dictionary<string, HashSet<string>> participantes = new();
    private readonly Dictionary<string, List<Envelope>> mensagens = new();
    private long relogioLogico = 0;

    private readonly Persistencia persistencia;
    private readonly List<LoginRegistro> historicoLogins = new();

    private readonly int porta;
    private readonly int portaPub;
    private readonly string servidorId;
    private readonly string proxyHost = Environment.GetEnvironmentVariable("PROXY_HOST") ?? "localhost";

    private readonly string brokerHost = Environment.GetEnvironmentVariable("BROKER_HOST") ?? "localhost";

    private readonly string referenciaHost = Environment.GetEnvironmentVariable("REFERENCIA_HOST") ?? "localhost";
    private readonly int referenciaPorta = int.Parse(Environment.GetEnvironmentVariable("REFERENCIA_PORT") ?? "5559");

    private int meuRank = 0;
    private int mensagensClientes = 0;

    // Diferença (ms) entre o relógio do servidor e o relógio do sistema
    private long deslocamentoRelogio = 0;

    public Servidor(int porta, int portaPub, string servidorId)
    {
        this.porta = porta;
        this.portaPub = portaPub;
        this.servidorId = servidorId;

        // Desvio inicial opcional, só para conseguir ver a sincronização funcionando
        deslocamentoRelogio = long.Parse(Environment.GetEnvironmentVariable("CLOCK_DRIFT_MS") ?? "0");

        persistencia = new Persistencia(servidorId);
    }
    
    public void Iniciar()
    {
        historicoLogins.AddRange(persistencia.CarregarLogins());

        foreach (string canal in persistencia.CarregarCanais())
        {
            canais.Add(canal);

            participantes.Add(
                canal,
                new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            );

            mensagens.Add(canal, persistencia.CarregarMensagens(canal));
        }

        Console.WriteLine($"Logins registrados: {historicoLogins.Count}");
        Console.WriteLine($"Canais carregados: {canais.Count}");
        Console.WriteLine($"Relógio do servidor: {DateTimeOffset.FromUnixTimeMilliseconds(Agora()):HH:mm:ss.fff} (desvio {deslocamentoRelogio} ms)");

        PedirRank();

        using var socket = new ResponseSocket();

        // Bind: pedidos dos outros servidores / Connect: pedidos dos clientes via broker
        socket.Bind($"tcp://*:{porta}");
        socket.Connect($"tcp://{brokerHost}:5556");

        using var publisher = new PublisherSocket();

        publisher.Connect($"tcp://{proxyHost}:5557");

        Console.WriteLine($"Servidor iniciado na porta {porta}!");
        Console.WriteLine($"Recebendo clientes pelo broker {brokerHost}:5556!");
        Console.WriteLine($"Publicações enviadas ao proxy {proxyHost}:5557!");

        while (true)
        {
            byte[] dados = socket.ReceiveFrameBytes();

            Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);
            LogMensagem.Exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            relogioLogico = Math.Max(relogioLogico, mensagem.Clock);

            Console.WriteLine($"Relógio lógico do servidor: {relogioLogico}");

            Console.WriteLine($"Mensagem recebida: {mensagem.Type}");

            Envelope resposta;

            switch (mensagem.Type.ToLowerInvariant())
            {
                case "login":
                    resposta = Login(mensagem);
                    break;

                case "create_channel":
                    resposta = CriarCanal(mensagem);
                    break;

                case "list_channels":
                    resposta = ListarCanais();
                    break;
                case "join_channel":
                    resposta = EntrarCanal(mensagem);
                    break;

                case "leave_channel":
                    resposta = SairCanal(mensagem);
                    break;
                case "post":
                    resposta = PublicarMensagem(mensagem, publisher);
                    break;
                case "get_messages":
                    resposta = ConsultarMensagens(mensagem);
                    break;
                case "logout":
                    resposta = Logout(mensagem);
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

            resposta.Ts = Agora();
            relogioLogico++;
            resposta.Clock = relogioLogico;
            resposta.From = "Server";

            LogMensagem.Exibir("SERVIDOR", "ENVIANDO REP", resposta);
            byte[] respostaBytes = MessagePackSerializer.Serialize(resposta);

            socket.SendFrame(respostaBytes);

            mensagensClientes++;

            if (mensagensClientes % 10 == 0)
            {
                Heartbeat();
            }
        }
    }

    private long Agora()
    {
        return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + deslocamentoRelogio;
    }

    private Envelope? EnviarRequisicao(string endereco, string tipo)
    {
        try
        {
            using var requisicaoSocket = new RequestSocket();
            requisicaoSocket.Options.Linger = TimeSpan.Zero;
            requisicaoSocket.Connect(endereco);

            relogioLogico++;

            Envelope requisicao = new Envelope
            {
                Type = tipo,
                Ts = Agora(),
                Clock = relogioLogico,
                From = servidorId,
                Rank = meuRank,
                Port = porta
            };

            LogMensagem.Exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

            requisicaoSocket.SendFrame(MessagePackSerializer.Serialize(requisicao));

            if (!requisicaoSocket.TryReceiveFrameBytes(TimeSpan.FromSeconds(3), out byte[]? respostaBytes))
            {
                Console.WriteLine($"Sem resposta de {endereco} para '{tipo}'");
                return null;
            }

            Envelope resposta = MessagePackSerializer.Deserialize<Envelope>(respostaBytes);

            LogMensagem.Exibir("SERVIDOR", "RECEBENDO REP", resposta);

            relogioLogico = Math.Max(relogioLogico, resposta.Clock);

            return resposta;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Erro ao enviar '{tipo}' para {endereco}: {ex.Message}");
            return null;
        }
    }

    private void PedirRank()
    {
        string endereco = $"tcp://{referenciaHost}:{referenciaPorta}";

        while (true)
        {
            Envelope? resposta = EnviarRequisicao(endereco, "rank");

            if (resposta?.Status == "ok" && resposta.Rank != null)
            {
                meuRank = resposta.Rank.Value;
                Console.WriteLine($"Rank recebido da referência: {meuRank}");
                return;
            }

            Console.WriteLine("Não foi possível obter o rank, tentando novamente...");
            Thread.Sleep(2000);
        }
    }

    private void Heartbeat()
    {
        long enviadoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Envelope? resposta = EnviarRequisicao($"tcp://{referenciaHost}:{referenciaPorta}", "heartbeat");

        long recebidoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (resposta == null)
        {
            Console.WriteLine("Heartbeat sem resposta da referência");
            return;
        }

        if (resposta.Status != "ok")
        {
            // Referência reiniciou e perdeu o cadastro
            Console.WriteLine($"Heartbeat recusado: {resposta.Error}. Pedindo rank de novo.");
            PedirRank();
            return;
        }

        if (resposta.Time != null)
        {
            // Hora correta + metade do tempo de ida e volta
            long horaCorreta = resposta.Time.Value + (recebidoEm - enviadoEm) / 2;
            long antes = Agora();

            deslocamentoRelogio = horaCorreta - recebidoEm;

            Console.WriteLine(
                $"Relógio atualizado pela referência: " +
                $"{DateTimeOffset.FromUnixTimeMilliseconds(antes):HH:mm:ss.fff} -> " +
                $"{DateTimeOffset.FromUnixTimeMilliseconds(Agora()):HH:mm:ss.fff} " +
                $"(ajuste de {Agora() - antes} ms)"
            );
        }
    }

    
    private Envelope Login(Envelope mensagem)
    {
        string usuario = mensagem.User ?? "";

        if (!Regex.IsMatch(usuario, "^[A-Za-z0-9_-]{3,32}$"))
        {
            return new Envelope
            {
                Type = "login_reply",
                Status = "error",
                Error = "Nome de usuário inválido"
            };
        }

        if (usuarios.Contains(usuario))
        {
            return new Envelope
            {
                Type = "login_reply",
                Status = "error",
                Error = "Usuário já está registrado"
            };
        }

        usuarios.Add(usuario);

        historicoLogins.Add(new LoginRegistro
        {
            User = usuario,
            Ts = Agora()
        });

        persistencia.SalvarLogins(historicoLogins);

        Console.WriteLine($"Usuário conectado: {usuario}");

        return new Envelope
        {
            Type = "login_reply",
            Status = "ok",
            User = usuario
        };
    }

    private Envelope CriarCanal(Envelope mensagem)
    {
        string canal = mensagem.Channel ?? "";

        if (!Regex.IsMatch(canal, "^[a-z0-9_-]{3,24}$"))
        {
            return new Envelope
            {
                Type = "create_channel_reply",
                Status = "error",
                Error = "Nome de canal inválido"
            };
        }

        if (canal == "servers" || canal == "replication")
        {
            return new Envelope
            {
                Type = "create_channel_reply",
                Status = "error",
                Error = "Nome de canal reservado"
            };
        }

        if (canais.Contains(canal))
        {
            return new Envelope
            {
                Type = "create_channel_reply",
                Status = "error",
                Error = "Canal já existe"
            };
        }

        canais.Add(canal);
        participantes.Add(canal, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        mensagens.Add(canal, new List<Envelope>());

        persistencia.SalvarCanais(canais);

        Console.WriteLine($"Canal criado: {canal}");

        return new Envelope
        {
            Type = "create_channel_reply",
            Status = "ok",
            Channel = canal
        };
    }

    private Envelope ListarCanais()
    {
        return new Envelope
        {
            Type = "list_channels_reply",
            Status = "ok",
            Channels = canais.OrderBy(canal => canal).ToList()
        };
    }

    private Envelope EntrarCanal(Envelope mensagem)
    {
        string usuario = mensagem.User ?? "";
        string canal = mensagem.Channel ?? "";

        if (!usuarios.Contains(usuario))
        {
            return new Envelope
            {
                Type = "join_channel_reply",
                Status = "error",
                Error = "Usuário não está registrado"
            };
        }

        if (!canais.Contains(canal))
        {
            return new Envelope
            {
                Type = "join_channel_reply",
                Status = "error",
                Error = "Canal não encontrado"
            };
        }

        if (participantes[canal].Contains(usuario))
        {
            return new Envelope
            {
                Type = "join_channel_reply",
                Status = "error",
                Error = "Usuário já está no canal"
            };
        }

        participantes[canal].Add(usuario);

        Console.WriteLine($"Usuário {usuario} entrou no canal {canal}");

        return new Envelope
        {
            Type = "join_channel_reply",
            Status = "ok",
            User = usuario,
            Channel = canal
        };
    }


    private Envelope SairCanal(Envelope mensagem)
    {
        string usuario = mensagem.User ?? "";
        string canal = mensagem.Channel ?? "";

        if (!usuarios.Contains(usuario))
        {
            return new Envelope
            {
                Type = "leave_channel_reply",
                Status = "error",
                Error = "Usuário não está registrado"
            };
        }

        if (!canais.Contains(canal))
        {
            return new Envelope
            {
                Type = "leave_channel_reply",
                Status = "error",
                Error = "Canal não encontrado"
            };
        }

        if (!participantes[canal].Contains(usuario))
        {
            return new Envelope
            {
                Type = "leave_channel_reply",
                Status = "error",
                Error = "Usuário não está no canal"
            };
        }

        participantes[canal].Remove(usuario);

        Console.WriteLine($"Usuário {usuario} saiu do canal {canal}");

        return new Envelope
        {
            Type = "leave_channel_reply",
            Status = "ok",
            User = usuario,
            Channel = canal
        };
    }


    private Envelope PublicarMensagem(Envelope mensagem, PublisherSocket publisher)
    {
        string usuario = mensagem.User ?? "";
        string canal = mensagem.Channel ?? "";
        string texto = mensagem.Text ?? "";

        if (!usuarios.Contains(usuario))
        {
            return new Envelope
            {
                Type = "post_reply",
                Status = "error",
                Error = "Usuário não está registrado"
            };
        }

        if (!canais.Contains(canal))
        {
            return new Envelope
            {
                Type = "post_reply",
                Status = "error",
                Error = "Canal não encontrado"
            };
        }

        if (!participantes[canal].Contains(usuario))
        {
            return new Envelope
            {
                Type = "post_reply",
                Status = "error",
                Error = "Usuário não participa do canal"
            };
        }

        if (string.IsNullOrWhiteSpace(texto))
        {
            return new Envelope
            {
                Type = "post_reply",
                Status = "error",
                Error = "Mensagem não pode estar vazia"
            };
        }

        relogioLogico++;
        Envelope novaMensagem = new Envelope
        {
            Id = Guid.NewGuid().ToString(),
            Type = "post",
            User = usuario,
            Channel = canal,
            Text = texto,
            From = usuario,
            Ts = Agora(),
            Clock = relogioLogico
        };

        mensagens[canal].Add(novaMensagem);
        persistencia.SalvarMensagens(canal, mensagens[canal]);
        LogMensagem.Exibir("SERVIDOR", "ENVIANDO PUB", novaMensagem);

        byte[] dadosMensagem = MessagePackSerializer.Serialize(novaMensagem);

        publisher
            .SendMoreFrame(canal)
            .SendFrame(dadosMensagem);

        Console.WriteLine($"[{canal}] {usuario}: {texto}");

        return new Envelope
        {
            Type = "post_reply",
            Status = "ok",
            Channel = canal
        };
    }


    private Envelope ConsultarMensagens(Envelope mensagem)
    {
        string usuario = mensagem.User ?? "";
        string canal = mensagem.Channel ?? "";

        if (!usuarios.Contains(usuario))
        {
            return new Envelope
            {
                Type = "get_messages_reply",
                Status = "error",
                Error = "Usuário não está registrado"
            };
        }

        if (!canais.Contains(canal))
        {
            return new Envelope
            {
                Type = "get_messages_reply",
                Status = "error",
                Error = "Canal não encontrado"
            };
        }

        if (!participantes[canal].Contains(usuario))
        {
            return new Envelope
            {
                Type = "get_messages_reply",
                Status = "error",
                Error = "Usuário não participa do canal"
            };
        }

        Console.WriteLine($"Usuário {usuario} consultou as mensagens do canal {canal}");

        return new Envelope
        {
            Type = "get_messages_reply",
            Status = "ok",
            Channel = canal,
            Messages = mensagens[canal].ToList()
        };
    }


    private Envelope Logout(Envelope mensagem)
    {
        string usuario = mensagem.User ?? "";

        if (!usuarios.Contains(usuario))
        {
            return new Envelope
            {
                Type = "logout_reply",
                Status = "error",
                Error = "Usuário não está registrado"
            };
        }

        usuarios.Remove(usuario);

        foreach (HashSet<string> usuariosCanal in participantes.Values)
        {
            usuariosCanal.Remove(usuario);
        }

        Console.WriteLine($"Usuário desconectado: {usuario}");

        return new Envelope
        {
            Type = "logout_reply",
            Status = "ok",
            User = usuario
        };
    }


}
