
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

    // Nome do servidor eleito como coordenador (null = ainda não sabe)
    private string? coordenador = null;
    private bool precisaEleicao = false;
    private DateTimeOffset ultimaEleicao = DateTimeOffset.MinValue;

    // Usado pelo coordenador no Berkeley: nome -> diferença (ms) do relógio do servidor
    private readonly Dictionary<string, long> diferencasRelogios = new();

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

        using var subscriber = new SubscriberSocket();

        subscriber.Connect($"tcp://{proxyHost}:5558");
        subscriber.Subscribe("servers");

        Console.WriteLine($"Servidor iniciado na porta {porta}!");
        Console.WriteLine($"Recebendo clientes pelo broker {brokerHost}:5556!");
        Console.WriteLine($"Publicações enviadas ao proxy {proxyHost}:5557!");

        while (true)
        {
            ReceberPublicacoesServidores(subscriber);

            if (precisaEleicao)
            {
                precisaEleicao = false;
                Eleicao(publisher);
            }

            if (!socket.TryReceiveFrameBytes(TimeSpan.FromMilliseconds(200), out byte[]? dados))
            {
                continue;
            }

            Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);
            LogMensagem.Exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            relogioLogico = Math.Max(relogioLogico, mensagem.Clock);

            Console.WriteLine($"Relógio lógico do servidor: {relogioLogico}");

            Console.WriteLine($"Mensagem recebida: {mensagem.Type}");

            Envelope resposta;
            string tipo = mensagem.Type.ToLowerInvariant();

            switch (tipo)
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
                case "election":
                    resposta = ResponderEleicao(mensagem);
                    break;
                case "clock":
                    resposta = ResponderRelogio(mensagem);
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

            // Mensagens de outros servidores não contam para heartbeat/relógio
            if (tipo == "election" || tipo == "clock")
            {
                continue;
            }

            mensagensClientes++;

            if (mensagensClientes % 10 == 0)
            {
                Heartbeat();
            }

            if (mensagensClientes % 15 == 0)
            {
                SincronizarRelogio(publisher);
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
        Envelope? resposta = EnviarRequisicao($"tcp://{referenciaHost}:{referenciaPorta}", "heartbeat");

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
        }
    }

    private List<ServidorInfo> ListarServidores()
    {
        Envelope? resposta = EnviarRequisicao($"tcp://{referenciaHost}:{referenciaPorta}", "list");

        if (resposta?.Status != "ok" || resposta.Servers == null)
        {
            Console.WriteLine("Não foi possível obter a lista de servidores");
            return new List<ServidorInfo>();
        }

        return resposta.Servers;
    }

    private void ReceberPublicacoesServidores(SubscriberSocket subscriber)
    {
        while (subscriber.TryReceiveFrameString(TimeSpan.Zero, out string? topico))
        {
            byte[] dados = subscriber.ReceiveFrameBytes();

            // Subscribe é por prefixo, então ignora canais tipo "servers_abc"
            if (topico != "servers")
            {
                continue;
            }

            Envelope anuncio = MessagePackSerializer.Deserialize<Envelope>(dados);
            LogMensagem.Exibir("SERVIDOR", "RECEBENDO PUB", anuncio);

            relogioLogico = Math.Max(relogioLogico, anuncio.Clock);

            coordenador = anuncio.Coordinator;
            diferencasRelogios.Clear();

            Console.WriteLine($"Novo coordenador: {coordenador}");
        }
    }

    private void Eleicao(PublisherSocket publisher)
    {
        Console.WriteLine($"ELEICAO iniciada por {servidorId} (rank {meuRank})");

        List<ServidorInfo> lista = ListarServidores();

        if (!lista.Any(servidor => servidor.Name == servidorId))
        {
            // Fora da lista da referência (sem heartbeat), não participa
            Console.WriteLine("ELEICAO: este servidor não está na lista de disponíveis, não participa");
            return;
        }

        bool alguemRespondeu = false;

        foreach (ServidorInfo servidor in lista.Where(servidor => servidor.Rank > meuRank))
        {
            Envelope? resposta = EnviarRequisicao($"tcp://{servidor.Name}:{servidor.Port}", "election");

            if (resposta?.Status == "ok")
            {
                alguemRespondeu = true;

                // Se quem respondeu já conhece o coordenador, já usa
                if (resposta.Coordinator != null)
                {
                    coordenador = resposta.Coordinator;
                }
            }
        }

        ultimaEleicao = DateTimeOffset.UtcNow;

        if (alguemRespondeu)
        {
            Console.WriteLine("ELEICAO: servidor com rank maior respondeu, aguardando anúncio do coordenador");
            return;
        }

        coordenador = servidorId;
        diferencasRelogios.Clear();

        relogioLogico++;

        Envelope anuncio = new Envelope
        {
            Type = "coordinator",
            From = servidorId,
            Coordinator = servidorId,
            Rank = meuRank,
            Ts = Agora(),
            Clock = relogioLogico
        };

        LogMensagem.Exibir("SERVIDOR", "ENVIANDO PUB", anuncio);

        publisher
            .SendMoreFrame("servers")
            .SendFrame(MessagePackSerializer.Serialize(anuncio));

        Console.WriteLine($"ELEICAO: {servidorId} é o novo coordenador");
    }

    private Envelope ResponderEleicao(Envelope mensagem)
    {
        Console.WriteLine($"Pedido de eleição recebido de {mensagem.From} (rank {mensagem.Rank})");

        // Já é o coordenador ou acabou de fazer eleição: só responde quem é o coordenador
        if (coordenador == servidorId || (DateTimeOffset.UtcNow - ultimaEleicao).TotalSeconds < 5)
        {
            return new Envelope
            {
                Type = "election_reply",
                Status = "ok",
                Coordinator = coordenador
            };
        }

        // Responde OK e começa a própria eleição depois de enviar a resposta
        precisaEleicao = true;

        return new Envelope
        {
            Type = "election_reply",
            Status = "ok"
        };
    }

    private Envelope ResponderRelogio(Envelope mensagem)
    {
        if (coordenador != servidorId)
        {
            return new Envelope
            {
                Type = "clock_reply",
                Status = "error",
                Error = "Este servidor não é o coordenador",
                Coordinator = coordenador
            };
        }

        // Berkeley: guarda a diferença de cada servidor e usa a média como hora correta
        diferencasRelogios[mensagem.From] = mensagem.Ts - Agora();

        long media = diferencasRelogios.Values.Sum() / (diferencasRelogios.Count + 1);

        deslocamentoRelogio += media;

        foreach (string nome in diferencasRelogios.Keys.ToList())
        {
            diferencasRelogios[nome] -= media;
        }

        // Quem pediu vai usar a hora correta, então a diferença dele passa a ser 0
        diferencasRelogios[mensagem.From] = 0;

        Console.WriteLine(
            $"BERKELEY: {mensagem.From} pediu o relógio, média das diferenças = {media} ms " +
            $"({diferencasRelogios.Count + 1} relógios)"
        );

        return new Envelope
        {
            Type = "clock_reply",
            Status = "ok",
            Coordinator = servidorId,
            Time = Agora()
        };
    }

    private void SincronizarRelogio(PublisherSocket publisher)
    {
        if (coordenador == null)
        {
            Console.WriteLine("Nenhum coordenador conhecido, iniciando eleição");
            Eleicao(publisher);
        }

        if (coordenador == null)
        {
            Console.WriteLine("Ainda sem coordenador, sincronização fica para a próxima");
            return;
        }

        if (coordenador == servidorId)
        {
            Console.WriteLine($"Este servidor é o coordenador. Hora: {DateTimeOffset.FromUnixTimeMilliseconds(Agora()):HH:mm:ss.fff}");
            return;
        }

        ServidorInfo? infoCoordenador = ListarServidores().FirstOrDefault(servidor => servidor.Name == coordenador);

        long enviadoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Envelope? resposta = infoCoordenador == null
            ? null
            : EnviarRequisicao($"tcp://{infoCoordenador.Name}:{infoCoordenador.Port}", "clock");

        long recebidoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (resposta?.Status != "ok" || resposta.Time == null)
        {
            Console.WriteLine($"Coordenador {coordenador} indisponível, pedindo eleição");
            coordenador = null;
            Eleicao(publisher);
            return;
        }

        long horaCorreta = resposta.Time.Value + (recebidoEm - enviadoEm) / 2;
        long antes = Agora();

        deslocamentoRelogio = horaCorreta - recebidoEm;

        Console.WriteLine(
            $"Relógio sincronizado com o coordenador {coordenador}: " +
            $"{DateTimeOffset.FromUnixTimeMilliseconds(antes):HH:mm:ss.fff} -> " +
            $"{DateTimeOffset.FromUnixTimeMilliseconds(Agora()):HH:mm:ss.fff} " +
            $"(ajuste de {Agora() - antes} ms)"
        );
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
