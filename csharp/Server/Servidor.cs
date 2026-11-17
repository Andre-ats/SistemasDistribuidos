
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

    public Servidor(int porta, int portaPub, string servidorId)
    {
        this.porta = porta;
        this.portaPub = portaPub;

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

            mensagens.Add(canal, new List<Envelope>());
        }

        Console.WriteLine($"Logins registrados: {historicoLogins.Count}");
        Console.WriteLine($"Canais carregados: {canais.Count}");

        using var socket = new ResponseSocket();

        socket.Bind($"tcp://*:{porta}");

        using var publisher = new PublisherSocket();

        publisher.Bind($"tcp://*:{portaPub}");

        Console.WriteLine($"Servidor iniciado na porta {porta}!");
        Console.WriteLine($"Publicações na porta {portaPub}!");

        while (true)
        {
            byte[] dados = socket.ReceiveFrameBytes();

            Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);

            relogioLogico = Math.Max(relogioLogico, mensagem.Clock) + 1;

            Console.WriteLine($"Relógio do servidor: {relogioLogico}");

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

            resposta.Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            relogioLogico++;
            resposta.Clock = relogioLogico;
            resposta.From = "Server";

            byte[] respostaBytes = MessagePackSerializer.Serialize(resposta);

            socket.SendFrame(respostaBytes);
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
            Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
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
            Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Clock = relogioLogico
        };

        mensagens[canal].Add(novaMensagem);

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
