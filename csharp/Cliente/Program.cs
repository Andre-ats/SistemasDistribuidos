
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

using System.Collections.Concurrent;
using System.Threading;

string servidorHost = Environment.GetEnvironmentVariable("SERVER_HOST") ?? "localhost";

int portaServidor = int.Parse(
    Environment.GetEnvironmentVariable("SERVER_PORT") ?? "6000"
);

int portaPub = int.Parse(
    Environment.GetEnvironmentVariable("PUB_PORT") ?? "6001"
);

using var socket = new RequestSocket();

socket.Connect($"tcp://{servidorHost}:{portaServidor}");

Console.WriteLine($"Conectado ao servidor na porta {portaServidor}");

string? usuarioLogado = null;
long relogioLogico = 0;


ConcurrentDictionary<string, byte> canaisInscritos = new();

Thread threadMensagens = new(() =>
{
    using var subscriber = new SubscriberSocket();

    subscriber.Connect($"tcp://{servidorHost}:{portaPub}");
    subscriber.Subscribe("");

    while (true)
    {
        string canal = subscriber.ReceiveFrameString();
        byte[] dadosMensagem = subscriber.ReceiveFrameBytes();

        if (!canaisInscritos.ContainsKey(canal))
        {
            continue;
        }

        Envelope mensagemRecebida =
            MessagePackSerializer.Deserialize<Envelope>(dadosMensagem);

        long atual;
        long novo;

        do
        {
            atual = Volatile.Read(ref relogioLogico);
            novo = Math.Max(atual, mensagemRecebida.Clock) + 1;
        }
        while (Interlocked.CompareExchange(ref relogioLogico, novo, atual) != atual);

        Console.WriteLine();
        Console.WriteLine($"[{canal}] {mensagemRecebida.User}: {mensagemRecebida.Text}");
        Console.Write("Escolha uma opção: ");
    }
});

threadMensagens.IsBackground = true;
threadMensagens.Start();

string modoCliente =
    Environment.GetEnvironmentVariable("CLIENT_MODE") ?? "interactive";

if (modoCliente.Equals("auto", StringComparison.OrdinalIgnoreCase))
{
    string usuario =
        Environment.GetEnvironmentVariable("CLIENT_USER") ?? "cliente1";

    string canal =
        Environment.GetEnvironmentVariable("CLIENT_CHANNEL") ?? "canal_cliente1";

    Console.WriteLine($"Modo automático iniciado: {usuario}");

    Envelope? EnviarAutomatico(string tipo, string? nomeCanal = null)
    {
        for (int tentativa = 1; tentativa <= 5; tentativa++)
        {
            try
            {
                using var requisicaoSocket = new RequestSocket();
                requisicaoSocket.Options.Linger = TimeSpan.Zero;
                requisicaoSocket.Connect($"tcp://{servidorHost}:{portaServidor}");

                var requisicao = new Envelope
                {
                    Type = tipo,
                    Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    Clock = Interlocked.Increment(ref relogioLogico),
                    From = "Cliente",
                    User = usuario,
                    Channel = nomeCanal
                };

                LogMensagem.Exibir("CLIENTE", "ENVIANDO REQ", requisicao);

                requisicaoSocket.SendFrame(
                    MessagePackSerializer.Serialize(requisicao)
                );

                if (!requisicaoSocket.TryReceiveFrameBytes(
                    TimeSpan.FromSeconds(3),
                    out byte[]? respostaBytes))
                {
                    Console.WriteLine(
                        $"[{usuario}] Sem resposta. Tentativa {tentativa}/5"
                    );

                    Thread.Sleep(2000);
                    continue;
                }

                Envelope resposta =
                    MessagePackSerializer.Deserialize<Envelope>(respostaBytes);

                LogMensagem.Exibir("CLIENTE", "RECEBENDO REP", resposta);

                long atual;
                long novo;

                do
                {
                    atual = Volatile.Read(ref relogioLogico);
                    novo = Math.Max(atual, resposta.Clock) + 1;
                }
                while (Interlocked.CompareExchange(
                    ref relogioLogico, novo, atual) != atual);

                return resposta;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{usuario}] Erro: {ex.Message}");
                Thread.Sleep(2000);
            }
        }

        return null;
    }

    Envelope? login = EnviarAutomatico("login");

    if (login?.Status != "ok")
    {
        Console.WriteLine(
            $"[{usuario}] Login falhou: {login?.Error ?? "Servidor indisponível"}"
        );

        Environment.ExitCode = 1;
        return;
    }

    Console.WriteLine($"[{usuario}] Login realizado!");

    Envelope? listagem = EnviarAutomatico("list_channels");

    if (listagem?.Status != "ok")
    {
        Console.WriteLine($"[{usuario}] Falha ao listar canais.");
        Environment.ExitCode = 1;
        return;
    }

    Console.WriteLine($"[{usuario}] Canais disponíveis:");

    foreach (string nome in listagem.Channels ?? new List<string>())
    {
        Console.WriteLine($" - {nome}");
    }

    if (listagem.Channels?.Contains(canal) == true)
    {
        Console.WriteLine($"[{usuario}] Canal '{canal}' já existe.");
    }
    else
    {
        Envelope? criacao = EnviarAutomatico("create_channel", canal);

        if (criacao?.Status == "ok")
        {
            Console.WriteLine($"[{usuario}] Canal '{canal}' criado!");
        }
        else
        {
            Console.WriteLine(
                $"[{usuario}] Falha ao criar canal: {criacao?.Error}"
            );

            Environment.ExitCode = 1;
            return;
        }
    }

    Envelope? logout = EnviarAutomatico("logout");

    if (logout?.Status == "ok")
    {
        Console.WriteLine($"[{usuario}] Logout realizado!");
    }
    else
    {
        Console.WriteLine($"[{usuario}] Falha no logout.");
        Environment.ExitCode = 1;
        return;
    }

    Console.WriteLine($"[{usuario}] Operações automáticas concluídas.");
    return;
}


while (true)
{
    Console.WriteLine();
    Console.WriteLine("===== BBS =====");

    if (usuarioLogado != null)
    {
        Console.WriteLine($"Usuário logado: {usuarioLogado}");
    }

    Console.WriteLine("1 - Login");
    Console.WriteLine("2 - Criar canal");
    Console.WriteLine("3 - Listar canais");
    Console.WriteLine("4 - Entrar em um canal");
    Console.WriteLine("5 - Sair de um canal");
    Console.WriteLine("6 - Enviar mensagem");
    Console.WriteLine("7 - Consultar mensagens");
    Console.WriteLine("8 - Logout");
    Console.WriteLine("0 - Encerrar");

    Console.Write("Escolha uma opção: ");
    string? opcao = Console.ReadLine();

    if (opcao == null)
    {
        Console.WriteLine("Entrada encerrada. Finalizando cliente.");
        break;
    }

    
    if (opcao == "0")
    {
        if (usuarioLogado != null)
        {
            Envelope logout = new Envelope
            {
                Type = "logout",
                User = usuarioLogado,
                From = "Cliente",
                Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Clock = Interlocked.Increment(ref relogioLogico)
            };

            LogMensagem.Exibir("CLIENTE", "ENVIANDO REQ", logout);
            socket.SendFrame(MessagePackSerializer.Serialize(logout));

            byte[] respostaLogout = socket.ReceiveFrameBytes();
            Envelope resultado = MessagePackSerializer.Deserialize<Envelope>(respostaLogout);

            LogMensagem.Exibir("CLIENTE", "RECEBENDO REP", resultado);

            if (resultado.Status == "ok")
            {
                Console.WriteLine("Logout realizado com sucesso.");
            }
            else
            {
                Console.WriteLine($"Erro ao realizar logout: {resultado.Error}");
            }
        }

        Console.WriteLine("Cliente encerrado.");
        break;
    }

    Envelope mensagem = new Envelope
    {
        Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Clock = 0,
        From = "Cliente",
        User = usuarioLogado
    };

    switch (opcao)
    {
        case "1":
            if (usuarioLogado != null)
            {
                Console.WriteLine("Você já está logado.");
                continue;
            }

            Console.Write("Nome do usuário: ");
            mensagem.Type = "login";
            mensagem.User = Console.ReadLine();
            break;

        case "2":
            Console.Write("Nome do canal: ");
            mensagem.Type = "create_channel";
            mensagem.Channel = Console.ReadLine();
            break;

        case "3":
            mensagem.Type = "list_channels";
            break;

        case "4":
            Console.Write("Nome do canal: ");
            mensagem.Type = "join_channel";
            mensagem.Channel = Console.ReadLine();
            break;

        case "5":
            Console.Write("Nome do canal: ");
            mensagem.Type = "leave_channel";
            mensagem.Channel = Console.ReadLine();
            break;

        case "6":
            Console.Write("Nome do canal: ");
            mensagem.Channel = Console.ReadLine();

            Console.Write("Mensagem: ");
            mensagem.Text = Console.ReadLine();

            mensagem.Type = "post";
            break;

        case "7":
            Console.Write("Nome do canal: ");
            mensagem.Type = "get_messages";
            mensagem.Channel = Console.ReadLine();
            break;

        case "8":
            mensagem.Type = "logout";
            break;
        default:
            Console.WriteLine("Opção inválida.");
            continue;
    }

    if (usuarioLogado == null && opcao != "1" && opcao != "3")
    {
        Console.WriteLine("Você precisa fazer login primeiro.");
        continue;
    }

    mensagem.Clock = Interlocked.Increment(ref relogioLogico);
    byte[] dados = MessagePackSerializer.Serialize(mensagem);

    LogMensagem.Exibir("CLIENTE", "ENVIANDO REQ", mensagem);
    socket.SendFrame(dados);

    byte[] respostaBytes = socket.ReceiveFrameBytes();

    Envelope resposta = MessagePackSerializer.Deserialize<Envelope>(respostaBytes);
    LogMensagem.Exibir("CLIENTE", "RECEBENDO REP", resposta);
    long atual;
    long novo;

    do
    {
        atual = Volatile.Read(ref relogioLogico);
        novo = Math.Max(atual, resposta.Clock) + 1;
    }
    while (Interlocked.CompareExchange(ref relogioLogico, novo, atual) != atual);

    Console.WriteLine($"Relógio lógico do Cliente: {novo}");

    Console.WriteLine($"Resposta do servidor: {resposta.Status}");

    if (resposta.Status == "error")
    {
        Console.WriteLine($"Erro: {resposta.Error}");
    }

    if (opcao == "1" && resposta.Status == "ok")
    {
        usuarioLogado = resposta.User;
        Console.WriteLine($"Login realizado: {usuarioLogado}");
    }

    if (opcao == "4" && resposta.Status == "ok" && mensagem.Channel != null)
    {
        canaisInscritos.TryAdd(mensagem.Channel, 0);
    }

    if (opcao == "5" && resposta.Status == "ok" && mensagem.Channel != null)
    {
        canaisInscritos.TryRemove(mensagem.Channel, out _);
    }

    if (opcao == "8" && resposta.Status == "ok")
    {
        usuarioLogado = null;
        canaisInscritos.Clear();
        Console.WriteLine("Logout realizado com sucesso.");
    }

    if (resposta.Channels != null)
    {
        Console.WriteLine("Canais cadastrados:");

        foreach (string canal in resposta.Channels)
        {
            Console.WriteLine($"- {canal}");
        }
    }

    if (resposta.Messages != null)
    {
        Console.WriteLine("Mensagens do canal:");

        if (resposta.Messages.Count == 0)
        {
            Console.WriteLine("Nenhuma mensagem encontrada.");
        }

        foreach (Envelope msg in resposta.Messages)
        {
            Console.WriteLine($"ID: {msg.Id}");
            Console.WriteLine($"[{msg.User}] {msg.Text}");
        }
    }
}
