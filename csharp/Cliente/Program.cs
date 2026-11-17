
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

using System.Collections.Concurrent;
using System.Threading;

using var socket = new RequestSocket();

socket.Connect("tcp://localhost:6000");

string? usuarioLogado = null;


ConcurrentDictionary<string, byte> canaisInscritos = new();

Thread threadMensagens = new(() =>
{
    using var subscriber = new SubscriberSocket();

    subscriber.Connect("tcp://localhost:6001");
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

        Console.WriteLine();
        Console.WriteLine($"[{canal}] {mensagemRecebida.User}: {mensagemRecebida.Text}");
        Console.Write("Escolha uma opção: ");
    }
});

threadMensagens.IsBackground = true;
threadMensagens.Start();


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
    string opcao = Console.ReadLine() ?? "";

    
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
                Clock = 1
            };

            socket.SendFrame(MessagePackSerializer.Serialize(logout));

            byte[] respostaLogout = socket.ReceiveFrameBytes();
            Envelope resultado = MessagePackSerializer.Deserialize<Envelope>(respostaLogout);

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
        Clock = 1,
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

    byte[] dados = MessagePackSerializer.Serialize(mensagem);

    socket.SendFrame(dados);

    byte[] respostaBytes = socket.ReceiveFrameBytes();

    Envelope resposta = MessagePackSerializer.Deserialize<Envelope>(respostaBytes);

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
            Console.WriteLine($"[{msg.User}] {msg.Text}");
        }
    }
}
