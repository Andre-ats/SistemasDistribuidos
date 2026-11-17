
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

using var socket = new RequestSocket();

Console.WriteLine("1 - Login");
Console.WriteLine("2 - Criar canal");
Console.WriteLine("3 - Listar canais");
Console.WriteLine("4 - Entrar em um canal");
Console.WriteLine("5 - Sair de um canal");
Console.WriteLine("6 - Enviar mensagem");
Console.WriteLine("7 - Consultar mensagens");

Console.Write("Escolha uma opção: ");
string opcao = Console.ReadLine() ?? "";

socket.Connect("tcp://localhost:6000");


Envelope mensagem = new Envelope
{
    Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    Clock = 1,
    From = "Cliente"
};

switch (opcao)
{
    case "1":
        Console.Write("Nome do usuário: ");
        mensagem.Type = "login";
        mensagem.User = Console.ReadLine();
        break;

    case "2":
        Console.Write("Nome do canal: ");
        mensagem.Type = "create_channel";
        mensagem.Channel = Console.ReadLine();
        mensagem.User = "Andre";
        break;

    case "3":
        mensagem.Type = "list_channels";
        break;

        case "4":
            Console.Write("Nome do usuário: ");
            mensagem.User = Console.ReadLine();

            Console.Write("Nome do canal: ");
            mensagem.Channel = Console.ReadLine();

            mensagem.Type = "join_channel";
            break;

        case "5":
            Console.Write("Nome do usuário: ");
            mensagem.User = Console.ReadLine();

            Console.Write("Nome do canal: ");
            mensagem.Channel = Console.ReadLine();

            mensagem.Type = "leave_channel";
            break;
        case "6":
            Console.Write("Nome do usuário: ");
            mensagem.User = Console.ReadLine();

            Console.Write("Nome do canal: ");
            mensagem.Channel = Console.ReadLine();

            Console.Write("Mensagem: ");
            mensagem.Text = Console.ReadLine();

            mensagem.Type = "post";
            break;
        case "7":
            Console.Write("Nome do usuário: ");
            mensagem.User = Console.ReadLine();

            Console.Write("Nome do canal: ");
            mensagem.Channel = Console.ReadLine();

            mensagem.Type = "get_messages";
            break;
        default:
            Console.WriteLine("Opção inválida");
            return;
}


byte[] dados = MessagePackSerializer.Serialize(mensagem);

socket.SendFrame(dados);

Console.WriteLine("Mensagem enviada!");

byte[] respostaBytes = socket.ReceiveFrameBytes();

Envelope resposta = MessagePackSerializer.Deserialize<Envelope>(respostaBytes);

Console.WriteLine($"Resposta do servidor: {resposta.Status}");

if (resposta.Status == "error")
{
    Console.WriteLine($"Erro: {resposta.Error}");
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
