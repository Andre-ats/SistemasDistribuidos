
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;
using System.Text.RegularExpressions;

namespace Server;

public class Servidor
{
    private readonly HashSet<string> usuarios = new(StringComparer.OrdinalIgnoreCase);
    public void Iniciar()
    {
        using var socket = new ResponseSocket();

        socket.Bind("tcp://*:6000");

        Console.WriteLine("Servidor iniciado na porta 6000!");

        while (true)
        {
            byte[] dados = socket.ReceiveFrameBytes();

            Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);

            Console.WriteLine($"Mensagem recebida: {mensagem.Type}");

            Envelope resposta;

            switch (mensagem.Type.ToLowerInvariant())
            {
                case "login":
                    resposta = Login(mensagem);
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
            resposta.Clock = mensagem.Clock + 1;
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

        Console.WriteLine($"Usuário conectado: {usuario}");

        return new Envelope
        {
            Type = "login_reply",
            Status = "ok",
            User = usuario
        };
    }

}
