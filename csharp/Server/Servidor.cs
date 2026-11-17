using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

namespace Server;

public class Servidor
{
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

            Envelope resposta = new Envelope
            {
                Type = "REP",
                Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Clock = mensagem.Clock + 1,
                From = "Server",
                Status = "OK"
            };

            byte[] respostaBytes = MessagePackSerializer.Serialize(resposta);

            socket.SendFrame(respostaBytes);
        }
    }
}
