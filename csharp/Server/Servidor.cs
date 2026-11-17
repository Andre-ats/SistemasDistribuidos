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
            string mensagem = socket.ReceiveFrameString();

            Console.WriteLine($"Mensagem recebida: {mensagem}");

            socket.SendFrame("Mensagem recebida com sucesso!");
        }
    }
}

