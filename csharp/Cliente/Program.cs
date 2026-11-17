
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

using var socket = new RequestSocket();

socket.Connect("tcp://localhost:6000");

Envelope mensagem = new Envelope
{
    Type = "LOGIN",
    Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
    Clock = 1,
    From = "Cliente",
    User = "Andre"
};

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
