
using Common;
using MessagePack;
using NetMQ;
using NetMQ.Sockets;

int porta = int.Parse(
    Environment.GetEnvironmentVariable("REFERENCIA_PORT") ?? "5559"
);

int tempoLimiteHeartbeat = int.Parse(
    Environment.GetEnvironmentVariable("HEARTBEAT_TIMEOUT") ?? "30"
);

// nome -> informações do servidor (sem repetição de nome)
Dictionary<string, ServidorInfo> servidores = new();

// nome -> último heartbeat recebido (apenas servidores disponíveis)
Dictionary<string, DateTimeOffset> ultimoHeartbeat = new();

int proximoRank = 1;
long relogioLogico = 0;

using var socket = new ResponseSocket();

socket.Bind($"tcp://*:{porta}");

Console.WriteLine($"Serviço de referência iniciado na porta {porta}!");
Console.WriteLine($"Servidores sem heartbeat por {tempoLimiteHeartbeat}s serão removidos.");

while (true)
{
    RemoverInativos();

    if (!socket.TryReceiveFrameBytes(TimeSpan.FromSeconds(1), out byte[]? dados))
    {
        continue;
    }

    Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);
    LogMensagem.Exibir("REFERENCIA", "RECEBENDO REQ", mensagem);

    relogioLogico = Math.Max(relogioLogico, mensagem.Clock);

    Envelope resposta;

    switch (mensagem.Type.ToLowerInvariant())
    {
        case "rank":
            resposta = Rank(mensagem);
            break;

        case "list":
            resposta = Listar();
            break;

        case "heartbeat":
            resposta = Heartbeat(mensagem);
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
    resposta.From = "Referencia";

    LogMensagem.Exibir("REFERENCIA", "ENVIANDO REP", resposta);

    socket.SendFrame(MessagePackSerializer.Serialize(resposta));
}


Envelope Rank(Envelope mensagem)
{
    string nome = mensagem.From;

    if (string.IsNullOrWhiteSpace(nome))
    {
        return new Envelope
        {
            Type = "rank_reply",
            Status = "error",
            Error = "Nome do servidor não informado"
        };
    }

    if (!servidores.ContainsKey(nome))
    {
        servidores.Add(nome, new ServidorInfo
        {
            Name = nome,
            Rank = proximoRank,
            Port = mensagem.Port ?? 0
        });

        proximoRank++;

        Console.WriteLine($"Servidor cadastrado: {nome} (rank {servidores[nome].Rank})");
    }
    else
    {
        servidores[nome].Port = mensagem.Port ?? servidores[nome].Port;

        Console.WriteLine($"Servidor {nome} pediu o rank novamente (rank {servidores[nome].Rank})");
    }

    ultimoHeartbeat[nome] = DateTimeOffset.UtcNow;

    return new Envelope
    {
        Type = "rank_reply",
        Status = "ok",
        Rank = servidores[nome].Rank
    };
}

Envelope Listar()
{
    return new Envelope
    {
        Type = "list_reply",
        Status = "ok",
        Servers = servidores.Values
            .Where(servidor => ultimoHeartbeat.ContainsKey(servidor.Name))
            .OrderBy(servidor => servidor.Rank)
            .ToList()
    };
}

Envelope Heartbeat(Envelope mensagem)
{
    string nome = mensagem.From;

    if (!servidores.ContainsKey(nome))
    {
        return new Envelope
        {
            Type = "heartbeat_reply",
            Status = "error",
            Error = "Servidor não cadastrado"
        };
    }

    if (!ultimoHeartbeat.ContainsKey(nome))
    {
        Console.WriteLine($"Servidor {nome} voltou para a lista de disponíveis");
    }

    ultimoHeartbeat[nome] = DateTimeOffset.UtcNow;

    // A hora agora vem do coordenador (Berkeley), o heartbeat só responde OK
    return new Envelope
    {
        Type = "heartbeat_reply",
        Status = "ok"
    };
}

void RemoverInativos()
{
    DateTimeOffset agora = DateTimeOffset.UtcNow;

    foreach (string nome in ultimoHeartbeat.Keys.ToList())
    {
        if ((agora - ultimoHeartbeat[nome]).TotalSeconds > tempoLimiteHeartbeat)
        {
            ultimoHeartbeat.Remove(nome);

            Console.WriteLine($"Servidor {nome} removido da lista (sem heartbeat)");
        }
    }
}
