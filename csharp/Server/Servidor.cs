
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

    // Relógio físico: hora local = hora do sistema + desvio (em ms).
    private long desvioRelogio = 0;
    private int rank = 0;
    private int mensagensClientes = 0;

    // Eleição (algoritmo do valentão): o servidor disponível de maior rank vira coordenador.
    private volatile string? coordenador = null;
    private volatile string? enderecoCoordenador = null;
    private int eleicaoEmAndamento = 0;
    private long anunciosRecebidos = 0;

    // Berkeley: diferença (ms) de cada servidor em relação ao coordenador.
    private readonly Dictionary<string, long> diferencasRelogio = new(StringComparer.OrdinalIgnoreCase);

    private readonly object travaRelogio = new();
    private readonly object travaPublisher = new();
    private readonly object travaEstado = new();
    private PublisherSocket? publisher;

    private readonly Persistencia persistencia;
    private readonly List<LoginRegistro> historicoLogins = new();

    private readonly int porta;
    private readonly int portaPub;
    private readonly int portaSincronizacao;
    private readonly string servidorId;
    private readonly string proxyHost = Environment.GetEnvironmentVariable("PROXY_HOST") ?? "localhost";
    private readonly string referenciaHost = Environment.GetEnvironmentVariable("REFERENCE_HOST") ?? "localhost";
    private readonly int referenciaPorta = int.Parse(Environment.GetEnvironmentVariable("REFERENCE_PORT") ?? "5000");

    public Servidor(int porta, int portaPub, string servidorId)
    {
        this.porta = porta;
        this.portaPub = portaPub;
        this.servidorId = servidorId;

        portaSincronizacao = int.Parse(
            Environment.GetEnvironmentVariable("SYNC_PORT") ?? $"{porta + 2}"
        );

        // Desvio inicial opcional para simular um relógio físico atrasado/adiantado.
        desvioRelogio = long.Parse(Environment.GetEnvironmentVariable("CLOCK_DRIFT_MS") ?? "0");

        persistencia = new Persistencia(servidorId);
    }

    private long HoraAtual()
    {
        lock (travaRelogio)
        {
            return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + desvioRelogio;
        }
    }

    private long IncrementarRelogio()
    {
        lock (travaRelogio)
        {
            relogioLogico++;
            return relogioLogico;
        }
    }

    private long AtualizarRelogio(long recebido)
    {
        lock (travaRelogio)
        {
            relogioLogico = Math.Max(relogioLogico, recebido);
            return relogioLogico;
        }
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

        // Regrava os arquivos na ordem padrão (dados antigos podem estar em outra ordem).
        foreach (string canal in canais)
        {
            OrdenarMensagens(canal);
            persistencia.SalvarMensagens(canal, mensagens[canal]);
        }

        persistencia.SalvarCanais(canais);
        persistencia.SalvarLogins(historicoLogins);

        Console.WriteLine($"Logins registrados: {historicoLogins.Count}");
        Console.WriteLine($"Canais carregados: {canais.Count}");

        using var socket = new ResponseSocket();

        socket.Bind($"tcp://*:{porta}");

        publisher = new PublisherSocket();

        publisher.Connect($"tcp://{proxyHost}:5557");

        Console.WriteLine($"Servidor iniciado na porta {porta}!");
        Console.WriteLine($"Publicações enviadas ao proxy {proxyHost}:5557!");
        Console.WriteLine($"Relógio físico inicial: {DateTimeOffset.FromUnixTimeMilliseconds(HoraAtual()):O} (desvio {desvioRelogio} ms)");

        PedirRank();
        ListarServidores();

        Thread threadServidores = new(AtenderServidores);
        threadServidores.IsBackground = true;
        threadServidores.Start();

        Thread threadCoordenador = new(OuvirServidores);
        threadCoordenador.IsBackground = true;
        threadCoordenador.Start();

        // O SUB de "replication" já está ativo; então busca o que foi perdido.
        Thread.Sleep(1000);
        SincronizarDados(ListarServidores());

        // Também envia aos outros o que só este servidor tem (ex.: dados de antes de cair).
        // Espera um pouco para os outros servidores já estarem assinando "replication".
        Task.Run(() =>
        {
            Thread.Sleep(10000);
            DivulgarEstado();
        });

        // Aguarda o SUB conectar ao proxy antes da primeira eleição.
        Task.Run(() =>
        {
            Thread.Sleep(3000);
            Eleicao();
        });

        while (true)
        {
            byte[] dados = socket.ReceiveFrameBytes();

            Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);
            LogMensagem.Exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            Console.WriteLine($"Relógio do servidor: {AtualizarRelogio(mensagem.Clock)}");

            Console.WriteLine($"Mensagem recebida: {mensagem.Type}");

            Envelope resposta;

            lock (travaEstado)
            {
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
                        resposta = PublicarMensagem(mensagem);
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
            }

            resposta.Ts = HoraAtual();
            resposta.Clock = IncrementarRelogio();
            resposta.From = "Server";

            LogMensagem.Exibir("SERVIDOR", "ENVIANDO REP", resposta);
            byte[] respostaBytes = MessagePackSerializer.Serialize(resposta);

            socket.SendFrame(respostaBytes);

            mensagensClientes++;

            // Heartbeat a cada 10 mensagens de clientes recebidas.
            if (mensagensClientes % 10 == 0)
            {
                EnviarHeartbeat();
            }

            // Sincronização do relógio físico com o coordenador a cada 15 mensagens.
            if (mensagensClientes % 15 == 0)
            {
                SincronizarRelogio();
            }
        }
    }

    private Envelope? EnviarReferencia(Envelope requisicao)
    {
        for (int tentativa = 1; tentativa <= 3; tentativa++)
        {
            try
            {
                using var requisicaoSocket = new RequestSocket();
                requisicaoSocket.Options.Linger = TimeSpan.Zero;
                requisicaoSocket.Connect($"tcp://{referenciaHost}:{referenciaPorta}");

                requisicao.Ts = HoraAtual();
                requisicao.Clock = IncrementarRelogio();
                requisicao.From = servidorId;

                LogMensagem.Exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

                requisicaoSocket.SendFrame(
                    MessagePackSerializer.Serialize(requisicao)
                );

                if (!requisicaoSocket.TryReceiveFrameBytes(
                    TimeSpan.FromSeconds(3),
                    out byte[]? respostaBytes))
                {
                    Console.WriteLine(
                        $"[{servidorId}] Referência sem resposta. Tentativa {tentativa}/3"
                    );

                    Thread.Sleep(1000);
                    continue;
                }

                Envelope resposta =
                    MessagePackSerializer.Deserialize<Envelope>(respostaBytes);

                LogMensagem.Exibir("SERVIDOR", "RECEBENDO REP", resposta);

                Console.WriteLine($"Relógio do servidor: {AtualizarRelogio(resposta.Clock)}");

                return resposta;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{servidorId}] Erro com a referência: {ex.Message}");
                Thread.Sleep(1000);
            }
        }

        return null;
    }

    private void PedirRank()
    {
        while (true)
        {
            Envelope? resposta = EnviarReferencia(new Envelope
            {
                Type = "rank",
                Server = servidorId,
                Address = $"{servidorId}:{portaSincronizacao}"
            });

            if (resposta?.Status == "ok" && resposta.Rank != null)
            {
                rank = resposta.Rank.Value;
                Console.WriteLine($"[{servidorId}] Rank recebido: {rank}");
                return;
            }

            Console.WriteLine($"[{servidorId}] Não foi possível obter o rank. Tentando novamente...");
            Thread.Sleep(2000);
        }
    }

    private List<ServidorInfo> ListarServidores()
    {
        Envelope? resposta = EnviarReferencia(new Envelope
        {
            Type = "list"
        });

        List<ServidorInfo> lista = resposta?.Servers ?? new List<ServidorInfo>();

        Console.WriteLine($"[{servidorId}] Servidores disponíveis:");

        foreach (ServidorInfo info in lista)
        {
            Console.WriteLine($"- {info.Name} (rank {info.Rank}) em {info.Address}");
        }

        return lista;
    }

    private void EnviarHeartbeat()
    {
        Envelope? resposta = EnviarReferencia(new Envelope
        {
            Type = "heartbeat",
            Server = servidorId
        });

        if (resposta?.Status == "error" && resposta.Error == "Servidor não registrado")
        {
            PedirRank();
            return;
        }

        if (resposta?.Status != "ok")
        {
            Console.WriteLine($"[{servidorId}] Heartbeat sem resposta.");
            return;
        }

        Console.WriteLine($"[{servidorId}] Heartbeat confirmado: {resposta.Text}");
    }

    private void AjustarRelogioFisico(long horaCorreta, long enviadoEm, long recebidoEm)
    {
        // Compensa metade do tempo de ida e volta (algoritmo de Cristian).
        long horaEstimada = horaCorreta + (recebidoEm - enviadoEm) / 2;
        long antes = HoraAtual();

        lock (travaRelogio)
        {
            desvioRelogio = horaEstimada - recebidoEm;
        }

        long depois = HoraAtual();

        Console.WriteLine($"[{servidorId}] Relógio físico ajustado:");
        Console.WriteLine($"  Antes: {DateTimeOffset.FromUnixTimeMilliseconds(antes):O}");
        Console.WriteLine($"  Depois: {DateTimeOffset.FromUnixTimeMilliseconds(depois):O}");
        Console.WriteLine($"  Correção: {depois - antes} ms");
    }

    
    // Atende as requisições de outros servidores (relógio e eleição) em uma porta própria,
    // para não misturar com as requisições dos clientes.
    private void AtenderServidores()
    {
        using var socketServidores = new ResponseSocket();

        socketServidores.Bind($"tcp://*:{portaSincronizacao}");

        Console.WriteLine($"[{servidorId}] Atendendo servidores na porta {portaSincronizacao}!");

        while (true)
        {
            byte[] dados = socketServidores.ReceiveFrameBytes();

            Envelope mensagem = MessagePackSerializer.Deserialize<Envelope>(dados);
            LogMensagem.Exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            Console.WriteLine($"Relógio do servidor: {AtualizarRelogio(mensagem.Clock)}");

            Envelope resposta;

            switch (mensagem.Type.ToLowerInvariant())
            {
                case "clock":
                    resposta = RelogioCoordenador(mensagem);
                    break;

                case "sync":
                    resposta = EstadoCompleto();
                    break;

                case "election":
                    resposta = new Envelope
                    {
                        Type = "election_reply",
                        Status = "ok",
                        Server = servidorId,
                        Rank = rank,
                        Text = "OK"
                    };

                    // Responde OK e assume a eleição a partir deste servidor.
                    Task.Run(Eleicao);
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

            resposta.Ts = HoraAtual();
            resposta.Clock = IncrementarRelogio();
            resposta.From = servidorId;

            LogMensagem.Exibir("SERVIDOR", "ENVIANDO REP", resposta);

            socketServidores.SendFrame(MessagePackSerializer.Serialize(resposta));
        }
    }

    // Recebe pelo tópico "servers" o nome do servidor eleito como coordenador
    // e pelo tópico "replication" as escritas feitas nos outros servidores.
    private void OuvirServidores()
    {
        using var subscriber = new SubscriberSocket();

        subscriber.Connect($"tcp://{proxyHost}:5558");
        subscriber.Subscribe("servers");
        subscriber.Subscribe("replication");

        while (true)
        {
            string topico = subscriber.ReceiveFrameString();
            byte[] dados = subscriber.ReceiveFrameBytes();

            if (topico == "replication")
            {
                Envelope operacao = MessagePackSerializer.Deserialize<Envelope>(dados);

                if (operacao.Server == servidorId)
                {
                    continue;
                }

                LogMensagem.Exibir("SERVIDOR", "RECEBENDO PUB", operacao);

                Console.WriteLine($"Relógio do servidor: {AtualizarRelogio(operacao.Clock)}");

                AplicarReplicacao(operacao);
                continue;
            }

            if (topico != "servers")
            {
                continue;
            }

            Envelope anuncio = MessagePackSerializer.Deserialize<Envelope>(dados);
            LogMensagem.Exibir("SERVIDOR", "RECEBENDO PUB", anuncio);

            Console.WriteLine($"Relógio do servidor: {AtualizarRelogio(anuncio.Clock)}");

            if (anuncio.Rank != null && anuncio.Rank.Value < rank)
            {
                // Valentão: um servidor de rank menor se anunciou, então este servidor assume.
                Console.WriteLine($"[{servidorId}] Coordenador {anuncio.Server} tem rank menor; iniciando eleição.");
                Task.Run(Eleicao);
                continue;
            }

            coordenador = anuncio.Server;
            enderecoCoordenador = anuncio.Address;
            Interlocked.Increment(ref anunciosRecebidos);

            lock (travaRelogio)
            {
                diferencasRelogio.Clear();
            }

            Console.WriteLine($"[{servidorId}] Coordenador atual: {coordenador} ({enderecoCoordenador})");
        }
    }

    private Envelope? EnviarServidor(string endereco, Envelope requisicao)
    {
        try
        {
            using var requisicaoSocket = new RequestSocket();
            requisicaoSocket.Options.Linger = TimeSpan.Zero;
            requisicaoSocket.Connect($"tcp://{endereco}");

            requisicao.Ts = HoraAtual();
            requisicao.Clock = IncrementarRelogio();
            requisicao.From = servidorId;
            requisicao.Server ??= servidorId;

            LogMensagem.Exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

            requisicaoSocket.SendFrame(
                MessagePackSerializer.Serialize(requisicao)
            );

            if (!requisicaoSocket.TryReceiveFrameBytes(
                TimeSpan.FromSeconds(2),
                out byte[]? respostaBytes))
            {
                Console.WriteLine($"[{servidorId}] Servidor {endereco} sem resposta.");
                return null;
            }

            Envelope resposta =
                MessagePackSerializer.Deserialize<Envelope>(respostaBytes);

            LogMensagem.Exibir("SERVIDOR", "RECEBENDO REP", resposta);

            Console.WriteLine($"Relógio do servidor: {AtualizarRelogio(resposta.Clock)}");

            return resposta;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{servidorId}] Erro com o servidor {endereco}: {ex.Message}");
            return null;
        }
    }

    private void Eleicao()
    {
        if (Interlocked.Exchange(ref eleicaoEmAndamento, 1) == 1)
        {
            Console.WriteLine($"[{servidorId}] Eleição já em andamento.");
            return;
        }

        try
        {
            while (true)
            {
                Console.WriteLine($"[{servidorId}] Iniciando eleição (rank {rank})");

                List<ServidorInfo> lista = ListarServidores();
                bool recebeuOk = false;

                foreach (ServidorInfo info in lista)
                {
                    if (info.Rank <= rank || info.Name == servidorId)
                    {
                        continue;
                    }

                    Envelope? resposta = EnviarServidor(info.Address, new Envelope
                    {
                        Type = "election",
                        Rank = rank
                    });

                    if (resposta?.Status == "ok")
                    {
                        recebeuOk = true;
                    }
                }

                if (!recebeuOk)
                {
                    TornarCoordenador();
                    return;
                }

                // Um servidor de rank maior respondeu: aguarda o anúncio dele no tópico "servers".
                Console.WriteLine($"[{servidorId}] Servidor de rank maior respondeu; aguardando anúncio do coordenador...");

                long antes = Interlocked.Read(ref anunciosRecebidos);

                for (int i = 0; i < 20 && Interlocked.Read(ref anunciosRecebidos) == antes; i++)
                {
                    Thread.Sleep(500);
                }

                if (Interlocked.Read(ref anunciosRecebidos) != antes)
                {
                    return;
                }

                Console.WriteLine($"[{servidorId}] Nenhum coordenador anunciado; repetindo eleição.");
            }
        }
        finally
        {
            Interlocked.Exchange(ref eleicaoEmAndamento, 0);
        }
    }

    private void TornarCoordenador()
    {
        coordenador = servidorId;
        enderecoCoordenador = $"{servidorId}:{portaSincronizacao}";

        Envelope anuncio = new Envelope
        {
            Type = "coordinator",
            Server = servidorId,
            Rank = rank,
            Address = enderecoCoordenador,
            From = servidorId,
            Ts = HoraAtual(),
            Clock = IncrementarRelogio()
        };

        LogMensagem.Exibir("SERVIDOR", "ENVIANDO PUB", anuncio);

        lock (travaPublisher)
        {
            publisher!
                .SendMoreFrame("servers")
                .SendFrame(MessagePackSerializer.Serialize(anuncio));
        }

        Console.WriteLine($"[{servidorId}] Eleito como coordenador!");
    }

    private void SincronizarRelogio()
    {
        string? atual = coordenador;
        string? endereco = enderecoCoordenador;

        if (atual == null || endereco == null)
        {
            Console.WriteLine($"[{servidorId}] Nenhum coordenador conhecido; iniciando eleição.");
            Task.Run(Eleicao);
            return;
        }

        if (atual == servidorId)
        {
            Console.WriteLine($"[{servidorId}] Este servidor é o coordenador; relógio usado como referência.");
            return;
        }

        long enviadoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        Envelope? resposta = EnviarServidor(endereco, new Envelope
        {
            Type = "clock"
        });

        long recebidoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        if (resposta?.Status != "ok" || resposta.Time == null)
        {
            Console.WriteLine($"[{servidorId}] Coordenador {atual} indisponível; iniciando eleição.");
            coordenador = null;
            enderecoCoordenador = null;
            Task.Run(Eleicao);
            return;
        }

        Console.WriteLine($"[{servidorId}] {resposta.Text}");

        AjustarRelogioFisico(resposta.Time.Value, enviadoEm, recebidoEm);
    }

    // Berkeley no coordenador: guarda a diferença de cada servidor, calcula a média
    // (incluindo o próprio coordenador com diferença 0), ajusta o próprio relógio
    // pela média e devolve a hora correta para o servidor que pediu.
    private Envelope RelogioCoordenador(Envelope mensagem)
    {
        if (coordenador != servidorId)
        {
            return new Envelope
            {
                Type = "clock_reply",
                Status = "error",
                Error = "Servidor não é o coordenador"
            };
        }

        string nome = mensagem.Server ?? mensagem.From;
        long media;
        long horaCorreta;
        int quantidade;

        lock (travaRelogio)
        {
            long agora = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + desvioRelogio;

            diferencasRelogio[nome] = mensagem.Ts - agora;

            media = diferencasRelogio.Values.Sum() / (diferencasRelogio.Count + 1);

            desvioRelogio += media;

            foreach (string chave in diferencasRelogio.Keys.ToList())
            {
                diferencasRelogio[chave] -= media;
            }

            // Quem pediu passa a usar a hora correta, então sua diferença vira zero.
            diferencasRelogio[nome] = 0;

            horaCorreta = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + desvioRelogio;
            quantidade = diferencasRelogio.Count + 1;
        }

        Console.WriteLine($"[{servidorId}] Berkeley: {quantidade} relógios, média {media} ms");

        return new Envelope
        {
            Type = "clock_reply",
            Status = "ok",
            Server = servidorId,
            Time = horaCorreta,
            Text = $"Hora correta do coordenador {servidorId} (média Berkeley {media} ms)"
        };
    }

    // ===== Replicação =====
    // Replicação ativa: toda escrita aceita por um servidor (login, canal e mensagem)
    // é publicada no tópico "replication" e aplicada por todos os outros servidores.

    private void Replicar(Envelope operacao)
    {
        operacao.Server = servidorId;
        operacao.From = servidorId;
        operacao.Ts = HoraAtual();
        operacao.Clock = IncrementarRelogio();

        LogMensagem.Exibir("SERVIDOR", "ENVIANDO PUB", operacao);

        lock (travaPublisher)
        {
            publisher!
                .SendMoreFrame("replication")
                .SendFrame(MessagePackSerializer.Serialize(operacao));
        }
    }

    private void AplicarReplicacao(Envelope operacao)
    {
        if (operacao.Server == servidorId)
        {
            return;
        }

        lock (travaEstado)
        {
            switch (operacao.Type)
            {
                case "replicate_login":
                    AplicarLogin(new LoginRegistro
                    {
                        User = operacao.User ?? "",
                        Ts = operacao.Time ?? operacao.Ts
                    });
                    break;

                case "replicate_channel":
                    AplicarCanal(operacao.Channel ?? "");
                    break;

                case "replicate_state":
                    foreach (LoginRegistro registro in operacao.Logins ?? new List<LoginRegistro>())
                    {
                        AplicarLogin(registro);
                    }

                    foreach (string canal in operacao.Channels ?? new List<string>())
                    {
                        AplicarCanal(canal);
                    }

                    foreach (Envelope item in operacao.Messages ?? new List<Envelope>())
                    {
                        AplicarMensagem(item);
                    }
                    break;

                case "replicate_post":
                    foreach (Envelope item in operacao.Messages ?? new List<Envelope>())
                    {
                        AplicarMensagem(item);
                    }
                    break;

                default:
                    Console.WriteLine($"[{servidorId}] Réplica com tipo desconhecido: {operacao.Type}");
                    break;
            }
        }
    }

    // As funções "Aplicar" são idempotentes: aplicar duas vezes a mesma operação
    // não duplica dados (importante para a transferência de estado na entrada).

    private bool AplicarLogin(LoginRegistro registro)
    {
        if (historicoLogins.Any(l => l.User == registro.User && l.Ts == registro.Ts))
        {
            return false;
        }

        historicoLogins.Add(registro);
        historicoLogins.Sort((a, b) => a.Ts.CompareTo(b.Ts));

        persistencia.SalvarLogins(historicoLogins);

        Console.WriteLine($"[{servidorId}] Réplica aplicada: login de {registro.User}");

        return true;
    }

    private bool AplicarCanal(string canal)
    {
        if (string.IsNullOrWhiteSpace(canal) || canais.Contains(canal))
        {
            return false;
        }

        canais.Add(canal);
        participantes.Add(canal, new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        mensagens.Add(canal, new List<Envelope>());

        persistencia.SalvarCanais(canais);
        persistencia.SalvarMensagens(canal, mensagens[canal]);

        Console.WriteLine($"[{servidorId}] Réplica aplicada: canal {canal}");

        return true;
    }

    private bool AplicarMensagem(Envelope mensagem)
    {
        string canal = mensagem.Channel ?? "";

        // Garante o canal caso a mensagem chegue antes da criação replicada.
        AplicarCanal(canal);

        if (!mensagens.ContainsKey(canal) || mensagens[canal].Any(m => m.Id == mensagem.Id))
        {
            return false;
        }

        mensagens[canal].Add(mensagem);
        OrdenarMensagens(canal);

        persistencia.SalvarMensagens(canal, mensagens[canal]);

        Console.WriteLine($"[{servidorId}] Réplica aplicada: [{canal}] {mensagem.User}: {mensagem.Text}");

        return true;
    }

    // Ordem total igual em todas as réplicas: relógio lógico, depois timestamp e Id.
    private void OrdenarMensagens(string canal)
    {
        mensagens[canal] = mensagens[canal]
            .OrderBy(m => m.Clock)
            .ThenBy(m => m.Ts)
            .ThenBy(m => m.Id, StringComparer.Ordinal)
            .ToList();
    }

    // Transferência de estado: ao iniciar, o servidor pede a outro servidor disponível
    // tudo o que perdeu enquanto estava fora (ou tudo, se for um servidor novo).
    private void SincronizarDados(List<ServidorInfo> lista)
    {
        foreach (ServidorInfo info in lista)
        {
            if (info.Name == servidorId)
            {
                continue;
            }

            Envelope? resposta = EnviarServidor(info.Address, new Envelope
            {
                Type = "sync"
            });

            if (resposta?.Status != "ok")
            {
                continue;
            }

            int novos = 0;

            lock (travaEstado)
            {
                foreach (LoginRegistro registro in resposta.Logins ?? new List<LoginRegistro>())
                {
                    if (AplicarLogin(registro)) novos++;
                }

                foreach (string canal in resposta.Channels ?? new List<string>())
                {
                    if (AplicarCanal(canal)) novos++;
                }

                foreach (Envelope item in resposta.Messages ?? new List<Envelope>())
                {
                    if (AplicarMensagem(item)) novos++;
                }
            }

            Console.WriteLine($"[{servidorId}] Dados sincronizados com {info.Name}: {novos} itens novos.");
            return;
        }

        Console.WriteLine($"[{servidorId}] Nenhum outro servidor disponível para sincronizar dados.");
    }

    private void DivulgarEstado()
    {
        Envelope estado = EstadoCompleto();

        estado.Type = "replicate_state";
        estado.Status = null;

        Console.WriteLine($"[{servidorId}] Enviando estado completo aos outros servidores.");

        Replicar(estado);
    }

    private Envelope EstadoCompleto()
    {
        lock (travaEstado)
        {
            return new Envelope
            {
                Type = "sync_reply",
                Status = "ok",
                Server = servidorId,
                Channels = canais.OrderBy(canal => canal).ToList(),
                Messages = mensagens.Values.SelectMany(lista => lista).ToList(),
                Logins = historicoLogins.ToList()
            };
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

        LoginRegistro registro = new LoginRegistro
        {
            User = usuario,
            Ts = HoraAtual()
        };

        historicoLogins.Add(registro);

        persistencia.SalvarLogins(historicoLogins);

        Replicar(new Envelope
        {
            Type = "replicate_login",
            User = usuario,
            Time = registro.Ts
        });

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
        persistencia.SalvarMensagens(canal, mensagens[canal]);

        Replicar(new Envelope
        {
            Type = "replicate_channel",
            Channel = canal
        });

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


    private Envelope PublicarMensagem(Envelope mensagem)
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

        Envelope novaMensagem = new Envelope
        {
            Id = Guid.NewGuid().ToString(),
            Type = "post",
            User = usuario,
            Channel = canal,
            Text = texto,
            From = usuario,
            Ts = HoraAtual(),
            Clock = IncrementarRelogio()
        };

        mensagens[canal].Add(novaMensagem);
        OrdenarMensagens(canal);
        persistencia.SalvarMensagens(canal, mensagens[canal]);
        LogMensagem.Exibir("SERVIDOR", "ENVIANDO PUB", novaMensagem);

        byte[] dadosMensagem = MessagePackSerializer.Serialize(novaMensagem);

        lock (travaPublisher)
        {
            publisher!
                .SendMoreFrame(canal)
                .SendFrame(dadosMensagem);
        }

        Console.WriteLine($"[{canal}] {usuario}: {texto}");

        Replicar(new Envelope
        {
            Type = "replicate_post",
            Channel = canal,
            Messages = new List<Envelope> { novaMensagem }
        });

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
