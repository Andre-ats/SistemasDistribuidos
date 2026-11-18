
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

string proxyHost = Environment.GetEnvironmentVariable("PROXY_HOST") ?? "localhost";

using var socket = new RequestSocket();

socket.Connect($"tcp://{servidorHost}:{portaServidor}");

Console.WriteLine($"Conectado ao servidor na porta {portaServidor}");

string? usuarioLogado = null;
long relogioLogico = 0;


ConcurrentDictionary<string, byte> canaisInscritos = new();

// No modo interativo, enquanto o usuário digita, as publicações recebidas ficam
// guardadas e só são exibidas depois, para não misturar com o que está sendo digitado.
int pausarMensagens = 0;
ConcurrentQueue<string> mensagensPendentes = new();

Thread threadMensagens = new(() =>
{
    using var subscriber = new SubscriberSocket();

    subscriber.Connect($"tcp://{proxyHost}:5558");
    // O socket SUB pertence exclusivamente a esta thread.
    // As inscricoes desejadas sao compartilhadas via ConcurrentDictionary.
    HashSet<string> topicosAtivos = new(StringComparer.Ordinal);

    while (true)
    {
        // Sincroniza inscricoes reais do ZeroMQ com os canais do cliente.
        foreach (string topico in topicosAtivos.ToArray())
        {
            if (!canaisInscritos.ContainsKey(topico))
            {
                subscriber.Unsubscribe(topico);
                topicosAtivos.Remove(topico);
            }
        }

        foreach (string topico in canaisInscritos.Keys)
        {
            if (topicosAtivos.Add(topico))
            {
                subscriber.Subscribe(topico);
            }
        }

        // Timeout curto permite atualizar inscricoes sem bloquear a thread.
        if (!subscriber.TryReceiveFrameString(TimeSpan.FromMilliseconds(100), out string? canal))
        {
            continue;
        }

        byte[] dadosMensagem = subscriber.ReceiveFrameBytes();

        if (canal == null || !canaisInscritos.ContainsKey(canal))
        {
            continue;
        }

        Envelope mensagemRecebida =
            MessagePackSerializer.Deserialize<Envelope>(dadosMensagem);
        long recebidoEm = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        long atual;
        long novo;

        do
        {
            atual = Volatile.Read(ref relogioLogico);
            novo = Math.Max(atual, mensagemRecebida.Clock);
        }
        while (Interlocked.CompareExchange(ref relogioLogico, novo, atual) != atual);

        var texto = new System.Text.StringBuilder();

        texto.AppendLine(LogMensagem.Formatar("CLIENTE", "RECEBENDO PUB", mensagemRecebida));
        texto.AppendLine($"Relógio lógico do Cliente: {novo}");
        texto.AppendLine();
        texto.AppendLine($"[{canal}] {mensagemRecebida.User}: {mensagemRecebida.Text}");
        texto.AppendLine($"Enviada em: {DateTimeOffset.FromUnixTimeMilliseconds(mensagemRecebida.Ts):O}");
        texto.AppendLine($"Recebida em: {DateTimeOffset.FromUnixTimeMilliseconds(recebidoEm):O}");

        if (Volatile.Read(ref pausarMensagens) == 1)
        {
            mensagensPendentes.Enqueue(texto.ToString());
            continue;
        }

        Console.Write(texto.ToString());
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

    Console.WriteLine($"Modo automático iniciado: {usuario}");

    Envelope? EnviarAutomatico(string tipo, string? nomeCanal = null, string? texto = null)
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
                    Channel = nomeCanal,
                    Text = texto
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
                    novo = Math.Max(atual, resposta.Clock);
                }
                while (Interlocked.CompareExchange(
                    ref relogioLogico, novo, atual) != atual);

                Console.WriteLine($"[{usuario}] Relógio lógico do Cliente: {novo}");

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

    // O servidor persiste o histórico, mas as sessões e participantes são voláteis.
    // Após reinício, refazemos login e reinscrevemos o bot nos canais anteriores.
    bool RecuperarSessao()
    {
        Console.WriteLine($"[{usuario}] RECUPERACAO: servidor perdeu a sessão; refazendo login...");
        Envelope? novoLogin = EnviarAutomatico("login");
        if (novoLogin?.Status != "ok")
        {
            Console.WriteLine($"[{usuario}] RECUPERACAO: login não realizado: {novoLogin?.Error ?? "sem resposta"}");
            return false;
        }

        Envelope? listagemAtual = EnviarAutomatico("list_channels");
        if (listagemAtual?.Status != "ok")
        {
            Console.WriteLine($"[{usuario}] RECUPERACAO: falha ao consultar canais.");
            return false;
        }

        var existentes = new HashSet<string>(listagemAtual.Channels ?? new List<string>());
        var desejados = canaisInscritos.Keys.ToList();

        canaisInscritos.Clear();
        foreach (string nome in desejados)
        {
            if (!existentes.Contains(nome)) continue;
            Envelope? entrada = EnviarAutomatico("join_channel", nome);
            if (entrada?.Status == "ok" || entrada?.Error == "Usuário já está no canal")
            {
                canaisInscritos.TryAdd(nome, 0);
                Console.WriteLine($"[{usuario}] RECUPERACAO: inscrito novamente em '{nome}'");
            }
        }
        Console.WriteLine($"[{usuario}] RECUPERACAO CONCLUIDA: {canaisInscritos.Count} canais.");
        return true;
    }

    Envelope? EnviarComRecuperacao(string tipo, string? nomeCanal = null, string? texto = null)
    {
        Envelope? resposta = EnviarAutomatico(tipo, nomeCanal, texto);
        // Não repetir uma publicação que apenas perdeu a resposta: isso poderia duplicá-la.
        // Só repetimos quando o servidor confirmou que a sessão não existe.
        if (resposta?.Status == "error" && resposta.Error == "Usuário não está registrado")
        {
            if (RecuperarSessao())
                return EnviarAutomatico(tipo, nomeCanal, texto);
        }
        return resposta;
    }

    // Em caso de erro, o login é repetido até ser aceito.
    Envelope? login = EnviarAutomatico("login");

    while (login?.Status != "ok")
    {
        Console.WriteLine(
            $"[{usuario}] Login falhou: {login?.Error ?? "Servidor indisponível"}"
        );

        // O bot reiniciou, mas o servidor ainda guarda a sessão antiga: encerra e entra de novo.
        if (login?.Error == "Usuário já está registrado")
        {
            EnviarAutomatico("logout");
        }
        else
        {
            Thread.Sleep(2000);
        }

        Console.WriteLine($"[{usuario}] Repetindo login...");
        login = EnviarAutomatico("login");
    }

    Console.WriteLine($"[{usuario}] Login realizado!");

    Random aleatorio = new();
    string[] textos = { "Bom dia!", "Tudo certo?", "Alguém por aqui?", "Mensagem de teste", "Olá pessoal!", "Mais uma mensagem", "Teste Pub/Sub" };

    while (true)
    {
        Envelope? listagem = EnviarComRecuperacao("list_channels");
        if (listagem?.Status != "ok")
        {
            Console.WriteLine($"[{usuario}] Não foi possível listar canais.");
            Thread.Sleep(2000);
            continue;
        }

        List<string> disponiveis = listagem.Channels ?? new List<string>();
        if (disponiveis.Count < 5)
        {
            string novoCanal = $"canal_{usuario}_{Guid.NewGuid().ToString("N")[..6]}";
            Envelope? criacao = EnviarComRecuperacao("create_channel", novoCanal);
            if (criacao?.Status == "ok")
            {
                disponiveis.Add(novoCanal);
                Console.WriteLine($"[{usuario}] Canal '{novoCanal}' criado!");
            }
        }

        if (canaisInscritos.Count < 3)
        {
            List<string> candidatos = disponiveis.Where(c => !canaisInscritos.ContainsKey(c)).ToList();
            if (candidatos.Count > 0)
            {
                string escolhido = candidatos[aleatorio.Next(candidatos.Count)];
                Envelope? entrada = EnviarComRecuperacao("join_channel", escolhido);
                if (entrada?.Status == "ok")
                {
                    canaisInscritos.TryAdd(escolhido, 0);
                    Console.WriteLine($"[{usuario}] Inscrito no canal '{escolhido}'");
                }
            }
        }

        List<string> inscritos = canaisInscritos.Keys.ToList();
        if (inscritos.Count == 0)
        {
            Thread.Sleep(1000);
            continue;
        }

        string canalEscolhido = inscritos[aleatorio.Next(inscritos.Count)];
        for (int i = 0; i < 10; i++)
        {
            string texto = textos[aleatorio.Next(textos.Length)];
            Envelope? resposta = EnviarComRecuperacao("post", canalEscolhido, texto);
            Console.WriteLine($"[{usuario}] Publicação em {canalEscolhido}: {resposta?.Status ?? "sem resposta"}");
            Thread.Sleep(1000);
        }
    }

}


while (true)
{
    // Libera a exibição e mostra o que chegou enquanto o usuário digitava.
    Volatile.Write(ref pausarMensagens, 0);

    if (!mensagensPendentes.IsEmpty)
    {
        Console.WriteLine();
        Console.WriteLine($"===== Mensagens recebidas enquanto você digitava: {mensagensPendentes.Count} =====");

        while (mensagensPendentes.TryDequeue(out string? pendente))
        {
            Console.Write(pendente);
        }
    }

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

    // A partir daqui o usuário vai digitar: segura as publicações recebidas.
    Volatile.Write(ref pausarMensagens, 1);

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

    // Timestamp do envio (e não de quando o menu montou a mensagem).
    mensagem.Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
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
        novo = Math.Max(atual, resposta.Clock);
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
