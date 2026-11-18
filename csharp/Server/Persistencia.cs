
using Common;
using MessagePack;

namespace Server;

public class Persistencia
{
    private readonly string pastaDados;

    public Persistencia(string servidorId)
{
    string diretorioBase = Environment.GetEnvironmentVariable("DATA_DIR")
    ?? Path.Combine(AppContext.BaseDirectory, "dados");

    pastaDados = Path.Combine(diretorioBase, servidorId);

    Directory.CreateDirectory(pastaDados);
}

    public List<LoginRegistro> CarregarLogins()
    {
        string caminho = Path.Combine(pastaDados, "logins.msgpack");

        if (!File.Exists(caminho))
        {
            return new List<LoginRegistro>();
        }

        byte[] dados = File.ReadAllBytes(caminho);

        return MessagePackSerializer.Deserialize<List<LoginRegistro>>(dados);
    }

    public void SalvarLogins(List<LoginRegistro> logins)
    {
        string caminho = Path.Combine(pastaDados, "logins.msgpack");

        // Ordem fixa: assim todas as réplicas gravam o arquivo igual.
        byte[] dados = MessagePackSerializer.Serialize(
            logins.OrderBy(l => l.Ts).ThenBy(l => l.User, StringComparer.Ordinal).ToList()
        );

        File.WriteAllBytes(caminho, dados);
    }

    public HashSet<string> CarregarCanais()
    {
        string caminho = Path.Combine(pastaDados, "canais.msgpack");

        if (!File.Exists(caminho))
        {
            return new HashSet<string>();
        }

        byte[] dados = File.ReadAllBytes(caminho);

        List<string> canais =
            MessagePackSerializer.Deserialize<List<string>>(dados);

        return new HashSet<string>(canais);
    }

    public void SalvarCanais(HashSet<string> canais)
    {
        string caminho = Path.Combine(pastaDados, "canais.msgpack");

        byte[] dados = MessagePackSerializer.Serialize(
            canais.OrderBy(canal => canal, StringComparer.Ordinal).ToList()
        );

        File.WriteAllBytes(caminho, dados);
    }
    public List<Envelope> CarregarMensagens(string canal)
    {
        string caminho = Path.Combine(pastaDados, $"mensagens_{canal}.msgpack");
        if (!File.Exists(caminho)) return new List<Envelope>();
        return MessagePackSerializer.Deserialize<List<Envelope>>(File.ReadAllBytes(caminho));
    }

    public void SalvarMensagens(string canal, List<Envelope> mensagens)
    {
        string caminho = Path.Combine(pastaDados, $"mensagens_{canal}.msgpack");
        File.WriteAllBytes(caminho, MessagePackSerializer.Serialize(mensagens));
    }
}
