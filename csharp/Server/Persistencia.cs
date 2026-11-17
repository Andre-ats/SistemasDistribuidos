
using Common;
using MessagePack;

namespace Server;

public class Persistencia
{
    private readonly string pastaDados;

    public Persistencia(string servidorId)
{
    pastaDados = Path.Combine(
        AppContext.BaseDirectory,
        "dados",
        servidorId
    );

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

        byte[] dados = MessagePackSerializer.Serialize(logins);

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

        byte[] dados = MessagePackSerializer.Serialize(canais.ToList());

        File.WriteAllBytes(caminho, dados);
    }
}
