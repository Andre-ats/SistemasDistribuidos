
using MessagePack;
using System.Text;

namespace Common;

public static class LogMensagem
{
    public static void Exibir(string origem, string acao, Envelope mensagem)
    {
        Console.WriteLine(Formatar(origem, acao, mensagem));
    }

    public static string Formatar(string origem, string acao, Envelope mensagem)
    {
        var texto = new StringBuilder();

        texto.AppendLine();
        texto.AppendLine($"[{origem}] {acao}");
        texto.AppendLine($"  Type: {mensagem.Type}");
        texto.AppendLine($"  Ts: {mensagem.Ts}");
        texto.AppendLine($"  Clock: {mensagem.Clock}");
        texto.AppendLine($"  From: {mensagem.From}");
        texto.AppendLine($"  User: {mensagem.User}");
        texto.AppendLine($"  Channel: {mensagem.Channel}");
        texto.AppendLine($"  Text: {mensagem.Text}");
        texto.AppendLine($"  Status: {mensagem.Status}");
        texto.AppendLine($"  Error: {mensagem.Error}");
        texto.AppendLine($"  Id: {mensagem.Id}");
        texto.AppendLine($"  Server: {mensagem.Server}");
        texto.AppendLine($"  Rank: {mensagem.Rank}");
        texto.AppendLine($"  Address: {mensagem.Address}");
        texto.AppendLine($"  Time: {mensagem.Time}");

        if (mensagem.Channels != null)
        {
            texto.AppendLine(
                $"  Channels: [{string.Join(", ", mensagem.Channels)}]"
            );
        }
        else
        {
            texto.AppendLine("  Channels: null");
        }

        if (mensagem.Messages != null)
        {
            texto.AppendLine("  Messages:");

            foreach (Envelope item in mensagem.Messages)
            {
                texto.AppendLine(
                    $"    - Id={item.Id}, User={item.User}, " +
                    $"Channel={item.Channel}, Text={item.Text}, " +
                    $"Ts={item.Ts}, Clock={item.Clock}"
                );
            }
        }
        else
        {
            texto.AppendLine("  Messages: null");
        }

        if (mensagem.Servers != null)
        {
            texto.AppendLine("  Servers:");

            foreach (ServidorInfo item in mensagem.Servers)
            {
                texto.AppendLine(
                    $"    - Name={item.Name}, Rank={item.Rank}, Address={item.Address}"
                );
            }
        }
        else
        {
            texto.AppendLine("  Servers: null");
        }

        if (mensagem.Logins != null)
        {
            texto.AppendLine("  Logins:");

            foreach (LoginRegistro item in mensagem.Logins)
            {
                texto.AppendLine($"    - User={item.User}, Ts={item.Ts}");
            }
        }
        else
        {
            texto.AppendLine("  Logins: null");
        }

        return texto.ToString();
    }
}
