
using MessagePack;
using System.Text;

namespace Common;

public static class LogMensagem
{
    public static void Exibir(string origem, string acao, Envelope mensagem)
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

        if (mensagem.Rank != null)
        {
            texto.AppendLine($"  Rank: {mensagem.Rank}");
        }

        if (mensagem.Port != null)
        {
            texto.AppendLine($"  Port: {mensagem.Port}");
        }

        if (mensagem.Time != null)
        {
            texto.AppendLine(
                $"  Time: {mensagem.Time} ({DateTimeOffset.FromUnixTimeMilliseconds(mensagem.Time.Value):HH:mm:ss.fff})"
            );
        }

        if (mensagem.Servers != null)
        {
            texto.AppendLine("  Servers:");

            foreach (ServidorInfo servidor in mensagem.Servers)
            {
                texto.AppendLine(
                    $"    - Name={servidor.Name}, Rank={servidor.Rank}, Port={servidor.Port}"
                );
            }
        }

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

        Console.WriteLine(texto.ToString());
    }
}
