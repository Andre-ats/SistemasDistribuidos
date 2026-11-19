
package common;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class LogMensagem {

    // Mesmo formato do C# (":O"): 2026-11-18T06:38:57.5330000+00:00
    private static final DateTimeFormatter FORMATO_HORA =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'0000+00:00'").withZone(ZoneOffset.UTC);

    private LogMensagem() {
    }

    public static String formatarHora(long milissegundos) {
        return FORMATO_HORA.format(Instant.ofEpochMilli(milissegundos));
    }

    // O C# mostra campos nulos como texto vazio.
    private static String valor(Object valor) {
        return valor == null ? "" : valor.toString();
    }

    public static void exibir(String origem, String acao, Envelope mensagem) {
        System.out.println(formatar(origem, acao, mensagem));
    }

    public static String formatar(String origem, String acao, Envelope mensagem) {
        StringBuilder texto = new StringBuilder();

        texto.append('\n');
        texto.append("[").append(origem).append("] ").append(acao).append('\n');
        texto.append("  Type: ").append(valor(mensagem.type)).append('\n');
        texto.append("  Ts: ").append(mensagem.ts).append('\n');
        texto.append("  Clock: ").append(mensagem.clock).append('\n');
        texto.append("  From: ").append(valor(mensagem.from)).append('\n');
        texto.append("  User: ").append(valor(mensagem.user)).append('\n');
        texto.append("  Channel: ").append(valor(mensagem.channel)).append('\n');
        texto.append("  Text: ").append(valor(mensagem.text)).append('\n');
        texto.append("  Status: ").append(valor(mensagem.status)).append('\n');
        texto.append("  Error: ").append(valor(mensagem.error)).append('\n');
        texto.append("  Id: ").append(valor(mensagem.id)).append('\n');
        texto.append("  Server: ").append(valor(mensagem.server)).append('\n');
        texto.append("  Rank: ").append(valor(mensagem.rank)).append('\n');
        texto.append("  Address: ").append(valor(mensagem.address)).append('\n');
        texto.append("  Time: ").append(valor(mensagem.time)).append('\n');

        if (mensagem.channels != null) {
            texto.append("  Channels: [").append(String.join(", ", mensagem.channels)).append("]\n");
        } else {
            texto.append("  Channels: null\n");
        }

        if (mensagem.messages != null) {
            texto.append("  Messages:\n");

            for (Envelope item : mensagem.messages) {
                texto.append("    - Id=").append(valor(item.id))
                    .append(", User=").append(valor(item.user))
                    .append(", Channel=").append(valor(item.channel))
                    .append(", Text=").append(valor(item.text))
                    .append(", Ts=").append(item.ts)
                    .append(", Clock=").append(item.clock)
                    .append('\n');
            }
        } else {
            texto.append("  Messages: null\n");
        }

        if (mensagem.servers != null) {
            texto.append("  Servers:\n");

            for (ServidorInfo item : mensagem.servers) {
                texto.append("    - Name=").append(item.name)
                    .append(", Rank=").append(item.rank)
                    .append(", Address=").append(item.address)
                    .append('\n');
            }
        } else {
            texto.append("  Servers: null\n");
        }

        if (mensagem.logins != null) {
            texto.append("  Logins:\n");

            for (LoginRegistro item : mensagem.logins) {
                texto.append("    - User=").append(item.user)
                    .append(", Ts=").append(item.ts)
                    .append('\n');
            }
        } else {
            texto.append("  Logins: null\n");
        }

        return texto.toString();
    }
}
