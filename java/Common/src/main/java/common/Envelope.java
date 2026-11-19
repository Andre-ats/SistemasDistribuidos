
package common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

// Mesmo formato do Envelope em C# e em Python: mapa MessagePack com as mesmas chaves,
// para que clientes e servidores das três linguagens troquem mensagens.
public class Envelope {

    public String type = "";
    public long ts;
    public long clock;
    public String from = "";
    public String user;
    public String channel;
    public String text;
    public String status;
    public String error;
    public List<String> channels;
    public List<Envelope> messages;
    public String id;
    public String server;
    public Integer rank;
    public String address;
    public Long time;
    public List<ServidorInfo> servers;
    public List<LoginRegistro> logins;

    // Métodos encadeados para montar a mensagem, no estilo do inicializador do C#:
    // new Envelope().type("login_reply").status("ok").user(usuario)

    public Envelope type(String valor) { type = valor; return this; }

    public Envelope ts(long valor) { ts = valor; return this; }

    public Envelope clock(long valor) { clock = valor; return this; }

    public Envelope from(String valor) { from = valor; return this; }

    public Envelope user(String valor) { user = valor; return this; }

    public Envelope channel(String valor) { channel = valor; return this; }

    public Envelope text(String valor) { text = valor; return this; }

    public Envelope status(String valor) { status = valor; return this; }

    public Envelope error(String valor) { error = valor; return this; }

    public Envelope channels(List<String> valor) { channels = valor; return this; }

    public Envelope messages(List<Envelope> valor) { messages = valor; return this; }

    public Envelope id(String valor) { id = valor; return this; }

    public Envelope server(String valor) { server = valor; return this; }

    public Envelope rank(Integer valor) { rank = valor; return this; }

    public Envelope address(String valor) { address = valor; return this; }

    public Envelope time(Long valor) { time = valor; return this; }

    public Envelope servers(List<ServidorInfo> valor) { servers = valor; return this; }

    public Envelope logins(List<LoginRegistro> valor) { logins = valor; return this; }

    public void escrever(MessagePacker packer) throws IOException {
        packer.packMapHeader(18);

        packer.packString("type");
        MsgPack.escreverTexto(packer, type);
        packer.packString("ts");
        packer.packLong(ts);
        packer.packString("clock");
        packer.packLong(clock);
        packer.packString("from");
        MsgPack.escreverTexto(packer, from);
        packer.packString("user");
        MsgPack.escreverTexto(packer, user);
        packer.packString("channel");
        MsgPack.escreverTexto(packer, channel);
        packer.packString("text");
        MsgPack.escreverTexto(packer, text);
        packer.packString("status");
        MsgPack.escreverTexto(packer, status);
        packer.packString("error");
        MsgPack.escreverTexto(packer, error);

        packer.packString("channels");
        if (channels == null) {
            packer.packNil();
        } else {
            packer.packArrayHeader(channels.size());

            for (String canal : channels) {
                packer.packString(canal);
            }
        }

        packer.packString("messages");
        if (messages == null) {
            packer.packNil();
        } else {
            packer.packArrayHeader(messages.size());

            for (Envelope item : messages) {
                item.escrever(packer);
            }
        }

        packer.packString("id");
        MsgPack.escreverTexto(packer, id);
        packer.packString("server");
        MsgPack.escreverTexto(packer, server);
        packer.packString("rank");
        MsgPack.escreverNumero(packer, rank != null ? rank.longValue() : null);
        packer.packString("address");
        MsgPack.escreverTexto(packer, address);
        packer.packString("time");
        MsgPack.escreverNumero(packer, time);

        packer.packString("servers");
        if (servers == null) {
            packer.packNil();
        } else {
            packer.packArrayHeader(servers.size());

            for (ServidorInfo item : servers) {
                item.escrever(packer);
            }
        }

        packer.packString("logins");
        if (logins == null) {
            packer.packNil();
        } else {
            packer.packArrayHeader(logins.size());

            for (LoginRegistro item : logins) {
                item.escrever(packer);
            }
        }
    }

    public static Envelope ler(Value valor) {
        Map<String, Value> campos = MsgPack.lerMapa(valor);

        Envelope mensagem = new Envelope();

        String tipo = MsgPack.lerTexto(campos, "type");
        Long ts = MsgPack.lerNumero(campos, "ts");
        Long clock = MsgPack.lerNumero(campos, "clock");
        String from = MsgPack.lerTexto(campos, "from");
        Long rank = MsgPack.lerNumero(campos, "rank");

        mensagem.type = tipo != null ? tipo : "";
        mensagem.ts = ts != null ? ts : 0;
        mensagem.clock = clock != null ? clock : 0;
        mensagem.from = from != null ? from : "";
        mensagem.user = MsgPack.lerTexto(campos, "user");
        mensagem.channel = MsgPack.lerTexto(campos, "channel");
        mensagem.text = MsgPack.lerTexto(campos, "text");
        mensagem.status = MsgPack.lerTexto(campos, "status");
        mensagem.error = MsgPack.lerTexto(campos, "error");
        mensagem.id = MsgPack.lerTexto(campos, "id");
        mensagem.server = MsgPack.lerTexto(campos, "server");
        mensagem.rank = rank != null ? rank.intValue() : null;
        mensagem.address = MsgPack.lerTexto(campos, "address");
        mensagem.time = MsgPack.lerNumero(campos, "time");

        List<Value> canais = MsgPack.lerLista(campos, "channels");
        if (canais != null) {
            mensagem.channels = new ArrayList<>();

            for (Value item : canais) {
                mensagem.channels.add(item.asStringValue().asString());
            }
        }

        List<Value> mensagens = MsgPack.lerLista(campos, "messages");
        if (mensagens != null) {
            mensagem.messages = new ArrayList<>();

            for (Value item : mensagens) {
                mensagem.messages.add(Envelope.ler(item));
            }
        }

        List<Value> servidores = MsgPack.lerLista(campos, "servers");
        if (servidores != null) {
            mensagem.servers = new ArrayList<>();

            for (Value item : servidores) {
                mensagem.servers.add(ServidorInfo.ler(item));
            }
        }

        List<Value> logins = MsgPack.lerLista(campos, "logins");
        if (logins != null) {
            mensagem.logins = new ArrayList<>();

            for (Value item : logins) {
                mensagem.logins.add(LoginRegistro.ler(item));
            }
        }

        return mensagem;
    }

    public byte[] serializar() {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            escrever(packer);
            return packer.toByteArray();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public static Envelope desserializar(byte[] dados) {
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(dados)) {
            return ler(unpacker.unpackValue());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
