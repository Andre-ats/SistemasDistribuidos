
package common;

import java.io.IOException;
import java.util.Map;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

public class ServidorInfo {

    public String name = "";
    public int rank;
    public String address = "";

    public void escrever(MessagePacker packer) throws IOException {
        packer.packMapHeader(3);
        packer.packString("name");
        MsgPack.escreverTexto(packer, name);
        packer.packString("rank");
        packer.packInt(rank);
        packer.packString("address");
        MsgPack.escreverTexto(packer, address);
    }

    public static ServidorInfo ler(Value valor) {
        Map<String, Value> campos = MsgPack.lerMapa(valor);

        ServidorInfo info = new ServidorInfo();

        String nome = MsgPack.lerTexto(campos, "name");
        Long rank = MsgPack.lerNumero(campos, "rank");
        String endereco = MsgPack.lerTexto(campos, "address");

        info.name = nome != null ? nome : "";
        info.rank = rank != null ? rank.intValue() : 0;
        info.address = endereco != null ? endereco : "";

        return info;
    }
}
