
package common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

// Funções de apoio para ler e escrever os campos do MessagePack.
public final class MsgPack {

    private MsgPack() {
    }

    public static void escreverTexto(MessagePacker packer, String valor) throws IOException {
        if (valor == null) {
            packer.packNil();
        } else {
            packer.packString(valor);
        }
    }

    public static void escreverNumero(MessagePacker packer, Long valor) throws IOException {
        if (valor == null) {
            packer.packNil();
        } else {
            packer.packLong(valor);
        }
    }

    public static Map<String, Value> lerMapa(Value valor) {
        Map<String, Value> campos = new HashMap<>();

        for (Map.Entry<Value, Value> item : valor.asMapValue().map().entrySet()) {
            campos.put(item.getKey().asStringValue().asString(), item.getValue());
        }

        return campos;
    }

    public static String lerTexto(Map<String, Value> campos, String chave) {
        Value valor = campos.get(chave);

        if (valor == null || valor.isNilValue()) {
            return null;
        }

        return valor.asStringValue().asString();
    }

    public static Long lerNumero(Map<String, Value> campos, String chave) {
        Value valor = campos.get(chave);

        if (valor == null || valor.isNilValue()) {
            return null;
        }

        return valor.asIntegerValue().toLong();
    }

    public static List<Value> lerLista(Map<String, Value> campos, String chave) {
        Value valor = campos.get(chave);

        if (valor == null || valor.isNilValue()) {
            return null;
        }

        return new ArrayList<>(valor.asArrayValue().list());
    }
}
