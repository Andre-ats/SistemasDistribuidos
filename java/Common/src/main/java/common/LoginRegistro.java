
package common;

import java.io.IOException;
import java.util.Map;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.Value;

public class LoginRegistro {

    public String user = "";
    public long ts;

    public LoginRegistro() {
    }

    public LoginRegistro(String user, long ts) {
        this.user = user;
        this.ts = ts;
    }

    public void escrever(MessagePacker packer) throws IOException {
        packer.packMapHeader(2);
        packer.packString("user");
        MsgPack.escreverTexto(packer, user);
        packer.packString("ts");
        packer.packLong(ts);
    }

    public static LoginRegistro ler(Value valor) {
        Map<String, Value> campos = MsgPack.lerMapa(valor);

        String user = MsgPack.lerTexto(campos, "user");
        Long ts = MsgPack.lerNumero(campos, "ts");

        return new LoginRegistro(user != null ? user : "", ts != null ? ts : 0);
    }
}
