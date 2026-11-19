
package server;

import common.Envelope;
import common.LoginRegistro;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;

public class Persistencia {

    private final Path pastaDados;

    public Persistencia(String servidorId) {
        String diretorioBase = System.getenv("DATA_DIR") != null
            ? System.getenv("DATA_DIR")
            : "dados";

        pastaDados = Path.of(diretorioBase, servidorId);

        try {
            Files.createDirectories(pastaDados);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private List<Value> ler(String nomeArquivo) {
        Path caminho = pastaDados.resolve(nomeArquivo);

        if (!Files.exists(caminho)) {
            return null;
        }

        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(Files.readAllBytes(caminho))) {
            return new ArrayList<>(unpacker.unpackValue().asArrayValue().list());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private void gravar(String nomeArquivo, MessageBufferPacker packer) {
        try {
            Files.write(pastaDados.resolve(nomeArquivo), packer.toByteArray());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<LoginRegistro> carregarLogins() {
        List<Value> dados = ler("logins.msgpack");
        List<LoginRegistro> logins = new ArrayList<>();

        if (dados == null) {
            return logins;
        }

        for (Value item : dados) {
            logins.add(LoginRegistro.ler(item));
        }

        return logins;
    }

    public void salvarLogins(List<LoginRegistro> logins) {
        // Ordem fixa: assim todas as réplicas gravam o arquivo igual.
        List<LoginRegistro> ordenados = new ArrayList<>(logins);
        ordenados.sort(Comparator.comparingLong((LoginRegistro l) -> l.ts).thenComparing(l -> l.user));

        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packArrayHeader(ordenados.size());

            for (LoginRegistro login : ordenados) {
                login.escrever(packer);
            }

            gravar("logins.msgpack", packer);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Set<String> carregarCanais() {
        List<Value> dados = ler("canais.msgpack");
        Set<String> canais = new HashSet<>();

        if (dados == null) {
            return canais;
        }

        for (Value item : dados) {
            canais.add(item.asStringValue().asString());
        }

        return canais;
    }

    public void salvarCanais(Set<String> canais) {
        List<String> ordenados = new ArrayList<>(canais);
        ordenados.sort(null);

        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packArrayHeader(ordenados.size());

            for (String canal : ordenados) {
                packer.packString(canal);
            }

            gravar("canais.msgpack", packer);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<Envelope> carregarMensagens(String canal) {
        List<Value> dados = ler("mensagens_" + canal + ".msgpack");
        List<Envelope> mensagens = new ArrayList<>();

        if (dados == null) {
            return mensagens;
        }

        for (Value item : dados) {
            mensagens.add(Envelope.ler(item));
        }

        return mensagens;
    }

    public void salvarMensagens(String canal, List<Envelope> mensagens) {
        try (MessageBufferPacker packer = MessagePack.newDefaultBufferPacker()) {
            packer.packArrayHeader(mensagens.size());

            for (Envelope mensagem : mensagens) {
                mensagem.escrever(packer);
            }

            gravar("mensagens_" + canal + ".msgpack", packer);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
