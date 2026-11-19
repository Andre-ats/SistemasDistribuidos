
package server;

public class Program {

    private static String variavel(String nome, String padrao) {
        String valor = System.getenv(nome);
        return valor != null ? valor : padrao;
    }

    public static void main(String[] args) {
        int porta = Integer.parseInt(variavel("SERVER_PORT", "6000"));

        int portaPub = Integer.parseInt(variavel("PUB_PORT", "6001"));

        String servidorId = variavel("SERVER_ID", "server1");

        Servidor servidor = new Servidor(porta, portaPub, servidorId);
        servidor.iniciar();
    }
}
