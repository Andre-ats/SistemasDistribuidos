
package referencia;

public class Program {

    private static String variavel(String nome, String padrao) {
        String valor = System.getenv(nome);
        return valor != null ? valor : padrao;
    }

    public static void main(String[] args) {
        int porta = Integer.parseInt(variavel("REFERENCE_PORT", "5000"));

        int tempoLimite = Integer.parseInt(variavel("HEARTBEAT_TIMEOUT", "30"));

        ServicoReferencia referencia = new ServicoReferencia(porta, tempoLimite);
        referencia.iniciar();
    }
}
