using Referencia;

int porta = int.Parse(
    Environment.GetEnvironmentVariable("REFERENCE_PORT") ?? "5000"
);

int tempoLimite = int.Parse(
    Environment.GetEnvironmentVariable("HEARTBEAT_TIMEOUT") ?? "30"
);

ServicoReferencia referencia = new ServicoReferencia(porta, tempoLimite);
referencia.Iniciar();
