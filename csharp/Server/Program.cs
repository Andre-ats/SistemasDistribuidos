using Server;

int porta = int.Parse(
    Environment.GetEnvironmentVariable("SERVER_PORT") ?? "6000"
);

int portaPub = int.Parse(
    Environment.GetEnvironmentVariable("PUB_PORT") ?? "6001"
);

string servidorId =
    Environment.GetEnvironmentVariable("SERVER_ID") ?? "server1";

Servidor servidor = new Servidor(porta, portaPub, servidorId);
servidor.Iniciar();
