
// Funções pequenas usadas por todos os programas.

export function variavel(nome: string, padrao: string): string {
    return process.env[nome] ?? padrao;
}

export function esperar(milissegundos: number): Promise<void> {
    return new Promise(resolver => setTimeout(resolver, milissegundos));
}

export function agoraMs(): number {
    return Date.now();
}

// O Node rodando como processo principal do container ignora o sinal de parada
// do Docker; assim o container para na hora com "docker compose stop".
export function encerrarComSinal(): void {
    process.on("SIGTERM", () => process.exit(0));
    process.on("SIGINT", () => process.exit(0));
}

// Trava simples para uma operação assíncrona por vez (como o "lock" do C#).
export class Trava {
    private fila: Promise<unknown> = Promise.resolve();

    executar<T>(tarefa: () => Promise<T>): Promise<T> {
        const resultado = this.fila.then(tarefa, tarefa);
        this.fila = resultado.catch(() => undefined);
        return resultado;
    }
}
