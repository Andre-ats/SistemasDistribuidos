
import { Envelope } from "./envelope";

// O C# mostra campos nulos como texto vazio.
function valor(campo: unknown): string {
    return campo === null || campo === undefined ? "" : String(campo);
}

// Mesmo formato do C# (":O"): 2026-11-18T06:38:57.5330000+00:00
export function formatarHora(milissegundos: number): string {
    return new Date(milissegundos).toISOString().slice(0, 23) + "0000+00:00";
}

export function formatar(origem: string, acao: string, mensagem: Envelope): string {
    const texto: string[] = [];

    texto.push("");
    texto.push(`[${origem}] ${acao}`);
    texto.push(`  Type: ${valor(mensagem.type)}`);
    texto.push(`  Ts: ${valor(mensagem.ts)}`);
    texto.push(`  Clock: ${valor(mensagem.clock)}`);
    texto.push(`  From: ${valor(mensagem.from)}`);
    texto.push(`  User: ${valor(mensagem.user)}`);
    texto.push(`  Channel: ${valor(mensagem.channel)}`);
    texto.push(`  Text: ${valor(mensagem.text)}`);
    texto.push(`  Status: ${valor(mensagem.status)}`);
    texto.push(`  Error: ${valor(mensagem.error)}`);
    texto.push(`  Id: ${valor(mensagem.id)}`);
    texto.push(`  Server: ${valor(mensagem.server)}`);
    texto.push(`  Rank: ${valor(mensagem.rank)}`);
    texto.push(`  Address: ${valor(mensagem.address)}`);
    texto.push(`  Time: ${valor(mensagem.time)}`);

    if (mensagem.channels !== null) {
        texto.push(`  Channels: [${mensagem.channels.join(", ")}]`);
    } else {
        texto.push("  Channels: null");
    }

    if (mensagem.messages !== null) {
        texto.push("  Messages:");

        for (const item of mensagem.messages) {
            texto.push(
                `    - Id=${valor(item.id)}, User=${valor(item.user)}, ` +
                `Channel=${valor(item.channel)}, Text=${valor(item.text)}, ` +
                `Ts=${item.ts}, Clock=${item.clock}`
            );
        }
    } else {
        texto.push("  Messages: null");
    }

    if (mensagem.servers !== null) {
        texto.push("  Servers:");

        for (const item of mensagem.servers) {
            texto.push(`    - Name=${item.name}, Rank=${item.rank}, Address=${item.address}`);
        }
    } else {
        texto.push("  Servers: null");
    }

    if (mensagem.logins !== null) {
        texto.push("  Logins:");

        for (const item of mensagem.logins) {
            texto.push(`    - User=${item.user}, Ts=${item.ts}`);
        }
    } else {
        texto.push("  Logins: null");
    }

    return texto.join("\n") + "\n";
}

export function exibir(origem: string, acao: string, mensagem: Envelope): void {
    console.log(formatar(origem, acao, mensagem));
}
