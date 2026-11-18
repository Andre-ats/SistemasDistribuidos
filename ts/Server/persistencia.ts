
import * as fs from "node:fs";
import * as path from "node:path";

import { decode, encode } from "@msgpack/msgpack";

import { Envelope } from "../Common/envelope";
import { LoginRegistro } from "../Common/loginRegistro";

export class Persistencia {
    private readonly pastaDados: string;

    constructor(servidorId: string) {
        const diretorioBase = process.env.DATA_DIR ?? path.join(__dirname, "dados");

        this.pastaDados = path.join(diretorioBase, servidorId);

        fs.mkdirSync(this.pastaDados, { recursive: true });
    }

    private ler(nomeArquivo: string): any[] | null {
        const caminho = path.join(this.pastaDados, nomeArquivo);

        if (!fs.existsSync(caminho)) {
            return null;
        }

        return decode(fs.readFileSync(caminho)) as any[];
    }

    private gravar(nomeArquivo: string, conteudo: unknown): void {
        const caminho = path.join(this.pastaDados, nomeArquivo);

        fs.writeFileSync(caminho, encode(conteudo));
    }

    carregarLogins(): LoginRegistro[] {
        const dados = this.ler("logins.msgpack");

        if (dados === null) {
            return [];
        }

        return dados.map(item => LoginRegistro.deMapa(item));
    }

    salvarLogins(logins: LoginRegistro[]): void {
        // Ordem fixa: assim todas as réplicas gravam o arquivo igual.
        const ordenados = [...logins].sort((a, b) =>
            a.ts !== b.ts ? a.ts - b.ts : (a.user < b.user ? -1 : a.user > b.user ? 1 : 0)
        );

        this.gravar("logins.msgpack", ordenados.map(login => login.paraMapa()));
    }

    carregarCanais(): Set<string> {
        const dados = this.ler("canais.msgpack");

        if (dados === null) {
            return new Set();
        }

        return new Set(dados as string[]);
    }

    salvarCanais(canais: Set<string>): void {
        this.gravar("canais.msgpack", [...canais].sort());
    }

    carregarMensagens(canal: string): Envelope[] {
        const dados = this.ler(`mensagens_${canal}.msgpack`);

        if (dados === null) {
            return [];
        }

        return dados.map(item => Envelope.deMapa(item));
    }

    salvarMensagens(canal: string, mensagens: Envelope[]): void {
        this.gravar(`mensagens_${canal}.msgpack`, mensagens.map(mensagem => mensagem.paraMapa()));
    }
}
