
import * as zmq from "zeromq";

import { Envelope } from "../Common/envelope";
import * as LogMensagem from "../Common/logMensagem";
import { ServidorInfo } from "../Common/servidorInfo";
import { agoraMs } from "../Common/utilitarios";

export class ServicoReferencia {
    // Chaves em minúsculo: nomes de servidores ignoram maiúsculas/minúsculas.
    private readonly servidores = new Map<string, ServidorInfo>();
    private readonly ultimoHeartbeat = new Map<string, number>();
    private readonly disponiveis = new Set<string>();
    private proximoRank = 1;
    private relogioLogico = 0;

    constructor(private readonly porta: number, private readonly tempoLimite: number) {
    }

    async iniciar(): Promise<void> {
        const socket = new zmq.Reply();

        await socket.bind(`tcp://*:${this.porta}`);

        console.log(`Serviço de referência iniciado na porta ${this.porta}!`);
        console.log(`Servidores sem heartbeat por ${this.tempoLimite}s serão removidos.`);

        // Verifica os heartbeats mesmo sem requisições.
        setInterval(() => this.verificarHeartbeats(), 1000);

        for await (const [dados] of socket) {
            const mensagem = Envelope.desserializar(dados);
            LogMensagem.exibir("REFERENCIA", "RECEBENDO REQ", mensagem);

            this.relogioLogico = Math.max(this.relogioLogico, mensagem.clock);

            console.log(`Relógio lógico da referência: ${this.relogioLogico}`);

            let resposta: Envelope;

            switch (mensagem.type.toLowerCase()) {
                case "rank":
                    resposta = this.rank(mensagem);
                    break;

                case "list":
                    resposta = this.listar();
                    break;

                case "heartbeat":
                    resposta = this.heartbeat(mensagem);
                    break;

                default:
                    resposta = new Envelope({
                        type: "error",
                        status: "error",
                        error: "Tipo de mensagem inválido"
                    });
                    break;
            }

            resposta.ts = agoraMs();
            this.relogioLogico++;
            resposta.clock = this.relogioLogico;
            resposta.from = "Referencia";

            LogMensagem.exibir("REFERENCIA", "ENVIANDO REP", resposta);

            await socket.send(resposta.serializar());
        }
    }

    private rank(mensagem: Envelope): Envelope {
        const nome = mensagem.server ?? "";

        if (nome.trim() === "") {
            return new Envelope({
                type: "rank_reply",
                status: "error",
                error: "Nome do servidor inválido"
            });
        }

        const chave = nome.toLowerCase();

        // Sem repetição de nomes: um servidor que reinicia recebe o mesmo rank.
        let info = this.servidores.get(chave);

        if (info === undefined) {
            info = new ServidorInfo({ name: nome, rank: this.proximoRank });

            this.proximoRank++;
            this.servidores.set(chave, info);

            console.log(`Servidor cadastrado: ${nome} (rank ${info.rank})`);
        } else {
            console.log(`Servidor já cadastrado: ${nome} (rank ${info.rank})`);
        }

        info.address = mensagem.address ?? info.address;

        this.disponiveis.add(chave);
        this.ultimoHeartbeat.set(chave, agoraMs());

        return new Envelope({
            type: "rank_reply",
            status: "ok",
            server: nome,
            rank: info.rank
        });
    }

    private listar(): Envelope {
        return new Envelope({
            type: "list_reply",
            status: "ok",
            servers: [...this.servidores.entries()]
                .filter(([chave]) => this.disponiveis.has(chave))
                .map(([, info]) => info)
                .sort((a, b) => a.rank - b.rank)
        });
    }

    private heartbeat(mensagem: Envelope): Envelope {
        const nome = mensagem.server ?? "";
        const chave = nome.toLowerCase();

        if (!this.servidores.has(chave)) {
            return new Envelope({
                type: "heartbeat_reply",
                status: "error",
                error: "Servidor não registrado"
            });
        }

        if (!this.disponiveis.has(chave)) {
            this.disponiveis.add(chave);
            console.log(`Servidor voltou para a lista: ${nome}`);
        }

        this.ultimoHeartbeat.set(chave, agoraMs());

        console.log(`Heartbeat recebido: ${nome}`);

        // A hora correta agora é fornecida pelo coordenador eleito (Berkeley).
        return new Envelope({
            type: "heartbeat_reply",
            status: "ok",
            server: nome,
            text: "OK"
        });
    }

    private verificarHeartbeats(): void {
        const agora = agoraMs();

        for (const chave of [...this.disponiveis]) {
            if (agora - (this.ultimoHeartbeat.get(chave) ?? 0) > this.tempoLimite * 1000) {
                this.disponiveis.delete(chave);

                const nomes = [...this.disponiveis].map(item => this.servidores.get(item)!.name);

                console.log(`Servidor removido por falta de heartbeat: ${this.servidores.get(chave)!.name}`);
                console.log(`Servidores disponíveis: [${nomes.join(", ")}]`);
            }
        }
    }
}
