
import { randomUUID } from "node:crypto";

import * as zmq from "zeromq";

import { Envelope } from "../Common/envelope";
import * as LogMensagem from "../Common/logMensagem";
import { LoginRegistro } from "../Common/loginRegistro";
import { ServidorInfo } from "../Common/servidorInfo";
import { Trava, agoraMs, esperar, variavel } from "../Common/utilitarios";
import { Persistencia } from "./persistencia";

// O Node executa o código em uma única thread (com async/await). Por isso o estado
// (usuários, canais e mensagens) não precisa de "lock": cada operação roda inteira
// sem ser interrompida. Só o envio pelo publisher passa por uma trava, para as
// publicações saírem em ordem, uma de cada vez.
export class Servidor {
    // Usuários e participantes ignoram maiúsculas/minúsculas (guardados em minúsculo).
    private readonly usuarios = new Set<string>();
    private readonly canais = new Set<string>();
    private readonly participantes = new Map<string, Set<string>>();
    private readonly mensagens = new Map<string, Envelope[]>();
    private relogioLogico = 0;

    // Relógio físico: hora local = hora do sistema + desvio (em ms).
    private desvioRelogio = 0;
    private rank = 0;
    private mensagensClientes = 0;

    // Eleição (algoritmo do valentão): o servidor disponível de maior rank vira coordenador.
    private coordenador: string | null = null;
    private enderecoCoordenador: string | null = null;
    private eleicaoEmAndamento = false;
    private anunciosRecebidos = 0;

    // Berkeley: diferença (ms) de cada servidor em relação ao coordenador.
    private readonly diferencasRelogio = new Map<string, number>();

    private readonly travaPublisher = new Trava();

    private readonly persistencia: Persistencia;
    private readonly historicoLogins: LoginRegistro[] = [];

    private readonly portaSincronizacao: number;
    private readonly proxyHost = variavel("PROXY_HOST", "localhost");
    private readonly referenciaHost = variavel("REFERENCE_HOST", "localhost");
    private readonly referenciaPorta = Number(variavel("REFERENCE_PORT", "5000"));

    private publisher!: zmq.Publisher;

    constructor(
        private readonly porta: number,
        private readonly portaPub: number,
        private readonly servidorId: string
    ) {
        this.portaSincronizacao = Number(variavel("SYNC_PORT", String(porta + 2)));

        // Desvio inicial opcional para simular um relógio físico atrasado/adiantado.
        this.desvioRelogio = Number(variavel("CLOCK_DRIFT_MS", "0"));

        this.persistencia = new Persistencia(servidorId);
    }

    // ===== Relógios =====

    private horaAtual(): number {
        return agoraMs() + this.desvioRelogio;
    }

    private incrementarRelogio(): number {
        this.relogioLogico++;
        return this.relogioLogico;
    }

    private atualizarRelogio(recebido: number): number {
        this.relogioLogico = Math.max(this.relogioLogico, recebido);
        return this.relogioLogico;
    }

    async iniciar(): Promise<void> {
        this.historicoLogins.push(...this.persistencia.carregarLogins());

        for (const canal of this.persistencia.carregarCanais()) {
            this.canais.add(canal);
            this.participantes.set(canal, new Set());
            this.mensagens.set(canal, this.persistencia.carregarMensagens(canal));
        }

        // Regrava os arquivos na ordem padrão (dados antigos podem estar em outra ordem).
        for (const canal of this.canais) {
            this.ordenarMensagens(canal);
            this.persistencia.salvarMensagens(canal, this.mensagens.get(canal)!);
        }

        this.persistencia.salvarCanais(this.canais);
        this.persistencia.salvarLogins(this.historicoLogins);

        console.log(`Logins registrados: ${this.historicoLogins.length}`);
        console.log(`Canais carregados: ${this.canais.size}`);

        const socket = new zmq.Reply();

        await socket.bind(`tcp://*:${this.porta}`);

        this.publisher = new zmq.Publisher();

        this.publisher.connect(`tcp://${this.proxyHost}:5557`);

        console.log(`Servidor iniciado na porta ${this.porta}!`);
        console.log(`Publicações enviadas ao proxy ${this.proxyHost}:5557!`);
        console.log(`Relógio físico inicial: ${LogMensagem.formatarHora(this.horaAtual())} (desvio ${this.desvioRelogio} ms)`);

        await this.pedirRank();
        await this.listarServidores();

        void this.atenderServidores();
        void this.ouvirServidores();

        // O SUB de "replication" já está ativo; então busca o que foi perdido.
        await esperar(1000);
        await this.sincronizarDados(await this.listarServidores());

        // Também envia aos outros o que só este servidor tem (ex.: dados de antes de cair).
        // Espera um pouco para os outros servidores já estarem assinando "replication".
        setTimeout(() => this.divulgarEstado(), 10000);

        // Aguarda o SUB conectar ao proxy antes da primeira eleição.
        setTimeout(() => void this.eleicao(), 3000);

        for await (const [dados] of socket) {
            const mensagem = Envelope.desserializar(dados);
            LogMensagem.exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            console.log(`Relógio do servidor: ${this.atualizarRelogio(mensagem.clock)}`);

            console.log(`Mensagem recebida: ${mensagem.type}`);

            let resposta: Envelope;

            switch (mensagem.type.toLowerCase()) {
                case "login":
                    resposta = this.login(mensagem);
                    break;

                case "create_channel":
                    resposta = this.criarCanal(mensagem);
                    break;

                case "list_channels":
                    resposta = this.listarCanais();
                    break;

                case "join_channel":
                    resposta = this.entrarCanal(mensagem);
                    break;

                case "leave_channel":
                    resposta = this.sairCanal(mensagem);
                    break;

                case "post":
                    resposta = this.publicarMensagem(mensagem);
                    break;

                case "get_messages":
                    resposta = this.consultarMensagens(mensagem);
                    break;

                case "logout":
                    resposta = this.logout(mensagem);
                    break;

                default:
                    resposta = new Envelope({
                        type: "error",
                        status: "error",
                        error: "Tipo de mensagem inválido"
                    });
                    break;
            }

            resposta.ts = this.horaAtual();
            resposta.clock = this.incrementarRelogio();
            resposta.from = "Server";

            LogMensagem.exibir("SERVIDOR", "ENVIANDO REP", resposta);

            await socket.send(resposta.serializar());

            this.mensagensClientes++;

            // Heartbeat a cada 10 mensagens de clientes recebidas.
            if (this.mensagensClientes % 10 === 0) {
                await this.enviarHeartbeat();
            }

            // Sincronização do relógio físico com o coordenador a cada 15 mensagens.
            if (this.mensagensClientes % 15 === 0) {
                await this.sincronizarRelogio();
            }
        }
    }

    // ===== Serviço de referência =====

    private async enviarReferencia(requisicao: Envelope): Promise<Envelope | null> {
        for (let tentativa = 1; tentativa <= 3; tentativa++) {
            const requisicaoSocket = new zmq.Request({ linger: 0, receiveTimeout: 3000 });

            try {
                requisicaoSocket.connect(`tcp://${this.referenciaHost}:${this.referenciaPorta}`);

                requisicao.ts = this.horaAtual();
                requisicao.clock = this.incrementarRelogio();
                requisicao.from = this.servidorId;

                LogMensagem.exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

                await requisicaoSocket.send(requisicao.serializar());

                const [respostaBytes] = await requisicaoSocket.receive();

                const resposta = Envelope.desserializar(respostaBytes);

                LogMensagem.exibir("SERVIDOR", "RECEBENDO REP", resposta);

                console.log(`Relógio do servidor: ${this.atualizarRelogio(resposta.clock)}`);

                return resposta;
            } catch (erro: any) {
                if (erro?.code === "EAGAIN") {
                    console.log(`[${this.servidorId}] Referência sem resposta. Tentativa ${tentativa}/3`);
                } else {
                    console.log(`[${this.servidorId}] Erro com a referência: ${erro?.message}`);
                }

                await esperar(1000);
            } finally {
                requisicaoSocket.close();
            }
        }

        return null;
    }

    private async pedirRank(): Promise<void> {
        while (true) {
            const resposta = await this.enviarReferencia(new Envelope({
                type: "rank",
                server: this.servidorId,
                address: `${this.servidorId}:${this.portaSincronizacao}`
            }));

            if (resposta?.status === "ok" && resposta.rank !== null) {
                this.rank = resposta.rank;
                console.log(`[${this.servidorId}] Rank recebido: ${this.rank}`);
                return;
            }

            console.log(`[${this.servidorId}] Não foi possível obter o rank. Tentando novamente...`);
            await esperar(2000);
        }
    }

    private async listarServidores(): Promise<ServidorInfo[]> {
        const resposta = await this.enviarReferencia(new Envelope({
            type: "list"
        }));

        const lista = resposta?.servers ?? [];

        console.log(`[${this.servidorId}] Servidores disponíveis:`);

        for (const info of lista) {
            console.log(`- ${info.name} (rank ${info.rank}) em ${info.address}`);
        }

        return lista;
    }

    private async enviarHeartbeat(): Promise<void> {
        const resposta = await this.enviarReferencia(new Envelope({
            type: "heartbeat",
            server: this.servidorId
        }));

        if (resposta?.status === "error" && resposta.error === "Servidor não registrado") {
            await this.pedirRank();
            return;
        }

        if (resposta?.status !== "ok") {
            console.log(`[${this.servidorId}] Heartbeat sem resposta.`);
            return;
        }

        console.log(`[${this.servidorId}] Heartbeat confirmado: ${resposta.text}`);
    }

    private ajustarRelogioFisico(horaCorreta: number, enviadoEm: number, recebidoEm: number): void {
        // Compensa metade do tempo de ida e volta (algoritmo de Cristian).
        const horaEstimada = horaCorreta + Math.trunc((recebidoEm - enviadoEm) / 2);
        const antes = this.horaAtual();

        this.desvioRelogio = horaEstimada - recebidoEm;

        const depois = this.horaAtual();

        console.log(`[${this.servidorId}] Relógio físico ajustado:`);
        console.log(`  Antes: ${LogMensagem.formatarHora(antes)}`);
        console.log(`  Depois: ${LogMensagem.formatarHora(depois)}`);
        console.log(`  Correção: ${depois - antes} ms`);
    }

    // ===== Comunicação entre servidores =====

    // Atende as requisições de outros servidores (relógio, eleição e sync) em uma porta própria,
    // para não misturar com as requisições dos clientes.
    private async atenderServidores(): Promise<void> {
        const socketServidores = new zmq.Reply();

        await socketServidores.bind(`tcp://*:${this.portaSincronizacao}`);

        console.log(`[${this.servidorId}] Atendendo servidores na porta ${this.portaSincronizacao}!`);

        for await (const [dados] of socketServidores) {
            const mensagem = Envelope.desserializar(dados);
            LogMensagem.exibir("SERVIDOR", "RECEBENDO REQ", mensagem);

            console.log(`Relógio do servidor: ${this.atualizarRelogio(mensagem.clock)}`);

            let resposta: Envelope;

            switch (mensagem.type.toLowerCase()) {
                case "clock":
                    resposta = this.relogioCoordenador(mensagem);
                    break;

                case "sync":
                    resposta = this.estadoCompleto();
                    break;

                case "election":
                    resposta = new Envelope({
                        type: "election_reply",
                        status: "ok",
                        server: this.servidorId,
                        rank: this.rank,
                        text: "OK"
                    });

                    // Responde OK e assume a eleição a partir deste servidor.
                    setImmediate(() => void this.eleicao());
                    break;

                default:
                    resposta = new Envelope({
                        type: "error",
                        status: "error",
                        error: "Tipo de mensagem inválido"
                    });
                    break;
            }

            resposta.ts = this.horaAtual();
            resposta.clock = this.incrementarRelogio();
            resposta.from = this.servidorId;

            LogMensagem.exibir("SERVIDOR", "ENVIANDO REP", resposta);

            await socketServidores.send(resposta.serializar());
        }
    }

    // Recebe pelo tópico "servers" o nome do servidor eleito como coordenador
    // e pelo tópico "replication" as escritas feitas nos outros servidores.
    private async ouvirServidores(): Promise<void> {
        const subscriber = new zmq.Subscriber();

        subscriber.connect(`tcp://${this.proxyHost}:5558`);
        subscriber.subscribe("servers");
        subscriber.subscribe("replication");

        for await (const [topicoBytes, dados] of subscriber) {
            const topico = topicoBytes.toString();

            if (topico === "replication") {
                const operacao = Envelope.desserializar(dados);

                if (operacao.server === this.servidorId) {
                    continue;
                }

                LogMensagem.exibir("SERVIDOR", "RECEBENDO PUB", operacao);

                console.log(`Relógio do servidor: ${this.atualizarRelogio(operacao.clock)}`);

                this.aplicarReplicacao(operacao);
                continue;
            }

            if (topico !== "servers") {
                continue;
            }

            const anuncio = Envelope.desserializar(dados);
            LogMensagem.exibir("SERVIDOR", "RECEBENDO PUB", anuncio);

            console.log(`Relógio do servidor: ${this.atualizarRelogio(anuncio.clock)}`);

            if (anuncio.rank !== null && anuncio.rank < this.rank) {
                // Valentão: um servidor de rank menor se anunciou, então este servidor assume.
                console.log(`[${this.servidorId}] Coordenador ${anuncio.server} tem rank menor; iniciando eleição.`);
                void this.eleicao();
                continue;
            }

            this.coordenador = anuncio.server;
            this.enderecoCoordenador = anuncio.address;
            this.anunciosRecebidos++;

            this.diferencasRelogio.clear();

            console.log(`[${this.servidorId}] Coordenador atual: ${this.coordenador} (${this.enderecoCoordenador})`);
        }
    }

    private async enviarServidor(endereco: string, requisicao: Envelope): Promise<Envelope | null> {
        const requisicaoSocket = new zmq.Request({ linger: 0, receiveTimeout: 2000 });

        try {
            requisicaoSocket.connect(`tcp://${endereco}`);

            requisicao.ts = this.horaAtual();
            requisicao.clock = this.incrementarRelogio();
            requisicao.from = this.servidorId;
            requisicao.server ??= this.servidorId;

            LogMensagem.exibir("SERVIDOR", "ENVIANDO REQ", requisicao);

            await requisicaoSocket.send(requisicao.serializar());

            const [respostaBytes] = await requisicaoSocket.receive();

            const resposta = Envelope.desserializar(respostaBytes);

            LogMensagem.exibir("SERVIDOR", "RECEBENDO REP", resposta);

            console.log(`Relógio do servidor: ${this.atualizarRelogio(resposta.clock)}`);

            return resposta;
        } catch (erro: any) {
            if (erro?.code === "EAGAIN") {
                console.log(`[${this.servidorId}] Servidor ${endereco} sem resposta.`);
            } else {
                console.log(`[${this.servidorId}] Erro com o servidor ${endereco}: ${erro?.message}`);
            }

            return null;
        } finally {
            requisicaoSocket.close();
        }
    }

    // ===== Eleição =====

    private async eleicao(): Promise<void> {
        if (this.eleicaoEmAndamento) {
            console.log(`[${this.servidorId}] Eleição já em andamento.`);
            return;
        }

        this.eleicaoEmAndamento = true;

        try {
            while (true) {
                console.log(`[${this.servidorId}] Iniciando eleição (rank ${this.rank})`);

                const lista = await this.listarServidores();
                let recebeuOk = false;

                for (const info of lista) {
                    if (info.rank <= this.rank || info.name === this.servidorId) {
                        continue;
                    }

                    const resposta = await this.enviarServidor(info.address, new Envelope({
                        type: "election",
                        rank: this.rank
                    }));

                    if (resposta?.status === "ok") {
                        recebeuOk = true;
                    }
                }

                if (!recebeuOk) {
                    this.tornarCoordenador();
                    return;
                }

                // Um servidor de rank maior respondeu: aguarda o anúncio dele no tópico "servers".
                console.log(`[${this.servidorId}] Servidor de rank maior respondeu; aguardando anúncio do coordenador...`);

                const antes = this.anunciosRecebidos;

                for (let i = 0; i < 20 && this.anunciosRecebidos === antes; i++) {
                    await esperar(500);
                }

                if (this.anunciosRecebidos !== antes) {
                    return;
                }

                console.log(`[${this.servidorId}] Nenhum coordenador anunciado; repetindo eleição.`);
            }
        } finally {
            this.eleicaoEmAndamento = false;
        }
    }

    private tornarCoordenador(): void {
        this.coordenador = this.servidorId;
        this.enderecoCoordenador = `${this.servidorId}:${this.portaSincronizacao}`;

        const anuncio = new Envelope({
            type: "coordinator",
            server: this.servidorId,
            rank: this.rank,
            address: this.enderecoCoordenador,
            from: this.servidorId,
            ts: this.horaAtual(),
            clock: this.incrementarRelogio()
        });

        LogMensagem.exibir("SERVIDOR", "ENVIANDO PUB", anuncio);

        this.publicar("servers", anuncio);

        console.log(`[${this.servidorId}] Eleito como coordenador!`);
    }

    private async sincronizarRelogio(): Promise<void> {
        const atual = this.coordenador;
        const endereco = this.enderecoCoordenador;

        if (atual === null || endereco === null) {
            console.log(`[${this.servidorId}] Nenhum coordenador conhecido; iniciando eleição.`);
            void this.eleicao();
            return;
        }

        if (atual === this.servidorId) {
            console.log(`[${this.servidorId}] Este servidor é o coordenador; relógio usado como referência.`);
            return;
        }

        const enviadoEm = agoraMs();

        const resposta = await this.enviarServidor(endereco, new Envelope({
            type: "clock"
        }));

        const recebidoEm = agoraMs();

        if (resposta?.status !== "ok" || resposta.time === null) {
            console.log(`[${this.servidorId}] Coordenador ${atual} indisponível; iniciando eleição.`);
            this.coordenador = null;
            this.enderecoCoordenador = null;
            void this.eleicao();
            return;
        }

        console.log(`[${this.servidorId}] ${resposta.text}`);

        this.ajustarRelogioFisico(resposta.time, enviadoEm, recebidoEm);
    }

    // Berkeley no coordenador: guarda a diferença de cada servidor, calcula a média
    // (incluindo o próprio coordenador com diferença 0), ajusta o próprio relógio
    // pela média e devolve a hora correta para o servidor que pediu.
    private relogioCoordenador(mensagem: Envelope): Envelope {
        if (this.coordenador !== this.servidorId) {
            return new Envelope({
                type: "clock_reply",
                status: "error",
                error: "Servidor não é o coordenador"
            });
        }

        const nome = mensagem.server ?? mensagem.from;

        const agora = agoraMs() + this.desvioRelogio;

        this.diferencasRelogio.set(nome, mensagem.ts - agora);

        let soma = 0;

        for (const diferenca of this.diferencasRelogio.values()) {
            soma += diferenca;
        }

        // Divisão inteira truncando em direção ao zero, como no C#.
        const media = Math.trunc(soma / (this.diferencasRelogio.size + 1));

        this.desvioRelogio += media;

        for (const [chave, diferenca] of this.diferencasRelogio) {
            this.diferencasRelogio.set(chave, diferenca - media);
        }

        // Quem pediu passa a usar a hora correta, então sua diferença vira zero.
        this.diferencasRelogio.set(nome, 0);

        const horaCorreta = agoraMs() + this.desvioRelogio;
        const quantidade = this.diferencasRelogio.size + 1;

        console.log(`[${this.servidorId}] Berkeley: ${quantidade} relógios, média ${media} ms`);

        return new Envelope({
            type: "clock_reply",
            status: "ok",
            server: this.servidorId,
            time: horaCorreta,
            text: `Hora correta do coordenador ${this.servidorId} (média Berkeley ${media} ms)`
        });
    }

    // ===== Replicação =====
    // Replicação ativa: toda escrita aceita por um servidor (login, canal e mensagem)
    // é publicada no tópico "replication" e aplicada por todos os outros servidores.

    // Publica no proxy pela trava: as publicações saem na ordem em que foram pedidas.
    private publicar(topico: string, mensagem: Envelope): void {
        const dados = mensagem.serializar();

        void this.travaPublisher.executar(() => this.publisher.send([topico, dados]));
    }

    private replicar(operacao: Envelope): void {
        operacao.server = this.servidorId;
        operacao.from = this.servidorId;
        operacao.ts = this.horaAtual();
        operacao.clock = this.incrementarRelogio();

        LogMensagem.exibir("SERVIDOR", "ENVIANDO PUB", operacao);

        this.publicar("replication", operacao);
    }

    private aplicarReplicacao(operacao: Envelope): void {
        if (operacao.server === this.servidorId) {
            return;
        }

        switch (operacao.type) {
            case "replicate_login":
                this.aplicarLogin(new LoginRegistro({
                    user: operacao.user ?? "",
                    ts: operacao.time ?? operacao.ts
                }));
                break;

            case "replicate_channel":
                this.aplicarCanal(operacao.channel ?? "");
                break;

            case "replicate_state":
                for (const registro of operacao.logins ?? []) {
                    this.aplicarLogin(registro);
                }

                for (const canal of operacao.channels ?? []) {
                    this.aplicarCanal(canal);
                }

                for (const item of operacao.messages ?? []) {
                    this.aplicarMensagem(item);
                }
                break;

            case "replicate_post":
                for (const item of operacao.messages ?? []) {
                    this.aplicarMensagem(item);
                }
                break;

            default:
                console.log(`[${this.servidorId}] Réplica com tipo desconhecido: ${operacao.type}`);
                break;
        }
    }

    // As funções "aplicar" são idempotentes: aplicar duas vezes a mesma operação
    // não duplica dados (importante para a transferência de estado na entrada).

    private aplicarLogin(registro: LoginRegistro): boolean {
        if (this.historicoLogins.some(login => login.user === registro.user && login.ts === registro.ts)) {
            return false;
        }

        this.historicoLogins.push(registro);
        this.historicoLogins.sort((a, b) => a.ts - b.ts);

        this.persistencia.salvarLogins(this.historicoLogins);

        console.log(`[${this.servidorId}] Réplica aplicada: login de ${registro.user}`);

        return true;
    }

    private aplicarCanal(canal: string): boolean {
        if (canal.trim() === "" || this.canais.has(canal)) {
            return false;
        }

        this.canais.add(canal);
        this.participantes.set(canal, new Set());
        this.mensagens.set(canal, []);

        this.persistencia.salvarCanais(this.canais);
        this.persistencia.salvarMensagens(canal, this.mensagens.get(canal)!);

        console.log(`[${this.servidorId}] Réplica aplicada: canal ${canal}`);

        return true;
    }

    private aplicarMensagem(mensagem: Envelope): boolean {
        const canal = mensagem.channel ?? "";

        // Garante o canal caso a mensagem chegue antes da criação replicada.
        this.aplicarCanal(canal);

        const lista = this.mensagens.get(canal);

        if (lista === undefined || lista.some(item => item.id === mensagem.id)) {
            return false;
        }

        lista.push(mensagem);
        this.ordenarMensagens(canal);

        this.persistencia.salvarMensagens(canal, this.mensagens.get(canal)!);

        console.log(`[${this.servidorId}] Réplica aplicada: [${canal}] ${mensagem.user}: ${mensagem.text}`);

        return true;
    }

    // Ordem total igual em todas as réplicas: relógio lógico, depois timestamp e Id.
    private ordenarMensagens(canal: string): void {
        this.mensagens.get(canal)!.sort((a, b) => {
            if (a.clock !== b.clock) return a.clock - b.clock;
            if (a.ts !== b.ts) return a.ts - b.ts;

            const idA = a.id ?? "";
            const idB = b.id ?? "";

            return idA < idB ? -1 : idA > idB ? 1 : 0;
        });
    }

    // Transferência de estado: ao iniciar, o servidor pede a outro servidor disponível
    // tudo o que perdeu enquanto estava fora (ou tudo, se for um servidor novo).
    private async sincronizarDados(lista: ServidorInfo[]): Promise<void> {
        for (const info of lista) {
            if (info.name === this.servidorId) {
                continue;
            }

            const resposta = await this.enviarServidor(info.address, new Envelope({
                type: "sync"
            }));

            if (resposta?.status !== "ok") {
                continue;
            }

            let novos = 0;

            for (const registro of resposta.logins ?? []) {
                if (this.aplicarLogin(registro)) novos++;
            }

            for (const canal of resposta.channels ?? []) {
                if (this.aplicarCanal(canal)) novos++;
            }

            for (const item of resposta.messages ?? []) {
                if (this.aplicarMensagem(item)) novos++;
            }

            console.log(`[${this.servidorId}] Dados sincronizados com ${info.name}: ${novos} itens novos.`);
            return;
        }

        console.log(`[${this.servidorId}] Nenhum outro servidor disponível para sincronizar dados.`);
    }

    private divulgarEstado(): void {
        const estado = this.estadoCompleto();

        estado.type = "replicate_state";
        estado.status = null;

        console.log(`[${this.servidorId}] Enviando estado completo aos outros servidores.`);

        this.replicar(estado);
    }

    private estadoCompleto(): Envelope {
        return new Envelope({
            type: "sync_reply",
            status: "ok",
            server: this.servidorId,
            channels: [...this.canais].sort(),
            messages: [...this.mensagens.values()].flat(),
            logins: [...this.historicoLogins]
        });
    }

    // ===== Operações dos clientes =====

    private login(mensagem: Envelope): Envelope {
        const usuario = mensagem.user ?? "";

        if (!/^[A-Za-z0-9_-]{3,32}$/.test(usuario)) {
            return new Envelope({
                type: "login_reply",
                status: "error",
                error: "Nome de usuário inválido"
            });
        }

        if (this.usuarios.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "login_reply",
                status: "error",
                error: "Usuário já está registrado"
            });
        }

        this.usuarios.add(usuario.toLowerCase());

        const registro = new LoginRegistro({
            user: usuario,
            ts: this.horaAtual()
        });

        this.historicoLogins.push(registro);

        this.persistencia.salvarLogins(this.historicoLogins);

        this.replicar(new Envelope({
            type: "replicate_login",
            user: usuario,
            time: registro.ts
        }));

        console.log(`Usuário conectado: ${usuario}`);

        return new Envelope({
            type: "login_reply",
            status: "ok",
            user: usuario
        });
    }

    private criarCanal(mensagem: Envelope): Envelope {
        const canal = mensagem.channel ?? "";

        if (!/^[a-z0-9_-]{3,24}$/.test(canal)) {
            return new Envelope({
                type: "create_channel_reply",
                status: "error",
                error: "Nome de canal inválido"
            });
        }

        if (canal === "servers" || canal === "replication") {
            return new Envelope({
                type: "create_channel_reply",
                status: "error",
                error: "Nome de canal reservado"
            });
        }

        if (this.canais.has(canal)) {
            return new Envelope({
                type: "create_channel_reply",
                status: "error",
                error: "Canal já existe"
            });
        }

        this.canais.add(canal);
        this.participantes.set(canal, new Set());
        this.mensagens.set(canal, []);

        this.persistencia.salvarCanais(this.canais);
        this.persistencia.salvarMensagens(canal, this.mensagens.get(canal)!);

        this.replicar(new Envelope({
            type: "replicate_channel",
            channel: canal
        }));

        console.log(`Canal criado: ${canal}`);

        return new Envelope({
            type: "create_channel_reply",
            status: "ok",
            channel: canal
        });
    }

    private listarCanais(): Envelope {
        return new Envelope({
            type: "list_channels_reply",
            status: "ok",
            channels: [...this.canais].sort()
        });
    }

    private entrarCanal(mensagem: Envelope): Envelope {
        const usuario = mensagem.user ?? "";
        const canal = mensagem.channel ?? "";

        if (!this.usuarios.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "join_channel_reply",
                status: "error",
                error: "Usuário não está registrado"
            });
        }

        if (!this.canais.has(canal)) {
            return new Envelope({
                type: "join_channel_reply",
                status: "error",
                error: "Canal não encontrado"
            });
        }

        if (this.participantes.get(canal)!.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "join_channel_reply",
                status: "error",
                error: "Usuário já está no canal"
            });
        }

        this.participantes.get(canal)!.add(usuario.toLowerCase());

        console.log(`Usuário ${usuario} entrou no canal ${canal}`);

        return new Envelope({
            type: "join_channel_reply",
            status: "ok",
            user: usuario,
            channel: canal
        });
    }

    private sairCanal(mensagem: Envelope): Envelope {
        const usuario = mensagem.user ?? "";
        const canal = mensagem.channel ?? "";

        if (!this.usuarios.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "leave_channel_reply",
                status: "error",
                error: "Usuário não está registrado"
            });
        }

        if (!this.canais.has(canal)) {
            return new Envelope({
                type: "leave_channel_reply",
                status: "error",
                error: "Canal não encontrado"
            });
        }

        if (!this.participantes.get(canal)!.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "leave_channel_reply",
                status: "error",
                error: "Usuário não está no canal"
            });
        }

        this.participantes.get(canal)!.delete(usuario.toLowerCase());

        console.log(`Usuário ${usuario} saiu do canal ${canal}`);

        return new Envelope({
            type: "leave_channel_reply",
            status: "ok",
            user: usuario,
            channel: canal
        });
    }

    private publicarMensagem(mensagem: Envelope): Envelope {
        const usuario = mensagem.user ?? "";
        const canal = mensagem.channel ?? "";
        const texto = mensagem.text ?? "";

        if (!this.usuarios.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "post_reply",
                status: "error",
                error: "Usuário não está registrado"
            });
        }

        if (!this.canais.has(canal)) {
            return new Envelope({
                type: "post_reply",
                status: "error",
                error: "Canal não encontrado"
            });
        }

        if (!this.participantes.get(canal)!.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "post_reply",
                status: "error",
                error: "Usuário não participa do canal"
            });
        }

        if (texto.trim() === "") {
            return new Envelope({
                type: "post_reply",
                status: "error",
                error: "Mensagem não pode estar vazia"
            });
        }

        const novaMensagem = new Envelope({
            id: randomUUID(),
            type: "post",
            user: usuario,
            channel: canal,
            text: texto,
            from: usuario,
            ts: this.horaAtual(),
            clock: this.incrementarRelogio()
        });

        this.mensagens.get(canal)!.push(novaMensagem);
        this.ordenarMensagens(canal);
        this.persistencia.salvarMensagens(canal, this.mensagens.get(canal)!);
        LogMensagem.exibir("SERVIDOR", "ENVIANDO PUB", novaMensagem);

        this.publicar(canal, novaMensagem);

        console.log(`[${canal}] ${usuario}: ${texto}`);

        this.replicar(new Envelope({
            type: "replicate_post",
            channel: canal,
            messages: [novaMensagem]
        }));

        return new Envelope({
            type: "post_reply",
            status: "ok",
            channel: canal
        });
    }

    private consultarMensagens(mensagem: Envelope): Envelope {
        const usuario = mensagem.user ?? "";
        const canal = mensagem.channel ?? "";

        if (!this.usuarios.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "get_messages_reply",
                status: "error",
                error: "Usuário não está registrado"
            });
        }

        if (!this.canais.has(canal)) {
            return new Envelope({
                type: "get_messages_reply",
                status: "error",
                error: "Canal não encontrado"
            });
        }

        if (!this.participantes.get(canal)!.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "get_messages_reply",
                status: "error",
                error: "Usuário não participa do canal"
            });
        }

        console.log(`Usuário ${usuario} consultou as mensagens do canal ${canal}`);

        return new Envelope({
            type: "get_messages_reply",
            status: "ok",
            channel: canal,
            messages: [...this.mensagens.get(canal)!]
        });
    }

    private logout(mensagem: Envelope): Envelope {
        const usuario = mensagem.user ?? "";

        if (!this.usuarios.has(usuario.toLowerCase())) {
            return new Envelope({
                type: "logout_reply",
                status: "error",
                error: "Usuário não está registrado"
            });
        }

        this.usuarios.delete(usuario.toLowerCase());

        for (const usuariosCanal of this.participantes.values()) {
            usuariosCanal.delete(usuario.toLowerCase());
        }

        console.log(`Usuário desconectado: ${usuario}`);

        return new Envelope({
            type: "logout_reply",
            status: "ok",
            user: usuario
        });
    }
}
