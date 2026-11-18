
import { randomUUID } from "node:crypto";
import * as readline from "node:readline";

import * as zmq from "zeromq";

import { Envelope } from "../Common/envelope";
import * as LogMensagem from "../Common/logMensagem";
import { agoraMs, encerrarComSinal, esperar, variavel } from "../Common/utilitarios";

encerrarComSinal();

const servidorHost = variavel("SERVER_HOST", "localhost");

const portaServidor = Number(variavel("SERVER_PORT", "6000"));

const proxyHost = variavel("PROXY_HOST", "localhost");

let relogioLogico = 0;

const canaisInscritos = new Set<string>();

// No modo interativo, enquanto o usuário digita, as publicações recebidas ficam
// guardadas e só são exibidas depois, para não misturar com o que está sendo digitado.
let pausarMensagens = false;
const mensagensPendentes: string[] = [];

function incrementarRelogio(): number {
    relogioLogico++;
    return relogioLogico;
}

function atualizarRelogio(recebido: number): number {
    relogioLogico = Math.max(relogioLogico, recebido);
    return relogioLogico;
}

// O Node roda em uma única thread, então as inscrições podem ser feitas direto
// no socket SUB, na hora em que o cliente entra ou sai de um canal.
const subscriber = new zmq.Subscriber();

subscriber.connect(`tcp://${proxyHost}:5558`);

function inscrever(canal: string): void {
    if (!canaisInscritos.has(canal)) {
        canaisInscritos.add(canal);
        subscriber.subscribe(canal);
    }
}

function desinscrever(canal: string): void {
    if (canaisInscritos.delete(canal)) {
        subscriber.unsubscribe(canal);
    }
}

function desinscreverTodos(): void {
    for (const canal of [...canaisInscritos]) {
        desinscrever(canal);
    }
}

async function receberMensagens(): Promise<void> {
    for await (const [canalBytes, dadosMensagem] of subscriber) {
        const canal = canalBytes.toString();

        if (!canaisInscritos.has(canal)) {
            continue;
        }

        const mensagemRecebida = Envelope.desserializar(dadosMensagem);
        const recebidoEm = agoraMs();

        const novo = atualizarRelogio(mensagemRecebida.clock);

        const texto: string[] = [];

        texto.push(LogMensagem.formatar("CLIENTE", "RECEBENDO PUB", mensagemRecebida));
        texto.push(`Relógio lógico do Cliente: ${novo}`);
        texto.push("");
        texto.push(`[${canal}] ${mensagemRecebida.user}: ${mensagemRecebida.text}`);
        texto.push(`Enviada em: ${LogMensagem.formatarHora(mensagemRecebida.ts)}`);
        texto.push(`Recebida em: ${LogMensagem.formatarHora(recebidoEm)}`);

        const saida = texto.join("\n") + "\n";

        if (pausarMensagens) {
            mensagensPendentes.push(saida);
            continue;
        }

        process.stdout.write(saida);
        process.stdout.write("Escolha uma opção: ");
    }
}

void receberMensagens();

// ===== Modo automático (bots) =====

const usuarioBot = variavel("CLIENT_USER", "cliente1");

async function enviarAutomatico(tipo: string, nomeCanal: string | null = null, texto: string | null = null): Promise<Envelope | null> {
    for (let tentativa = 1; tentativa <= 5; tentativa++) {
        const requisicaoSocket = new zmq.Request({ linger: 0, receiveTimeout: 3000 });

        try {
            requisicaoSocket.connect(`tcp://${servidorHost}:${portaServidor}`);

            const requisicao = new Envelope({
                type: tipo,
                ts: agoraMs(),
                clock: incrementarRelogio(),
                from: "Cliente",
                user: usuarioBot,
                channel: nomeCanal,
                text: texto
            });

            LogMensagem.exibir("CLIENTE", "ENVIANDO REQ", requisicao);

            await requisicaoSocket.send(requisicao.serializar());

            const [respostaBytes] = await requisicaoSocket.receive();

            const resposta = Envelope.desserializar(respostaBytes);

            LogMensagem.exibir("CLIENTE", "RECEBENDO REP", resposta);

            const novo = atualizarRelogio(resposta.clock);

            console.log(`[${usuarioBot}] Relógio lógico do Cliente: ${novo}`);

            return resposta;
        } catch (erro: any) {
            if (erro?.code === "EAGAIN") {
                console.log(`[${usuarioBot}] Sem resposta. Tentativa ${tentativa}/5`);
            } else {
                console.log(`[${usuarioBot}] Erro: ${erro?.message}`);
            }

            await esperar(2000);
        } finally {
            requisicaoSocket.close();
        }
    }

    return null;
}

// O servidor persiste o histórico, mas as sessões e participantes são voláteis.
// Após reinício, refazemos login e reinscrevemos o bot nos canais anteriores.
async function recuperarSessao(): Promise<boolean> {
    console.log(`[${usuarioBot}] RECUPERACAO: servidor perdeu a sessão; refazendo login...`);
    const novoLogin = await enviarAutomatico("login");
    if (novoLogin?.status !== "ok") {
        console.log(`[${usuarioBot}] RECUPERACAO: login não realizado: ${novoLogin?.error ?? "sem resposta"}`);
        return false;
    }

    const listagemAtual = await enviarAutomatico("list_channels");
    if (listagemAtual?.status !== "ok") {
        console.log(`[${usuarioBot}] RECUPERACAO: falha ao consultar canais.`);
        return false;
    }

    const existentes = new Set(listagemAtual.channels ?? []);
    const desejados = [...canaisInscritos];

    desinscreverTodos();
    for (const nome of desejados) {
        if (!existentes.has(nome)) continue;
        const entrada = await enviarAutomatico("join_channel", nome);
        if (entrada?.status === "ok" || entrada?.error === "Usuário já está no canal") {
            inscrever(nome);
            console.log(`[${usuarioBot}] RECUPERACAO: inscrito novamente em '${nome}'`);
        }
    }
    console.log(`[${usuarioBot}] RECUPERACAO CONCLUIDA: ${canaisInscritos.size} canais.`);
    return true;
}

async function enviarComRecuperacao(tipo: string, nomeCanal: string | null = null, texto: string | null = null): Promise<Envelope | null> {
    const resposta = await enviarAutomatico(tipo, nomeCanal, texto);
    // Não repetir uma publicação que apenas perdeu a resposta: isso poderia duplicá-la.
    // Só repetimos quando o servidor confirmou que a sessão não existe.
    if (resposta?.status === "error" && resposta.error === "Usuário não está registrado") {
        if (await recuperarSessao())
            return enviarAutomatico(tipo, nomeCanal, texto);
    }
    return resposta;
}

function sortear<T>(lista: T[]): T {
    return lista[Math.floor(Math.random() * lista.length)];
}

async function executarBot(): Promise<void> {
    console.log(`Modo automático iniciado: ${usuarioBot}`);

    // Em caso de erro, o login é repetido até ser aceito.
    let login = await enviarAutomatico("login");

    while (login?.status !== "ok") {
        console.log(`[${usuarioBot}] Login falhou: ${login?.error ?? "Servidor indisponível"}`);

        // O bot reiniciou, mas o servidor ainda guarda a sessão antiga: encerra e entra de novo.
        if (login?.error === "Usuário já está registrado") {
            await enviarAutomatico("logout");
        } else {
            await esperar(2000);
        }

        console.log(`[${usuarioBot}] Repetindo login...`);
        login = await enviarAutomatico("login");
    }

    console.log(`[${usuarioBot}] Login realizado!`);

    const textos = ["Bom dia!", "Tudo certo?", "Alguém por aqui?", "Mensagem de teste", "Olá pessoal!", "Mais uma mensagem", "Teste Pub/Sub"];

    while (true) {
        const listagem = await enviarComRecuperacao("list_channels");
        if (listagem?.status !== "ok") {
            console.log(`[${usuarioBot}] Não foi possível listar canais.`);
            await esperar(2000);
            continue;
        }

        const disponiveis = [...(listagem.channels ?? [])];
        if (disponiveis.length < 5) {
            const novoCanal = `canal_${usuarioBot}_${randomUUID().replace(/-/g, "").slice(0, 6)}`;
            const criacao = await enviarComRecuperacao("create_channel", novoCanal);
            if (criacao?.status === "ok") {
                disponiveis.push(novoCanal);
                console.log(`[${usuarioBot}] Canal '${novoCanal}' criado!`);
            }
        }

        if (canaisInscritos.size < 3) {
            const candidatos = disponiveis.filter(canal => !canaisInscritos.has(canal));
            if (candidatos.length > 0) {
                const escolhido = sortear(candidatos);
                const entrada = await enviarComRecuperacao("join_channel", escolhido);
                if (entrada?.status === "ok") {
                    inscrever(escolhido);
                    console.log(`[${usuarioBot}] Inscrito no canal '${escolhido}'`);
                }
            }
        }

        const inscritos = [...canaisInscritos];
        if (inscritos.length === 0) {
            await esperar(1000);
            continue;
        }

        const canalEscolhido = sortear(inscritos);
        for (let i = 0; i < 10; i++) {
            const texto = sortear(textos);
            const resposta = await enviarComRecuperacao("post", canalEscolhido, texto);
            console.log(`[${usuarioBot}] Publicação em ${canalEscolhido}: ${resposta?.status ?? "sem resposta"}`);
            await esperar(1000);
        }
    }
}

// ===== Modo interativo =====

// Linhas digitadas ficam numa fila; "lerLinha" pega a próxima (ou null quando a entrada acaba).
const linhasDigitadas: string[] = [];
const esperandoLinha: ((linha: string | null) => void)[] = [];
let entradaEncerrada = false;

function prepararEntrada(): void {
    const leitor = readline.createInterface({ input: process.stdin, terminal: false });

    leitor.on("line", linha => {
        const proximo = esperandoLinha.shift();

        if (proximo) {
            proximo(linha);
        } else {
            linhasDigitadas.push(linha);
        }
    });

    leitor.on("close", () => {
        entradaEncerrada = true;

        for (const proximo of esperandoLinha.splice(0)) {
            proximo(null);
        }
    });
}

function lerLinha(rotulo: string | null = null): Promise<string | null> {
    if (rotulo !== null) {
        process.stdout.write(rotulo);
    }

    const linha = linhasDigitadas.shift();

    if (linha !== undefined) {
        return Promise.resolve(linha);
    }

    if (entradaEncerrada) {
        return Promise.resolve(null);
    }

    return new Promise(resolver => esperandoLinha.push(resolver));
}

async function executarInterativo(): Promise<void> {
    prepararEntrada();

    const socket = new zmq.Request();

    socket.connect(`tcp://${servidorHost}:${portaServidor}`);

    console.log(`Conectado ao servidor na porta ${portaServidor}`);

    let usuarioLogado: string | null = null;

    while (true) {
        // Libera a exibição e mostra o que chegou enquanto o usuário digitava.
        pausarMensagens = false;

        if (mensagensPendentes.length > 0) {
            console.log();
            console.log(`===== Mensagens recebidas enquanto você digitava: ${mensagensPendentes.length} =====`);

            for (const pendente of mensagensPendentes.splice(0)) {
                process.stdout.write(pendente);
            }
        }

        console.log();
        console.log("===== BBS =====");

        if (usuarioLogado !== null) {
            console.log(`Usuário logado: ${usuarioLogado}`);
        }

        console.log("1 - Login");
        console.log("2 - Criar canal");
        console.log("3 - Listar canais");
        console.log("4 - Entrar em um canal");
        console.log("5 - Sair de um canal");
        console.log("6 - Enviar mensagem");
        console.log("7 - Consultar mensagens");
        console.log("8 - Logout");
        console.log("0 - Encerrar");

        let opcao = await lerLinha("Escolha uma opção: ");

        // A partir daqui o usuário vai digitar: segura as publicações recebidas.
        pausarMensagens = true;

        if (opcao === null) {
            console.log("Entrada encerrada. Finalizando cliente.");
            break;
        }

        opcao = opcao.trim();

        if (opcao === "0") {
            if (usuarioLogado !== null) {
                const logout = new Envelope({
                    type: "logout",
                    user: usuarioLogado,
                    from: "Cliente",
                    ts: agoraMs(),
                    clock: incrementarRelogio()
                });

                LogMensagem.exibir("CLIENTE", "ENVIANDO REQ", logout);
                await socket.send(logout.serializar());

                const [respostaLogout] = await socket.receive();
                const resultado = Envelope.desserializar(respostaLogout);

                LogMensagem.exibir("CLIENTE", "RECEBENDO REP", resultado);

                if (resultado.status === "ok") {
                    console.log("Logout realizado com sucesso.");
                } else {
                    console.log(`Erro ao realizar logout: ${resultado.error}`);
                }
            }

            console.log("Cliente encerrado.");
            break;
        }

        const mensagem = new Envelope({
            from: "Cliente",
            user: usuarioLogado
        });

        switch (opcao) {
            case "1":
                if (usuarioLogado !== null) {
                    console.log("Você já está logado.");
                    continue;
                }

                mensagem.type = "login";
                mensagem.user = await lerLinha("Nome do usuário: ");
                break;

            case "2":
                mensagem.type = "create_channel";
                mensagem.channel = await lerLinha("Nome do canal: ");
                break;

            case "3":
                mensagem.type = "list_channels";
                break;

            case "4":
                mensagem.type = "join_channel";
                mensagem.channel = await lerLinha("Nome do canal: ");
                break;

            case "5":
                mensagem.type = "leave_channel";
                mensagem.channel = await lerLinha("Nome do canal: ");
                break;

            case "6":
                mensagem.channel = await lerLinha("Nome do canal: ");
                mensagem.text = await lerLinha("Mensagem: ");
                mensagem.type = "post";
                break;

            case "7":
                mensagem.type = "get_messages";
                mensagem.channel = await lerLinha("Nome do canal: ");
                break;

            case "8":
                mensagem.type = "logout";
                break;

            default:
                console.log("Opção inválida.");
                continue;
        }

        if (usuarioLogado === null && opcao !== "1" && opcao !== "3") {
            console.log("Você precisa fazer login primeiro.");
            continue;
        }

        // Timestamp do envio (e não de quando o menu montou a mensagem).
        mensagem.ts = agoraMs();
        mensagem.clock = incrementarRelogio();

        LogMensagem.exibir("CLIENTE", "ENVIANDO REQ", mensagem);
        await socket.send(mensagem.serializar());

        const [respostaBytes] = await socket.receive();

        const resposta = Envelope.desserializar(respostaBytes);
        LogMensagem.exibir("CLIENTE", "RECEBENDO REP", resposta);

        const novo = atualizarRelogio(resposta.clock);

        console.log(`Relógio lógico do Cliente: ${novo}`);

        console.log(`Resposta do servidor: ${resposta.status}`);

        if (resposta.status === "error") {
            console.log(`Erro: ${resposta.error}`);
        }

        if (opcao === "1" && resposta.status === "ok") {
            usuarioLogado = resposta.user;
            console.log(`Login realizado: ${usuarioLogado}`);
        }

        if (opcao === "4" && resposta.status === "ok" && mensagem.channel !== null) {
            inscrever(mensagem.channel);
        }

        if (opcao === "5" && resposta.status === "ok" && mensagem.channel !== null) {
            desinscrever(mensagem.channel);
        }

        if (opcao === "8" && resposta.status === "ok") {
            usuarioLogado = null;
            desinscreverTodos();
            console.log("Logout realizado com sucesso.");
        }

        if (resposta.channels !== null) {
            console.log("Canais cadastrados:");

            for (const canal of resposta.channels) {
                console.log(`- ${canal}`);
            }
        }

        if (resposta.messages !== null) {
            console.log("Mensagens do canal:");

            if (resposta.messages.length === 0) {
                console.log("Nenhuma mensagem encontrada.");
            }

            for (const msg of resposta.messages) {
                console.log(`ID: ${msg.id}`);
                console.log(`[${msg.user}] ${msg.text}`);
            }
        }
    }

    process.exit(0);
}

const modoCliente = variavel("CLIENT_MODE", "interactive");

if (modoCliente.toLowerCase() === "auto") {
    void executarBot();
} else {
    void executarInterativo();
}
