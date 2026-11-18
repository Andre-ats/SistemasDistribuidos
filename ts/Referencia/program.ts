
import { encerrarComSinal, variavel } from "../Common/utilitarios";
import { ServicoReferencia } from "./servicoReferencia";

encerrarComSinal();

const porta = Number(variavel("REFERENCE_PORT", "5000"));

const tempoLimite = Number(variavel("HEARTBEAT_TIMEOUT", "30"));

const referencia = new ServicoReferencia(porta, tempoLimite);
referencia.iniciar();
