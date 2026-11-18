
import { encerrarComSinal, variavel } from "../Common/utilitarios";
import { Servidor } from "./servidor";

encerrarComSinal();

const porta = Number(variavel("SERVER_PORT", "6000"));

const portaPub = Number(variavel("PUB_PORT", "6001"));

const servidorId = variavel("SERVER_ID", "server1");

const servidor = new Servidor(porta, portaPub, servidorId);
servidor.iniciar();
