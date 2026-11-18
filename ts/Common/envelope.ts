
import { decode, encode } from "@msgpack/msgpack";

import { LoginRegistro } from "./loginRegistro";
import { ServidorInfo } from "./servidorInfo";

// Mesmo formato do Envelope em C#, Python e Java: mapa MessagePack com as mesmas chaves,
// para que clientes e servidores de todas as linguagens troquem mensagens.
//
// Montagem no estilo do inicializador do C#:
// new Envelope({ type: "login_reply", status: "ok", user: usuario })
export class Envelope {
    type = "";
    ts = 0;
    clock = 0;
    from = "";
    user: string | null = null;
    channel: string | null = null;
    text: string | null = null;
    status: string | null = null;
    error: string | null = null;
    channels: string[] | null = null;
    messages: Envelope[] | null = null;
    id: string | null = null;
    server: string | null = null;
    rank: number | null = null;
    address: string | null = null;
    time: number | null = null;
    servers: ServidorInfo[] | null = null;
    logins: LoginRegistro[] | null = null;

    constructor(campos: Partial<Envelope> = {}) {
        Object.assign(this, campos);
    }

    paraMapa(): Record<string, unknown> {
        return {
            type: this.type,
            ts: this.ts,
            clock: this.clock,
            from: this.from,
            user: this.user,
            channel: this.channel,
            text: this.text,
            status: this.status,
            error: this.error,
            channels: this.channels,
            messages: this.messages?.map(item => item.paraMapa()) ?? null,
            id: this.id,
            server: this.server,
            rank: this.rank,
            address: this.address,
            time: this.time,
            servers: this.servers?.map(item => item.paraMapa()) ?? null,
            logins: this.logins?.map(item => item.paraMapa()) ?? null
        };
    }

    static deMapa(dados: any): Envelope {
        return new Envelope({
            type: dados?.type ?? "",
            ts: Number(dados?.ts ?? 0),
            clock: Number(dados?.clock ?? 0),
            from: dados?.from ?? "",
            user: dados?.user ?? null,
            channel: dados?.channel ?? null,
            text: dados?.text ?? null,
            status: dados?.status ?? null,
            error: dados?.error ?? null,
            channels: dados?.channels ?? null,
            messages: dados?.messages ? dados.messages.map((item: any) => Envelope.deMapa(item)) : null,
            id: dados?.id ?? null,
            server: dados?.server ?? null,
            rank: dados?.rank != null ? Number(dados.rank) : null,
            address: dados?.address ?? null,
            time: dados?.time != null ? Number(dados.time) : null,
            servers: dados?.servers ? dados.servers.map((item: any) => ServidorInfo.deMapa(item)) : null,
            logins: dados?.logins ? dados.logins.map((item: any) => LoginRegistro.deMapa(item)) : null
        });
    }

    serializar(): Buffer {
        return Buffer.from(encode(this.paraMapa()));
    }

    static desserializar(dados: Uint8Array): Envelope {
        return Envelope.deMapa(decode(dados));
    }
}
