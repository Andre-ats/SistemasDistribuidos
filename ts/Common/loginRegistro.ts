
export class LoginRegistro {
    user = "";
    ts = 0;

    constructor(campos: Partial<LoginRegistro> = {}) {
        Object.assign(this, campos);
    }

    paraMapa(): Record<string, unknown> {
        return {
            user: this.user,
            ts: this.ts
        };
    }

    static deMapa(dados: any): LoginRegistro {
        return new LoginRegistro({
            user: dados?.user ?? "",
            ts: Number(dados?.ts ?? 0)
        });
    }
}
