
export class ServidorInfo {
    name = "";
    rank = 0;
    address = "";

    constructor(campos: Partial<ServidorInfo> = {}) {
        Object.assign(this, campos);
    }

    paraMapa(): Record<string, unknown> {
        return {
            name: this.name,
            rank: this.rank,
            address: this.address
        };
    }

    static deMapa(dados: any): ServidorInfo {
        return new ServidorInfo({
            name: dados?.name ?? "",
            rank: Number(dados?.rank ?? 0),
            address: dados?.address ?? ""
        });
    }
}
