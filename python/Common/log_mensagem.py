
from Common.envelope import Envelope


def _valor(valor) -> str:
    # O C# mostra campos nulos como texto vazio.
    return "" if valor is None else str(valor)


def formatar(origem: str, acao: str, mensagem: Envelope) -> str:
    texto = []

    texto.append("")
    texto.append(f"[{origem}] {acao}")
    texto.append(f"  Type: {_valor(mensagem.type)}")
    texto.append(f"  Ts: {_valor(mensagem.ts)}")
    texto.append(f"  Clock: {_valor(mensagem.clock)}")
    texto.append(f"  From: {_valor(mensagem.from_)}")
    texto.append(f"  User: {_valor(mensagem.user)}")
    texto.append(f"  Channel: {_valor(mensagem.channel)}")
    texto.append(f"  Text: {_valor(mensagem.text)}")
    texto.append(f"  Status: {_valor(mensagem.status)}")
    texto.append(f"  Error: {_valor(mensagem.error)}")
    texto.append(f"  Id: {_valor(mensagem.id)}")
    texto.append(f"  Server: {_valor(mensagem.server)}")
    texto.append(f"  Rank: {_valor(mensagem.rank)}")
    texto.append(f"  Address: {_valor(mensagem.address)}")
    texto.append(f"  Time: {_valor(mensagem.time)}")

    if mensagem.channels is not None:
        texto.append(f"  Channels: [{', '.join(mensagem.channels)}]")
    else:
        texto.append("  Channels: null")

    if mensagem.messages is not None:
        texto.append("  Messages:")

        for item in mensagem.messages:
            texto.append(
                f"    - Id={_valor(item.id)}, User={_valor(item.user)}, "
                f"Channel={_valor(item.channel)}, Text={_valor(item.text)}, "
                f"Ts={item.ts}, Clock={item.clock}"
            )
    else:
        texto.append("  Messages: null")

    if mensagem.servers is not None:
        texto.append("  Servers:")

        for item in mensagem.servers:
            texto.append(f"    - Name={item.name}, Rank={item.rank}, Address={item.address}")
    else:
        texto.append("  Servers: null")

    if mensagem.logins is not None:
        texto.append("  Logins:")

        for item in mensagem.logins:
            texto.append(f"    - User={item.user}, Ts={item.ts}")
    else:
        texto.append("  Logins: null")

    return "\n".join(texto) + "\n"


def exibir(origem: str, acao: str, mensagem: Envelope) -> None:
    print(formatar(origem, acao, mensagem), flush=True)
