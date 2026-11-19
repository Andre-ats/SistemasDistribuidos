
from dataclasses import dataclass
from typing import Optional

import msgpack

from Common.login_registro import LoginRegistro
from Common.servidor_info import ServidorInfo


# Mesmo formato do Envelope em C#: mapa MessagePack com as mesmas chaves,
# para que clientes e servidores das duas linguagens troquem mensagens.
@dataclass
class Envelope:
    type: str = ""
    ts: int = 0
    clock: int = 0
    from_: str = ""  # "from" é palavra reservada no Python
    user: Optional[str] = None
    channel: Optional[str] = None
    text: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    channels: Optional[list[str]] = None
    messages: Optional[list["Envelope"]] = None
    id: Optional[str] = None
    server: Optional[str] = None
    rank: Optional[int] = None
    address: Optional[str] = None
    time: Optional[int] = None
    servers: Optional[list[ServidorInfo]] = None
    logins: Optional[list[LoginRegistro]] = None

    def para_dict(self) -> dict:
        return {
            "type": self.type,
            "ts": self.ts,
            "clock": self.clock,
            "from": self.from_,
            "user": self.user,
            "channel": self.channel,
            "text": self.text,
            "status": self.status,
            "error": self.error,
            "channels": self.channels,
            "messages": [item.para_dict() for item in self.messages] if self.messages is not None else None,
            "id": self.id,
            "server": self.server,
            "rank": self.rank,
            "address": self.address,
            "time": self.time,
            "servers": [item.para_dict() for item in self.servers] if self.servers is not None else None,
            "logins": [item.para_dict() for item in self.logins] if self.logins is not None else None
        }

    @staticmethod
    def de_dict(dados: dict) -> "Envelope":
        mensagens = dados.get("messages")
        servidores = dados.get("servers")
        logins = dados.get("logins")

        return Envelope(
            type=dados.get("type") or "",
            ts=dados.get("ts") or 0,
            clock=dados.get("clock") or 0,
            from_=dados.get("from") or "",
            user=dados.get("user"),
            channel=dados.get("channel"),
            text=dados.get("text"),
            status=dados.get("status"),
            error=dados.get("error"),
            channels=dados.get("channels"),
            messages=[Envelope.de_dict(item) for item in mensagens] if mensagens is not None else None,
            id=dados.get("id"),
            server=dados.get("server"),
            rank=dados.get("rank"),
            address=dados.get("address"),
            time=dados.get("time"),
            servers=[ServidorInfo.de_dict(item) for item in servidores] if servidores is not None else None,
            logins=[LoginRegistro.de_dict(item) for item in logins] if logins is not None else None
        )

    def serializar(self) -> bytes:
        return msgpack.packb(self.para_dict(), use_bin_type=True)

    @staticmethod
    def desserializar(dados: bytes) -> "Envelope":
        return Envelope.de_dict(msgpack.unpackb(dados, raw=False))
