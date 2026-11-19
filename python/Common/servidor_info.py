
from dataclasses import dataclass


@dataclass
class ServidorInfo:
    name: str = ""
    rank: int = 0
    address: str = ""

    def para_dict(self) -> dict:
        return {
            "name": self.name,
            "rank": self.rank,
            "address": self.address
        }

    @staticmethod
    def de_dict(dados: dict) -> "ServidorInfo":
        return ServidorInfo(
            name=dados.get("name") or "",
            rank=dados.get("rank") or 0,
            address=dados.get("address") or ""
        )
