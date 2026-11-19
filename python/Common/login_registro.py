
from dataclasses import dataclass


@dataclass
class LoginRegistro:
    user: str = ""
    ts: int = 0

    def para_dict(self) -> dict:
        return {
            "user": self.user,
            "ts": self.ts
        }

    @staticmethod
    def de_dict(dados: dict) -> "LoginRegistro":
        return LoginRegistro(
            user=dados.get("user") or "",
            ts=dados.get("ts") or 0
        )
