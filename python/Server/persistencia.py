
import os

import msgpack

from Common.envelope import Envelope
from Common.login_registro import LoginRegistro


class Persistencia:
    def __init__(self, servidor_id: str):
        diretorio_base = os.environ.get("DATA_DIR") or os.path.join(os.path.dirname(__file__), "dados")

        self.pasta_dados = os.path.join(diretorio_base, servidor_id)

        os.makedirs(self.pasta_dados, exist_ok=True)

    def _ler(self, nome_arquivo: str):
        caminho = os.path.join(self.pasta_dados, nome_arquivo)

        if not os.path.exists(caminho):
            return None

        with open(caminho, "rb") as arquivo:
            return msgpack.unpackb(arquivo.read(), raw=False)

    def _gravar(self, nome_arquivo: str, conteudo) -> None:
        caminho = os.path.join(self.pasta_dados, nome_arquivo)

        with open(caminho, "wb") as arquivo:
            arquivo.write(msgpack.packb(conteudo, use_bin_type=True))

    def carregar_logins(self) -> list[LoginRegistro]:
        dados = self._ler("logins.msgpack")

        if dados is None:
            return []

        return [LoginRegistro.de_dict(item) for item in dados]

    def salvar_logins(self, logins: list[LoginRegistro]) -> None:
        # Ordem fixa: assim todas as réplicas gravam o arquivo igual.
        ordenados = sorted(logins, key=lambda login: (login.ts, login.user))

        self._gravar("logins.msgpack", [login.para_dict() for login in ordenados])

    def carregar_canais(self) -> set[str]:
        dados = self._ler("canais.msgpack")

        if dados is None:
            return set()

        return set(dados)

    def salvar_canais(self, canais: set[str]) -> None:
        self._gravar("canais.msgpack", sorted(canais))

    def carregar_mensagens(self, canal: str) -> list[Envelope]:
        dados = self._ler(f"mensagens_{canal}.msgpack")

        if dados is None:
            return []

        return [Envelope.de_dict(item) for item in dados]

    def salvar_mensagens(self, canal: str, mensagens: list[Envelope]) -> None:
        self._gravar(f"mensagens_{canal}.msgpack", [mensagem.para_dict() for mensagem in mensagens])
