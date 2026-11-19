# BBS distribuído (Python / pyzmq)

Projeto da disciplina **Sistemas Distribuídos — FEI**. Sistema de troca de mensagens inspirado no BBS/IRC: bots fazem login, criam e listam canais, entram em canais e publicam mensagens; os servidores persistem tudo em disco, usam relógio lógico, elegem um coordenador para sincronizar o relógio físico (Berkeley) e **replicam todos os dados para todos os servidores**.

Todas as partes do projeto (1 a 5) estão nesta mesma pasta: o código abaixo já é a versão completa, com tudo o que cada parte pediu.

Esta é a versão em **Python** do mesmo sistema da pasta `csharp/`: mesma estrutura, mesmas mensagens e mesmo formato de dados. Por isso servidores e clientes em Python e em C# conversam entre si (ver seção 10).

## 1. Escolhas do projeto

- **Linguagem:** Python 3.12 para cliente, servidor, serviço de referência e proxy Pub/Sub.
- **Comunicação:** ZeroMQ com a biblioteca `pyzmq`. REQ/REP para requisições e PUB/SUB pelo proxy para publicações.
- **Serialização:** MessagePack (binário) com a biblioteca `msgpack`. Toda mensagem é um `Envelope` com `ts` (timestamp do envio) e `clock` (relógio lógico de quem envia), com as mesmas chaves do `Envelope` da versão C#.
- **Persistência:** arquivos `.msgpack` por servidor em `/data/<servidor>` (volume Docker): `logins.msgpack`, `canais.msgpack` e `mensagens_<canal>.msgpack`. Cada servidor tem os seus próprios arquivos; nenhum arquivo é compartilhado.

## 2. Requisitos

- Docker Engine e Docker Compose v2 (`docker compose`).
- Nenhuma porta precisa estar livre no computador: os containers conversam só pela rede do Compose. Assim esta versão pode rodar ao mesmo tempo que a versão C#.

## 3. Estrutura

```text
python/
├── Cliente/program.py              # Bots (modo automático) e cliente interativo
├── Server/
│   ├── program.py                  # Lê as variáveis de ambiente e inicia o servidor
│   ├── servidor.py                 # Requisições, relógios, eleição e réplica
│   └── persistencia.py             # Arquivos .msgpack
├── Referencia/
│   ├── program.py
│   └── servico_referencia.py       # Rank, lista de servidores e heartbeat
├── Common/                         # Envelope, ServidorInfo, LoginRegistro e log_mensagem
├── Proxy/proxy.py                  # Proxy XSUB/XPUB
├── Dockerfile.client
├── Dockerfile.server
├── Dockerfile.reference
├── Dockerfile.proxy
├── docker-compose.yaml
└── README.md
```

O Compose inicia **18 containers**: `server1` a `server8`, `client1` a `client8`, `proxy` e `referencia`.

## 4. Como executar

```bash
docker compose up --build -d
```

```bash
docker compose logs -f --tail=50
```

## 5. O que cada parte adicionou

- **Parte 1:** `login`, `create_channel` e `list_channels` por REQ/REP, com persistência de logins (com timestamp) e canais.
- **Parte 2:** `join_channel`, `leave_channel`, `post`, `get_messages` e `logout`; publicações pelo proxy (porta 5557 entrada / 5558 saída) no tópico com o nome do canal; bots automáticos.
- **Parte 3:** relógio lógico em todas as mensagens; serviço de referência com `rank`, `list` e `heartbeat` (a cada 10 mensagens de clientes).
- **Parte 4:** eleição do coordenador (valentão, maior rank vence), anúncio no tópico `servers` e sincronização do relógio físico com Berkeley a cada 15 mensagens. A referência deixa de mandar a hora no heartbeat.
- **Parte 5:** replicação de logins, canais e mensagens em todos os servidores (seção 6).

### Relógio lógico

Clientes, servidores e a referência mantêm um contador `relogioLogico`. Antes de enviar qualquer mensagem o contador é incrementado e vai no campo `clock`; ao receber, o processo fica com `max(relógio local, clock recebido)`.

### Serviço de referência

Processo `referencia` (porta **5000**) que conversa só com os servidores:

| Requisição (`type`) | Envio | Resposta |
|---|---|---|
| `rank` | nome (`server`) e endereço (`address`) | `rank` do servidor |
| `list` | — | `servers`: nome, rank e endereço dos disponíveis |
| `heartbeat` | nome (`server`) | `OK` |

O servidor manda heartbeat a cada 10 mensagens de clientes; quem fica `HEARTBEAT_TIMEOUT` segundos (padrão 30) sem mandar sai da lista. Como o `server2` não tem cliente ligado a ele, ele sai da lista depois de 30 s, o que demonstra essa atualização.

### Eleição e Berkeley

Cada servidor tem uma segunda porta REP (`SYNC_PORT`) só para outros servidores (`clock`, `election` e `sync`).

- **Eleição (valentão):** quando o servidor sobe ou o coordenador não responde, ele manda `election` aos servidores de rank maior; quem recebe responde `OK` e assume a eleição. Se ninguém de rank maior responde, o servidor vira coordenador e publica seu nome no tópico `servers`.
- **Berkeley:** a cada 15 mensagens o servidor manda `clock` ao coordenador com sua hora. O coordenador guarda a diferença de cada servidor, calcula a média (contando ele mesmo com 0), ajusta o próprio relógio e responde a hora correta. Adaptação: no Berkeley clássico o coordenador consulta todos de uma vez; aqui, para seguir o REQ/REP do enunciado, ele acumula as diferenças a cada pedido.
- Os desvios simulados (`CLOCK_DRIFT_MS`: `server3` +4000 ms, `server5` −3000 ms, `server7` +2500 ms) aparecem como correções grandes no começo e depois ficam perto de 0 ms.

## 6. Replicação dos dados

### O problema

Cada servidor só guardava o que recebia dos seus próprios clientes. Se um servidor parasse, uma parte do histórico ficava indisponível, e um cliente que pedisse o histórico recebia só a parte guardada no servidor dele. O objetivo é que **todos os servidores tenham todos os dados**.

### Método escolhido: replicação ativa (escrita replicada) com multicast pelo Pub/Sub

Entre os métodos vistos na teoria (primário-backup e escrita replicada/replicação ativa), foi escolhida a **replicação ativa**: não existe servidor primário; **qualquer servidor aceita escritas** e **envia cada operação de escrita a todas as réplicas**, que a aplicam na mesma ordem.

Por que resolve o problema do projeto:

- Toda escrita aceita (login, canal criado, mensagem publicada) chega a todos os servidores, então **todos ficam com o histórico completo** e qualquer um responde `get_messages` com todas as mensagens.
- Se um servidor cai, os outros continuam com tudo, e os clientes dos outros servidores não percebem nada.
- Não depende de um primário: a queda de qualquer servidor não bloqueia as escritas (no primário-backup, a queda do primário pararia as escritas até uma nova eleição).

### Como foi implementado

1. **Envio das operações.** Quando um servidor aceita uma escrita, ele grava no próprio disco, responde o cliente e publica a operação no tópico **`replication`** do proxy (o mesmo proxy XSUB/XPUB já usado pelos clientes):

   | `type` | Conteúdo |
   |---|---|
   | `replicate_login` | `user` e `time` (timestamp original do login) |
   | `replicate_channel` | `channel` |
   | `replicate_post` | `channel` e `messages` com a mensagem original (Id, usuário, texto, `ts` e `clock`) |

   Todas levam `server` (quem originou), `ts` e `clock`, como qualquer outra mensagem do sistema.

2. **Aplicação nas réplicas.** Todo servidor assina `replication` (na mesma thread que já assina `servers`), ignora as próprias publicações e aplica as dos outros, gravando no disco. A aplicação é **idempotente**: login com o mesmo usuário e timestamp, canal já existente ou mensagem com `Id` já existente são ignorados. Assim, receber a mesma operação duas vezes não duplica dados.

3. **Ordem igual em todas as réplicas.** As mensagens de cada canal são ordenadas por **relógio lógico (`clock`)**, depois `ts` e por último `Id`. Como o `Id` é único, o desempate é igual em todos os servidores e todos chegam à **mesma ordem total**, que respeita a causalidade capturada pelo relógio lógico (Lamport). Os relógios lógicos e físicos sincronizados das Partes 3 e 4 são usados aqui.

4. **Concorrência.** Requisições de clientes, réplicas recebidas e pedidos de outros servidores rodam em threads diferentes; o estado (canais, mensagens e logins) é protegido por uma trava (`travaEstado`).

### Mudanças feitas no método para funcionar no projeto

- **Transferência de estado na entrada.** O Pub/Sub do ZeroMQ não guarda mensagens: um servidor que estava parado (ou que é novo) perde as operações publicadas nesse período. Por isso, ao iniciar, o servidor **primeiro assina `replication`** e depois pede a outro servidor disponível (lista da referência) uma cópia completa com a requisição **`sync`** na porta entre servidores (`SYNC_PORT`). A resposta traz `channels`, `messages` e `logins`, que são aplicados pelas mesmas funções idempotentes. Isso permite **adicionar e remover servidores sem parar o serviço**.
- **Divulgação do próprio estado.** Depois de sincronizar, o servidor espera 10 s e publica em `replication` uma operação `replicate_state` com tudo o que ele tem. Assim, dados que só ele possuía (por exemplo, gravados enquanto os outros estavam fora, ou de execuções antigas) também chegam a todos. Com o `sync` (puxar) e o `replicate_state` (empurrar), a troca fica nos dois sentidos.
- **Arquivos gravados na mesma ordem.** Canais são gravados em ordem alfabética, logins por horário e mensagens pela ordem total acima, para que todas as réplicas gravem arquivos idênticos. Ao iniciar, o servidor regrava os arquivos carregados nessa ordem (dados antigos podiam estar em outra), e todo canal tem o seu arquivo de mensagens, mesmo vazio.
- **Consistência eventual.** Como a réplica é enviada depois da resposta ao cliente, durante alguns milissegundos uma réplica pode ainda não ter a última mensagem. Depois que a publicação chega, todas ficam iguais.
- **Canais com o mesmo nome criados ao mesmo tempo** em servidores diferentes são aceitos nos dois e se juntam em um só canal nas réplicas (a operação é idempotente).
- **Sessões não são replicadas.** Quem está logado e quem participa de cada canal são dados de sessão, válidos só no servidor em que o cliente está conectado; o que é replicado são os dados persistidos (logins com timestamp, canais e mensagens). Os bots refazem a sessão sozinhos se o servidor reiniciar.

## 7. Como testar

### Teste A — réplica em todos os servidores

Deixe os bots rodarem por cerca de 1 minuto (cada servidor só divulga o próprio estado 10 s depois de iniciar), pare os clientes e compare os arquivos de todos os servidores:

```bash
docker compose stop client1 client2 client3 client4 client5 client6 client7 client8
```

```bash
for s in 1 2 3 4 5 6 7 8; do echo "server$s: $(docker compose exec -T server$s sh -c "cd /data/server$s && md5sum * | md5sum")"; done
```

Esperado: **o mesmo hash para os 8 servidores**, inclusive o `server2`, que não tem cliente ligado a ele.

### Teste B — réplicas sendo aplicadas

```bash
docker compose logs server2 | grep 'Réplica aplicada' | tail -20
```

### Teste C — servidor parado e recuperado

```bash
docker compose start client1 client2 client3 client4 client5 client6 client7 client8
docker compose stop server3
```

Aguarde uns 30 s com os bots publicando e volte o servidor:

```bash
docker compose start server3
docker compose logs server3 | grep 'Dados sincronizados'
```

Esperado: `Dados sincronizados com serverX: N itens novos.` com N > 0. Repetindo o Teste A, o `server3` fica com o mesmo hash dos outros.

### Teste D — mensagens de todos os clientes

```bash
docker compose logs client3 | grep -A5 'RECEBENDO PUB' | grep 'From:' | sort | uniq -c
```

Esperado: publicações de bots de **vários servidores** diferentes, porque os canais criados em um servidor são replicados e os bots de outros servidores entram neles.

### Teste E — referência, heartbeat e relógio lógico

```bash
docker compose logs referencia | grep -E 'cadastrado|removido|voltou'
docker compose logs client1 | grep -E 'Clock:|Relógio lógico' | tail -20
```

### Teste F — eleição e Berkeley

```bash
docker compose logs | grep 'Eleito como coordenador'
docker compose logs | grep -E 'Berkeley:|Correção' | tail -20
```

Derrube o coordenador (exemplo `server4`) e veja o próximo maior rank assumir:

```bash
docker compose stop server4
docker compose logs --since=1m | grep -E 'indisponível|Iniciando eleição|Eleito como coordenador'
```

### Teste G — cliente interativo (opcional)

```bash
docker compose run --rm -it -e CLIENT_MODE=interactive -e SERVER_HOST=server1 -e SERVER_PORT=6000 client1
```

Use um nome diferente dos bots (por exemplo `teste_manual`). Enquanto você digita (depois de escolher uma opção), as publicações recebidas ficam guardadas e aparecem juntas quando a operação termina, em `Mensagens recebidas enquanto você digitava`, para não misturar com o texto digitado.

## 8. Variáveis de ambiente

| Variável | Serviço | Finalidade | Exemplo |
|---|---|---|---|
| `SERVER_ID` | servidor | Nome do servidor e subpasta de dados | `server1` |
| `SERVER_PORT` | servidor | Porta REQ/REP dos clientes | `6000` |
| `SYNC_PORT` | servidor | Porta REQ/REP entre servidores (relógio, eleição e `sync`) | `6002` |
| `PROXY_HOST` | servidor/cliente | Host do proxy | `proxy` |
| `DATA_DIR` | servidor | Diretório-base da persistência | `/data` |
| `REFERENCE_HOST` | servidor | Host do serviço de referência | `referencia` |
| `REFERENCE_PORT` | servidor/referência | Porta da referência | `5000` |
| `HEARTBEAT_TIMEOUT` | referência | Segundos sem heartbeat até remover o servidor | `30` |
| `CLOCK_DRIFT_MS` | servidor | Desvio inicial simulado do relógio físico | `4000` |
| `SERVER_HOST` | cliente | Servidor acessado pelo cliente | `server1` |
| `CLIENT_MODE` | cliente | Modo do cliente | `auto` ou `interactive` |
| `CLIENT_USER` | cliente | Usuário do bot | `cliente1` |

## 9. Parar e limpar

```bash
docker compose down
```

**Atenção:** `docker compose down -v` **apaga os volumes nomeados** e os históricos persistidos.

## 10. Conversando com a versão C#

O `Envelope` (Common/envelope.py) usa as mesmas chaves MessagePack do `Envelope.cs` (`type`, `ts`, `clock`, `from`, `user`, `channel`, `text`, `status`, `error`, `channels`, `messages`, `id`, `server`, `rank`, `address`, `time`, `servers`, `logins`), os mesmos tipos de requisição e os mesmos tópicos (`servers`, `replication` e o nome de cada canal). Os arquivos `.msgpack` também têm o mesmo formato.

Foi testado um Compose misto com 4 servidores e 4 clientes em C# e 4 servidores e 4 clientes em Python, usando o mesmo proxy e a mesma referência:

- clientes C# recebem as publicações dos bots Python e vice-versa;
- um servidor C# foi eleito coordenador e sincronizou o relógio dos servidores Python (Berkeley com 8 relógios);
- os 8 servidores terminaram com os arquivos de mensagens, logins e canais idênticos.

Para isso basta que os serviços das duas linguagens estejam no mesmo Compose, apontando para o mesmo `proxy` e a mesma `referencia`, com nomes de serviço diferentes.
