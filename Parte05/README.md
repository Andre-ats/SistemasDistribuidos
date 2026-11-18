# BBS distribuído — Parte 5 (versão final, C# / NetMQ)

Projeto da disciplina **Sistemas Distribuídos — FEI**. Implementa operações de um BBS por **REQ/REP** e distribuição de mensagens por **Publisher/Subscriber (PUB/SUB)**, com proxy ZeroMQ, bots automáticos e persistência de dados.

Nesta parte foram adicionados o **relógio lógico** em todas as mensagens (clientes, servidores e referência), o **serviço de referência** (rank, lista de servidores e heartbeat) e a **atualização do relógio físico** dos servidores usando a hora da referência.

Na Parte 4 a hora deixou de vir da referência: os servidores **elegem um coordenador** (algoritmo do valentão / bully, usando o rank) e sincronizam o relógio com ele usando o **algoritmo de Berkeley**.

Na Parte 5 todos os servidores passam a ter **todos os canais e todas as mensagens** (replicação). A explicação do método está na seção [Replicação](#replicação-parte-5).

## 1. Requisitos

- Docker Engine e Docker Compose v2 (`docker compose`).
- Portas **5555, 5557, 5558, 6000, 6001, 7000 e 7001** livres no computador para o mapeamento atual.
- Não é necessário instalar .NET ou Python localmente para executar com Docker.

## 2. Estrutura

```text
csharp/
├── Cliente/              # Cliente C#: modo automático e interativo
├── Server/               # Servidor C#: requisições, canais e persistência
├── Common/               # Envelope, LoginRegistro, ServidorInfo e logs compartilhados
├── Referencia/           # Serviço de referência: rank, lista, heartbeat e hora
├── Proxy/proxy.py        # Proxy XSUB/XPUB
├── Broker/broker.py      # Broker ROUTER/DEALER (round-robin entre servidores)
├── Dockerfile.client
├── Dockerfile.server
├── Dockerfile.proxy
├── Dockerfile.broker
├── Dockerfile.referencia
├── docker-compose.yaml
└── README.md
```

O Compose inicia **19 containers**: `server1` a `server8`, `client1` a `client8`, `proxy`, `broker` e `referencia`. Os clientes não falam direto com um servidor: mandam as requisições para o **broker** (porta 5555), que distribui entre os 8 servidores em **round-robin**. Com a replicação, não importa para qual servidor o pedido vai: todos têm os mesmos dados.

## 3. Como executar

Abra um terminal **dentro da pasta `csharp/`**, onde está `docker-compose.yaml`:

```bash
docker compose up --build -d
```

Verifique se os containers subiram:

```bash
docker compose ps -a
```

Acompanhe as mensagens de dois clientes:

```bash
docker compose logs -f client1 client2
```

Para interromper a visualização dos logs, pressione **Ctrl+C**. Isso não para os containers.

Para acompanhar todos os serviços:

```bash
docker compose logs -f --tail=50
```

## 4. Como funciona

### REQ/REP e broker

O cliente envia uma requisição ao **broker** (ROUTER, porta 5555) e aguarda a resposta. O broker repassa pelo DEALER (porta 5556) para um dos servidores em round-robin. O socket REP de cada servidor faz `Connect` no broker (pedidos dos clientes) e continua com `Bind` na própria porta (pedidos dos outros servidores). Operações implementadas: `login`, `create_channel`, `list_channels`, `join_channel`, `leave_channel`, `post`, `get_messages` e `logout`.

### PUB/SUB

1. O cliente envia `post` ao servidor via REQ.
2. O servidor valida e registra a mensagem, publica no proxy via PUB e responde via REP.
3. O proxy recebe as publicações na porta **5557** e as distribui aos clientes SUB pela porta **5558**.
4. Os clientes inscritos no tópico do canal recebem a publicação sem fazer outra requisição.

O cliente usa **`Subscribe(nomeDoCanal)`** e **`Unsubscribe(nomeDoCanal)`**. A thread do SubscriberSocket sincroniza os tópicos efetivamente assinados com a lista de canais do cliente. Isso evita manipular o mesmo socket NetMQ em threads diferentes.

### Bots automáticos

Os oito clientes do Compose usam `CLIENT_MODE=auto`. Os bots fazem login, consultam canais, criam um canal quando há menos de cinco, entram em canais enquanto tiverem menos de três inscrições e enviam lotes de **dez mensagens**, com aproximadamente **um segundo** entre envios. A execução continua em ciclo.

### Relógio lógico

Clientes, servidores e referência mantêm um contador (`relogioLogico`) que começa em 0:

1. antes de enviar qualquer mensagem o contador é incrementado e enviado no campo `clock`;
2. ao receber uma mensagem o processo usa `max(contador local, clock recebido)`.

Todas as mensagens têm `ts` (timestamp) e `clock`.

### Serviço de referência

Processo `referencia` (porta **5559**, REQ/REP). Só conversa com servidores, nunca com clientes.

| Requisição (`type`) | Campos enviados | Resposta |
|---|---|---|
| `rank` | `from` = nome do servidor, `port` | `rank_reply` com `rank` |
| `list` | — | `list_reply` com `servers` (nome, rank e porta dos disponíveis) |
| `heartbeat` | `from` = nome do servidor | `heartbeat_reply` com `status: ok` (sem hora a partir da Parte 4) |

- Ranks são dados por ordem de cadastro (1, 2, 3...). Se o mesmo nome pedir rank de novo recebe o mesmo valor, sem repetição.
- O servidor envia `heartbeat` a cada **10 mensagens de clientes**. Se ficar mais de `HEARTBEAT_TIMEOUT` segundos (30) sem heartbeat ele sai da lista. Se voltar a mandar heartbeat, volta para a lista.
- Se a referência reiniciar e não conhecer o servidor, o heartbeat volta com erro e o servidor pede o rank de novo.

### Relógio físico

O servidor usa `Agora() = hora do sistema + deslocamentoRelogio`. Na Parte 3 o ajuste vinha do campo `time` da resposta do heartbeat; na Parte 4 vem do coordenador (seção abaixo). Como todos os containers usam o mesmo relógio do computador, o Compose coloca um desvio inicial (`CLOCK_DRIFT_MS`) em `server3` (+3 s), `server5` (−2,5 s) e `server7` (+5 s) para dar para ver o ajuste nos logs.

### Eleição do coordenador (Parte 4)

Cada servidor guarda o nome do coordenador na variável `coordenador`. Mensagens novas (todas pelo socket REP que o servidor já tinha):

| Requisição (`type`) | Resposta | Uso |
|---|---|---|
| `clock` | `clock_reply` com `time` (hora correta) | servidor → coordenador |
| `election` | `election_reply` com `status: ok` | servidor → servidores de rank maior |

E o anúncio do coordenador é publicado no tópico **`servers`** do proxy, com `type: coordinator` e `coordinator: <nome>`. Todos os servidores assinam esse tópico.

Funcionamento:

1. A cada **15 mensagens de clientes** o servidor sincroniza o relógio. Se não conhece coordenador, começa uma eleição.
2. Eleição: pede a lista para a referência e manda `election` para todos com rank maior. Se algum responder OK, espera o anúncio. Se ninguém responder, ele vira coordenador e publica em `servers`.
3. Quem recebe `election` responde OK e depois começa a sua própria eleição.
4. Se o coordenador não responder o `clock` (ou disser que não é mais coordenador), o servidor pede eleição.
5. Servidor que não está na lista da referência (sem heartbeat) não participa da eleição.

### Sincronização com Berkeley (Parte 4)

O coordenador não fica chamando os outros servidores (isso travaria, porque o REQ/REP de cada servidor é síncrono). A adaptação feita foi:

- todo pedido `clock` traz a hora do servidor no campo `ts`;
- o coordenador guarda a diferença de cada servidor (`diferencasRelogios`);
- calcula a **média** das diferenças contando ele mesmo (diferença 0), ajusta o próprio relógio por essa média e responde com a hora resultante;
- o servidor que pediu aplica essa hora + metade do tempo de ida e volta.

Assim o relógio de todos vai para a média dos relógios conhecidos, como no Berkeley, e não para uma hora "externa".

## Replicação (Parte 5)

### Método escolhido

Foi usada **replicação ativa** (protocolo de escrita replicada): toda operação de escrita que um servidor recebe de um cliente é executada nele e enviada para **todas as outras réplicas**, que executam a mesma operação. Para que todas as réplicas fiquem com o histórico na mesma ordem é usada a **ordem total pelo relógio lógico de Lamport** (feito na Parte 3), com o `id` da mensagem para desempatar.

### Por que resolve o problema do projeto

Sem replicação, como o broker faz round-robin, cada servidor só tem uma parte dos logins, canais e mensagens. Se um servidor cai, essa parte do histórico some, e o `get_messages` em outro servidor devolve só um pedaço. Com a replicação ativa todo servidor recebe todas as escritas, então:

- qualquer servidor responde o histórico completo do canal;
- se um servidor parar, os outros continuam com tudo;
- como todos ordenam por `(clock, id)`, a lista de mensagens de um canal é a mesma em todos os servidores.

### Como foi implementado

- O "multicast" para as réplicas usa o próprio proxy PUB/SUB: o servidor publica no tópico **`replication`** e todos os servidores assinam esse tópico (o nome `replication` já era reservado para canais desde a Parte 2).
- Operações replicadas:

| `type` | Conteúdo | O que a réplica faz |
|---|---|---|
| `replicate_channel` | `channel` | cria o canal se não existir |
| `replicate_post` | `channel` e `messages` com a mensagem original (mesmo `id`, `ts` e `clock`) | guarda a mensagem se o `id` ainda não existir e reordena por `(clock, id)` |
| `replicate_login` | `user` | marca o usuário como logado e grava no histórico de logins |
| `replicate_logout` | `user` | tira o usuário dos logados e dos canais |
| `replicate_join` | `user`, `channel` | coloca o usuário no canal |
| `replicate_leave` | `user`, `channel` | tira o usuário do canal |

- O servidor ignora as próprias publicações (campo `from` igual ao seu nome) e mensagens com `id` repetido (operação idempotente).
- Ao receber uma réplica o servidor atualiza o relógio lógico com `max`, igual a qualquer outra mensagem.
- Tudo que chega pela replicação também é salvo em disco (mesmos arquivos `canais.msgpack` e `mensagens_<canal>.msgpack`).

### Mudanças no método para funcionar no projeto

1. **Sem espera de confirmação (ack):** na replicação ativa "pura" a entrega só acontece depois que todos confirmam a ordem (multicast totalmente ordenado). Aqui a mensagem é guardada na hora e a lista é reordenada por `(clock, id)` quando chega uma réplica. Isso deixa o sistema com **consistência eventual**: por um instante um servidor pode não ter a última mensagem, mas todos terminam com a mesma lista, na mesma ordem.
2. **Transferência de estado na entrada:** o PUB/SUB não guarda mensagens para quem estava desligado. Por isso, quando um servidor sobe (novo ou reiniciado), ele pede a lista para a referência e manda `sync` para outro servidor disponível, que responde com `channels`, todas as `messages` e as sessões ativas (itens com `type: session`). O servidor junta com o que tem no disco (sem duplicar pelo `id`). Isso permite adicionar e remover servidores sem parar o serviço.
3. **Sessões também são replicadas:** além das mensagens, login, logout, entrada e saída de canal são replicados. Isso foi necessário por causa do round-robin do broker: o bot faz login em um servidor e o próximo pedido vai para outro, que precisa saber que o usuário está logado e no canal.

**Escopo:** este pacote corresponde à implementação C# da Parte 5 (versão final). O enunciado geral exige outras linguagens conforme o número de integrantes; este pacote não substitui as implementações nas outras linguagens.

## 5. Como testar

### Teste A — todos os containers

```bash
docker compose ps -a
```

Esperado: 8 clientes, 8 servidores e 1 proxy com status `Up`. Isso confirma que estão em execução, não necessariamente que todas as operações estão corretas.

### Teste B — envio REQ/REP

```bash
docker compose logs --tail=150 client1 | grep -E 'ENVIANDO REQ|RECEBENDO REP|Publicação em|Status: error'
```

Procure `Type: post`, `Type: post_reply` e `Status: ok` nos logs completos. `Status: ok` indica que o servidor aceitou a publicação.

### Teste C — recebimento PUB/SUB entre clientes inscritos no mesmo canal

```bash
docker compose logs client2 | grep -A 9 'RECEBENDO PUB' | grep 'From:' | sort | uniq -c
```

Esperado: mensagens com `From: cliente1` **e** `From: cliente2`. Isso demonstra que o `client2` recebeu publicações de outro cliente, além das próprias. Confira também o sentido inverso:

```bash
docker compose logs client1 | grep -A 9 'RECEBENDO PUB' | grep 'From:' | sort | uniq -c
```

### Teste D — assinaturas reais de tópicos

```bash
grep -nE 'subscriber\.(Subscribe|Unsubscribe)' Cliente/Program.cs
```

Esperado: chamadas com `topico` (nome do canal), e **não** `Subscribe("")`.

### Teste E — recuperação após reinício

Com os bots em execução:

```bash
docker compose restart server1
```

Aguarde alguns segundos e confira:

```bash
docker compose logs --since=2m client1 client2 | grep -iE 'RECUPERACAO|Usuário não está registrado|Publicação em'
```

O comportamento esperado é detectar a perda de sessão, refazer o login, entrar novamente nos canais disponíveis e continuar publicando com `ok`. O servidor recupera canais e mensagens dos arquivos; as sessões e inscrições ativas são reconstruídas pelos clientes.

### Teste F — persistência

```bash
docker compose exec server1 sh -c 'echo "DATA_DIR=$DATA_DIR"; ls -lh /data/server1/*.msgpack'
```

Arquivos esperados: `logins.msgpack`, `canais.msgpack` e `mensagens_<canal>.msgpack`. O volume nomeado `server1_data` mantém os arquivos após `docker compose restart` e `docker compose down` (sem `-v`). A existência dos arquivos comprova a gravação; a recuperação deve ser conferida também pelos logs e consultas após o reinício.

### Teste G — todos os bots publicando

```bash
docker compose logs --tail=100 client1 client2 client3 client4 client5 client6 client7 client8 | grep -E 'Publicação em|Status: error'
```

Procure publicações `ok` de cada um dos oito clientes.

### Teste H — referência e ranks

```bash
docker compose logs referencia | grep -E 'cadastrado|removido|voltou'
docker compose logs server1 | grep 'Rank recebido'
```

Esperado: os 8 servidores cadastrados com ranks diferentes.

### Teste I — heartbeat e remoção

```bash
docker compose logs --tail=200 referencia | grep -A 3 'RECEBENDO REQ' | grep -E 'Type: heartbeat|From: server'
```

Para ver um servidor sendo removido:

```bash
docker compose stop server4
docker compose logs -f referencia | grep removido
```

### Teste J — eleição

```bash
docker compose logs | grep -E 'ELEICAO|Novo coordenador'
```

Esperado: os servidores elegem o de maior rank disponível e todos mostram `Novo coordenador: <nome>`. Para forçar uma nova eleição, pare o coordenador:

```bash
docker compose stop <coordenador>
docker compose logs -f --since=1m | grep -E 'indisponível|ELEICAO|Novo coordenador'
```

### Teste J2 — Berkeley

```bash
docker compose logs | grep -E 'BERKELEY|Relógio sincronizado'
```

Esperado: ajustes grandes nas primeiras sincronizações (por causa dos desvios de `server3`, `server5` e `server7`) e depois ajustes pequenos, mostrando que os relógios convergiram para a média.

### Teste K — relógio lógico

```bash
docker compose logs --tail=60 client1 | grep -E 'Type:|Clock:'
```

O `clock` de cada requisição do cliente é maior do que o da última resposta recebida.

### Teste L — replicação

Depois de uns 30 s rodando:

```bash
docker compose logs server2 | grep 'REPLICA:' | tail
```

Cada servidor mostra `REPLICA:` para tudo que veio dos outros. Para comparar os arquivos de dois servidores:

```bash
docker compose exec server1 sh -c 'ls -l /data/server1/'
docker compose exec server6 sh -c 'ls -l /data/server6/'
```

Os mesmos arquivos `mensagens_<canal>.msgpack` devem existir nos dois.

### Teste M — servidor que volta pega o que perdeu

```bash
docker compose stop server3
# espera uns 20 s com os outros bots publicando
docker compose start server3
docker compose logs --since=1m server3 | grep 'Sincronizado com'
```

Esperado: `Sincronizado com serverX: N canais novos e M mensagens novas`.

## 6. Cliente interativo (opcional)

Os clientes do Compose já iniciam automaticamente. Para abrir um cliente interativo adicional na mesma rede do projeto, rode:

```bash
docker compose run --rm -it -e CLIENT_MODE=interactive -e SERVER_HOST=broker -e SERVER_PORT=5555 client1
```

O menu permite: `1` login, `2` criar canal, `3` listar canais, `4` entrar em canal, `5` sair de canal, `6` enviar mensagem, `7` consultar mensagens, `8` logout e `0` encerrar. Use um nome de usuário **diferente** dos bots automáticos (por exemplo, `teste_manual`) para evitar conflito de login. O cliente interativo também conecta ao proxy para receber publicações.

## 7. Variáveis de ambiente

| Variável | Finalidade | Exemplo |
|---|---|---|
| `SERVER_ID` | Identificador do servidor e subpasta de dados | `server1` |
| `SERVER_PORT` | Porta REQ/REP | `6000` |
| `PROXY_HOST` | Host do proxy na rede Docker | `proxy` |
| `DATA_DIR` | Diretório-base da persistência | `/data` |
| `SERVER_HOST` | Host acessado pelo cliente (broker) | `broker` |
| `CLIENT_MODE` | Modo do cliente | `auto` ou `interactive` |
| `CLIENT_USER` | Usuário do bot | `cliente1` |
| `CLIENT_TEST_CHANNEL` | Canal comum opcional de teste | `canal_teste_compartilhado` |
| `BROKER_HOST` | Host do broker (servidores conectam na porta 5556) | `broker` |
| `REFERENCIA_HOST` | Host do serviço de referência | `referencia` |
| `REFERENCIA_PORT` | Porta REQ/REP da referência | `5559` |
| `HEARTBEAT_TIMEOUT` | Segundos sem heartbeat até remover o servidor | `30` |
| `CLOCK_DRIFT_MS` | Desvio inicial do relógio do servidor (teste) | `3000` |

As portas de PUB/SUB do proxy são **5557** (entrada das publicações dos servidores) e **5558** (saída para assinantes). O Compose também declara `PUB_PORT` por servidor, mas a publicação no proxy utiliza o endereço `proxy:5557`.

## 8. Parar, reiniciar e limpar

```bash
# Reiniciar apenas um servidor
docker compose restart server1

# Parar e remover containers, preservando volumes
docker compose down

# Subir novamente
docker compose up --build -d
```

**Atenção:** `docker compose down -v` **apaga os volumes nomeados** e pode excluir definitivamente os históricos persistidos. Use apenas se quiser limpar os dados de propósito.

## 9. Solução de problemas

- **Porta ocupada:** verifique se há containers antigos com `docker ps -a`; pare os antigos ou altere o mapeamento de portas no Compose.
- **`Usuário não está registrado` após reinício:** no modo automático, o bot deve executar a rotina de recuperação; confira os logs com `grep RECUPERACAO`.
- **Só recebe mensagens próprias:** confirme que os dois clientes estão no mesmo servidor e inscritos no mesmo canal; no Compose atual, `client1` e `client2` compartilham o canal de teste.
- **Container `Up`, mas sem mensagens:** consulte `docker compose logs --tail=100 <serviço>` para identificar erros ou tentativas de conexão.
- **Mensagens antigas não aparecem em outro servidor:** confira se o servidor está assinando o tópico `replication` (`docker compose logs serverX | grep REPLICA`) e se fez o `sync` na subida.

## 10. Escopo e limitações

Esta entrega é a **versão final em C#** (Partes 1 a 5). Na Parte 5 mudaram só os servidores (replicação e sincronização na subida); cliente, proxy e referência são os mesmos da Parte 4.
