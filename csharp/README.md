# Sistema de Troca de Mensagens — BBS (C#)

## 1. Introdução

Este projeto foi desenvolvido para a disciplina de Sistemas Distribuídos do Centro Universitário FEI.

O objetivo é implementar um sistema de troca de mensagens inspirado nos Bulletin Board Systems (BBS), utilizando comunicação distribuída entre clientes e servidores.

Nesta primeira etapa, foram implementadas as funcionalidades de login de usuários, criação de canais e listagem dos canais disponíveis.

O projeto foi desenvolvido em **C# com .NET 8**, utilizando **ZeroMQ (NetMQ)** para comunicação e **MessagePack** para serialização binária das mensagens.

## 2. Tecnologias utilizadas

- **C# / .NET 8:** implementação dos clientes e servidores.
- **NetMQ:** biblioteca de comunicação baseada no padrão ZeroMQ.
- **MessagePack:** serialização e desserialização binária das mensagens.
- **Docker:** criação e execução dos containers.
- **Docker Compose:** orquestração dos clientes e servidores.
- **Volumes Docker:** persistência dos dados dos servidores.

## 3. Estrutura do projeto

```text
csharp/
├── Common/
│   ├── Envelope.cs
│   ├── LoginRegistro.cs
│   └── Common.csproj
├── Cliente/
│   ├── Program.cs
│   └── Cliente.csproj
├── Server/
│   ├── Program.cs
│   ├── Servidor.cs
│   ├── Persistencia.cs
│   └── Server.csproj
├── Dockerfile.client
├── Dockerfile.server
├── docker-compose.yaml
├── .dockerignore
└── README.md
```

### Common

Contém as estruturas compartilhadas entre clientes e servidores, incluindo o formato das mensagens e os registros de login.

### Cliente

Responsável por enviar requisições ao servidor, interpretar respostas e executar as operações do sistema.

Possui dois modos de funcionamento:

- **Interativo:** permite executar operações por meio de um menu no terminal.
- **Automático:** realiza login, consulta os canais, cria um canal quando necessário e realiza logout, sem intervenção do usuário.

### Server

Responsável por receber e processar requisições, gerenciar usuários e canais e armazenar os dados persistentes.

## 4. Arquitetura de comunicação

A comunicação entre clientes e servidores utiliza o padrão **Request/Reply (REQ/REP)** do ZeroMQ.

O cliente envia uma requisição e aguarda a resposta do servidor.

```text
Cliente                     Servidor
   |                           |
   |-------- REQ: login ------->|
   |<------- REP: status -------|
   |                           |
   |---- REQ: list_channels --->|
   |<------ REP: canais --------|
   |                           |
   |---- REQ: create_channel -->|
   |<------- REP: status -------|
```

O projeto também possui suporte ao padrão Publish/Subscribe (PUB/SUB), utilizado para notificações de mensagens em canais.

Cada servidor mantém seu próprio conjunto de dados.

## 5. Formato das mensagens

Todas as mensagens são serializadas em formato binário utilizando MessagePack.

A estrutura compartilhada é representada pela classe `Envelope`.

| Campo | Descrição |
|---|---|
| `type` | Tipo da operação |
| `ts` | Timestamp do envio |
| `clock` | Relógio lógico |
| `from` | Identificação da origem |
| `user` | Nome do usuário |
| `channel` | Nome do canal |
| `text` | Conteúdo textual, quando aplicável |
| `status` | Resultado da operação |
| `error` | Descrição do erro |
| `channels` | Lista de canais |
| `messages` | Lista de mensagens |
| `id` | Identificador da mensagem |

O timestamp é representado em milissegundos desde a época Unix, utilizando UTC.

O relógio lógico segue o conceito de relógios de Lamport, permitindo acompanhar a ordem lógica dos eventos.

Os nomes dos campos e os tipos de dados devem ser mantidos nas demais implementações do projeto, independentemente da linguagem utilizada.

## 6. Funcionalidades da Parte 1

### Login de usuários

O cliente envia uma requisição `login` contendo o nome do usuário.

O servidor verifica a solicitação e retorna uma resposta de sucesso ou erro.

O sistema não utiliza senhas, conforme definido no enunciado.

Os logins realizados com sucesso são registrados com seus respectivos timestamps.

### Listagem de canais

O cliente envia uma requisição `list_channels`.

O servidor responde com a lista dos nomes dos canais cadastrados.

### Criação de canais

O cliente envia uma requisição `create_channel`, informando o nome do canal.

O servidor verifica se o canal já existe e retorna o resultado da operação.

Canais criados com sucesso são armazenados para consultas posteriores.

### Logout

O cliente pode enviar uma requisição `logout` para encerrar sua sessão.

No modo automático, o logout é executado após as operações de login, listagem e criação de canal.

## 7. Persistência dos dados

Os servidores armazenam os registros de login e os canais criados em disco.

A persistência é gerenciada pela classe `Persistencia.cs`.

Cada servidor possui um volume Docker independente, evitando o compartilhamento direto de arquivos entre servidores.

Os dados são preservados mesmo quando os containers são recriados, desde que os volumes não sejam removidos.

As mensagens publicadas serão contempladas nas próximas etapas do projeto, conforme o enunciado.

## 8. Execução com Docker

### Pré-requisitos

- Docker
- Docker Compose

### Iniciar o projeto

Na pasta raiz do projeto, executar:

```bash
docker compose up --build
```

O Docker Compose construirá as imagens e iniciará os clientes e servidores.

A configuração atual contém:

- 8 containers de servidores.
- 8 containers de clientes.
- 8 volumes independentes de persistência.

Essa quantidade corresponde à execução de dois clientes e dois servidores para cada um dos quatro integrantes do grupo.

### Funcionamento automático

Ao iniciar, cada cliente executa as seguintes operações:

1. Conecta-se ao servidor configurado.
2. Realiza login.
3. Solicita a lista de canais.
4. Cria seu canal, caso ainda não exista.
5. Realiza logout.
6. Encerra a execução.

Os servidores permanecem ativos para receber novas requisições.

É esperado que os clientes terminem com o status `Exited (0)`, indicando execução concluída sem erros.

### Verificar os containers

```bash
docker compose ps -a
```

### Consultar os logs

```bash
docker compose logs client1 client2 client3 client4 client5 client6 client7 client8
```

### Encerrar os containers

```bash
docker compose down
```

Esse comando preserva os volumes.

Para remover também os dados persistentes:

```bash
docker compose down -v
```

**Atenção:** o último comando apaga os dados armazenados nos volumes do projeto.

## 9. Execução local

Para compilar o projeto C#:

```bash
dotnet build Cliente/Cliente.csproj
dotnet build Server/Server.csproj
```

Para iniciar um servidor:

```bash
SERVER_ID=server1 SERVER_PORT=6000 PUB_PORT=6001 dotnet run --project Server
```

Em outro terminal, para iniciar o cliente interativo:

```bash
SERVER_HOST=localhost SERVER_PORT=6000 PUB_PORT=6001 dotnet run --project Cliente
```

## 10. Decisões de implementação

As principais escolhas técnicas foram:

- **C#:** linguagem utilizada nesta implementação do sistema.
- **NetMQ:** permite utilizar os padrões de comunicação do ZeroMQ em aplicações .NET.
- **MessagePack:** fornece serialização binária compacta e compatível com diferentes linguagens.
- **REQ/REP:** estabelece a troca de requisições e respostas entre clientes e servidores.
- **Persistência independente:** cada servidor mantém seus próprios registros.
- **Docker Compose:** permite iniciar e testar múltiplos processos distribuídos em uma única máquina.
- **Clientes automáticos:** garantem que as operações da primeira etapa sejam executadas sem interação manual.

## 11. Integração com outras linguagens

O projeto C# faz parte de um sistema distribuído que também será implementado em outras linguagens de programação.

Para garantir a interoperabilidade, as implementações deverão utilizar o mesmo protocolo de comunicação, os mesmos tipos de operações e a mesma estrutura de mensagens MessagePack.

A compatibilidade entre diferentes implementações deverá ser validada por meio de testes de comunicação.

## 12. Considerações finais

A primeira etapa da implementação em C# contempla as operações básicas de autenticação por nome de usuário, criação e consulta de canais, comunicação por ZeroMQ e persistência de dados.

O ambiente Docker permite executar automaticamente múltiplos clientes e servidores, preparando a aplicação para integração com as demais implementações e para as próximas etapas do sistema distribuído.
