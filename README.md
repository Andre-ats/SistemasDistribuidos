# BBS — Sistema Distribuído de Mensagens

## 📌 1. Descrição do Projeto

O **BBS (Bulletin Board System)** é um sistema distribuído de troca de mensagens desenvolvido para a disciplina de Sistemas Distribuídos.

O projeto simula uma plataforma na qual clientes podem realizar login, consultar e participar de canais e publicar mensagens. A comunicação acontece entre diferentes servidores, implementados em quatro linguagens de programação, demonstrando interoperabilidade entre tecnologias distintas.

A execução é automatizada por meio de bots e containers Docker, sem necessidade de interação manual durante o funcionamento.

## 🛠️ 2. Tecnologias Utilizadas

- **C# / .NET**
- **Python**
- **Java**
- **TypeScript / Node.js**
- **ZeroMQ:** comunicação e troca de mensagens entre os componentes.
- **MessagePack:** serialização das mensagens.
- **Docker e Docker Compose:** containerização e orquestração dos serviços.

## 🏗️ 3. Arquitetura do Projeto

O sistema é composto por **19 serviços**, organizados da seguinte maneira:

| Componente | Quantidade | Descrição |
|:---|:---:|:---|
| Servidores | 8 | Dois servidores por linguagem |
| Bots (clientes) | 8 | Dois clientes por linguagem |
| Broker | 1 | Gerencia a comunicação Request/Reply |
| Proxy | 1 | Intermedeia a comunicação Publisher/Subscriber |
| Reference | 1 | Serviço de referência para registro, ranking e heartbeat |
| **Total** | **19** | |

Os servidores e clientes estão distribuídos entre C#, Python, Java e TypeScript.

## ⚙️ 4. Funcionalidades Implementadas

- **Request/Reply:** comunicação entre clientes e servidores, incluindo login, consulta de canais e publicação de mensagens.
- **Publisher/Subscriber:** distribuição de mensagens para os clientes inscritos nos canais.
- **Replicação:** compartilhamento de dados entre os servidores.
- **Persistência:** armazenamento dos dados em volumes Docker.
- **Relógios lógicos de Lamport:** ordenação lógica dos eventos.
- **Sincronização de relógios:** utilização do algoritmo de Berkeley.
- **Heartbeat:** monitoramento da disponibilidade dos servidores.
- **Eleição de coordenador:** utilização do algoritmo Bully.
- **Recuperação e sincronização:** recuperação dos dados após a indisponibilidade de um servidor.

## 🚀 5. Como Executar o Projeto

### 📋 Pré-requisitos

Antes de iniciar, é necessário ter:

- Windows 10 ou 11.
- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado e em execução.
- Docker Compose disponível pelo comando `docker compose`.
- [Visual Studio Code](https://code.visualstudio.com/) (opcional, recomendado para execução pelo terminal integrado).

### 5.1. Clonar o repositório

```bash
git clone URL_DO_REPOSITORIO
```

Entre na pasta do projeto:

```bash
cd bbs
```

### 5.2. Construir e iniciar os serviços

Execute na raiz do projeto:

```powershell
docker compose up --build -d
```

Esse comando constrói as imagens necessárias e inicia os 19 serviços em segundo plano.

### 5.3. Verificar os serviços

Para conferir se os containers foram iniciados:

```powershell
docker compose ps
```

Os serviços devem aparecer com o estado `Up` ou `running`.

### 5.4. Acompanhar os logs

Para visualizar os registros de todos os serviços:

```powershell
docker compose logs -f
```

Para acompanhar um serviço específico:

```powershell
docker compose logs -f server-py-1
```

Substitua o nome do serviço conforme necessário, por exemplo, `server-cs-1`, `server-java-1` ou `server-ts-1`.

Para sair da visualização dos logs, pressione `Ctrl + C`. Isso não encerra os containers.

## 🛑 6. Encerrar a Execução

Para parar e remover os containers e a rede criada pelo Docker Compose:

```powershell
docker compose down
```

Os volumes persistentes são mantidos por padrão.

> **Atenção:** não utilize `docker compose down -v` se desejar preservar os dados armazenados pelos servidores, pois esse comando também remove os volumes.

## 📁 7. Organização do Projeto

O código-fonte está separado por linguagem:

```text
bbs/
├── csharp/
├── python/
├── java/
├── ts/
└── docker-compose.yml
```

Cada diretório contém as implementações dos servidores e clientes correspondentes à sua linguagem.

Os serviços de infraestrutura, como broker, proxy e reference, são configurados e iniciados pelo Docker Compose.

---

## 🎓 Projeto Acadêmico — Sistemas Distribuídos

Implementação de comunicação, replicação, sincronização e tolerância a falhas em um ambiente distribuído e heterogêneo.
