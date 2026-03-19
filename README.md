# Projeto de Sistemas Distribuídos - Chat com ZeroMQ e Protobuf

Este projeto foi desenvolvido para atender aos requisitos essenciais da disciplina de Sistemas Distribuídos e representa a primeira parte para a criação de um sistema de bate-papo utilizando arquiteturas altamente desacopladas, focadas em comunicação via sockets orientada a mensagens.

## Visão Geral da Arquitetura Distribuída

Para lidar com os requisitos rigorosos do projeto (dois nós de clientes, dois nós de servidor e proibição de serialização em texto explícito como JSON/XML), a arquitetura adotada foi a combinação nativa em Python e Java associada com:
- **ZeroMQ** atuando como malha de mensagens via soquetes.
- **Protocol Buffers (Protobuf)** efetuando a serialização estrita em formato binário minimizado.
- **Docker Compose** empacotando os serviços de forma autônoma e executável sem interação externa.

### Abordagem ZeroMQ - Router / Dealer
Embora fosse possível criar conexões P2P puras interconectadas em malha (`mesh`), a inclusão de um **Message Broker intermediário** melhora infinitamente a escalabilidade prevista. O serviço **`broker`** utiliza o padrão oficial ROUTER/DEALER do ZeroMQ:
- Os **Clientes/Bots (Java)** fazem REQ (Request) em direção à porta `5555` (ROUTER).
- Os **Servidores/Workers (Python)** utilizam REP (Reply) escutando na porta `5556` (DEALER).
O broker unifica, despacha as requisições (Load Balancing) e reencaminha a resposta certa para o Bot originário de forma assíncrona.

## Justificativa Tecnológica

1. **Protocol Buffers (.proto)**: Diferente de HTTP/JSON puro que envia tags repetitivas gerando *payload overhead*, o Protobuf envia dados em cadeia serializada rigorosamente tipada. A comunicação binária implementada através de um `.proto` compartilhado traz um compilador (`protoc`) que gera classes nativas e herméticas compatíveis em Java e Python, atendendo a restrição de evitar "texto simples".
2. **SQLite embarcado em Python**: Garantindo a persistência estipulada em requisitos, foi desenvolvido um banco de dados relacional leve (SQLite) armazenado em arquivo físico na camada dos servidores Python, separados por volumes Docker e garantindo o seu próprio disco por instância de contêiner. O Sqlite processa inserções, verificação de concorrência local para nomes de canais evitando conflito de forma eficaz.
3. **Java/JeroMQ para Clientes**: O cliente construído em Java opera independentemente rodando rotinas temporizadas automáticas, o módulo JeroMQ proporciona a exata mesma implementação C++ em formato Pure-Java descartando complexas bibliotecas nativas C++.

## Instruções de Execução

Como foi exigido automação máxima utilizando `docker compose up`, rodar todo o sistema e visualizar seu fluxo natural demora poucos segundos:

1. Assegure que não existe nada em sua máquina segurando as portas `5555`/`5556`. (Ou acesse-as rodando com Docker Engine).
2. Abra o terminal raiz do projeto (`/Proj Sistemas Distribuidos`)
3. Execute o comando principal de construção de imagens e orquestração dos contêineres:

```bash
docker compose up --build
```

### O que irá acontecer em tela:

1. O Docker vai empacotar as dependências do servidor (Python pip + grpc/protoc) para garantir que a classe serializada compilada estará visível dentro do Worker.
2. Em paralelo o Maven vai baixar todos os artefatos `pom.xml`, baixar compilador OS Protoc via _os_maven_plugin_ compilar o `chat.proto` em arquivos .java auto-gerados e produzir seu Fat-Jar para o executor final da máquina virtual `openjdk:11`.
3. O Broker zeroMQ e os 2 Servidores Python irão se apresentar no painel de console.
4. Consecutivamente, `client-1` (Alice) e `client-2` (Bob) darão *"Attempting login"* no painel REQ.
5. Em loop os Bots farão: Request *Login* ➔ Request *Channels List* ➔ Request *Create personal bot_channel* ➔ *Delay...* Request *Channels Update Infinity Loop*.
6. Todas as respostas provém da camada de Servidores com identificadores indicando qual Nó Servidor atendeu aquele Load Balancing.

> **💡 Note**: As bases de login e de dados são independentes. Logo o primeiro Bot que for direcionado para `server_1` será resguardado por uma tabela separada em disco em `/app/data/server_1.db`. Se o Bot cair e enviar nova requisição para o broker e a flag de RoundRobin mandá-lo para `server_2`, internamente o ZeroMQ/DB vai recriar esse registro em `/app/data/server_2.db`. Fator imprescindível na topologia desenhada e pronto para extensão futura de P2P ou Distributed Hashes em próximos trabalhos.
