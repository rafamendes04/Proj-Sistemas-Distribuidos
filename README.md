# Projeto de Sistemas Distribuídos - Chat com ZeroMQ e MessagePack

Este projeto foi desenvolvido para atender aos requisitos essenciais da disciplina de Sistemas Distribuídos e representa a primeira parte para a criação de um sistema de bate-papo utilizando arquiteturas altamente desacopladas, focadas em comunicação via sockets orientada a mensagens.

## Visão Geral da Arquitetura Distribuída

Para lidar com os requisitos rigorosos do projeto (dois nós de clientes, dois nós de servidor e proibição de serialização em texto explícito como JSON/XML), a arquitetura adotada foi a combinação nativa em Python e Java associada com:
- **ZeroMQ** atuando como malha de mensagens via soquetes.
- **MessagePack** efetuando a serialização dinamicamente em formato binário minimizado sem esquema.
- **Docker Compose** empacotando os serviços de forma autônoma e executável sem interação externa.

### Abordagem ZeroMQ - Router / Dealer
Embora fosse possível criar conexões P2P puras interconectadas em malha (`mesh`), a inclusão de um **Message Broker intermediário** melhora infinitamente a escalabilidade prevista. O serviço **`broker`** utiliza o padrão oficial ROUTER/DEALER do ZeroMQ:
- Os **Clientes/Bots (Java)** fazem REQ (Request) em direção à porta `5555` (ROUTER).
- Os **Servidores/Workers (Python)** utilizam REP (Reply) escutando na porta `5556` (DEALER).
O broker unifica, despacha as requisições (Load Balancing) e reencaminha a resposta certa para o Bot originário de forma assíncrona.

## Justificativa Tecnológica
1. **MessagePack**: Diferente de HTTP/JSON puro que envia tags repetitivas gerando *payload overhead*, o MessagePack envia dados em cadeia serializada de forma muito mais compacta (compressão de tipos diretos em binário). A comunicação binária atende perfeitamente à restrição de evitar "texto puro", mas com a extrema vantagem de mapeamento direto para `Map`/`Dict` em linguagens dinâmicas e tipadas sem exigir um esquema ou um processo de pré-compilação e plugins rígidos como o Protobuf exige.
2. **SQLite embarcado em Python**: Garantindo a persistência estipulada em requisitos, foi desenvolvido um banco de dados relacional leve (SQLite) armazenado em arquivo físico na camada dos servidores Python, separados por volumes Docker e garantindo o seu próprio disco por instância de contêiner. O Sqlite processa inserções, verificação de concorrência local para nomes de canais evitando conflito de forma eficaz.
3. **Java/JeroMQ para Clientes**: O cliente construído em Java opera independentemente rodando rotinas temporizadas automáticas, o módulo JeroMQ proporciona a exata mesma implementação C++ em formato Pure-Java descartando complexas bibliotecas nativas C++. Jackson com `jackson-dataformat-msgpack` é usado para conversão direta das mensagens binárias.

### O que irá acontecer em tela:

1. O Docker vai empacotar as dependências do servidor (Python pip + dependência `msgpack`) garantindo um processamento limpo e rápido.
2. Em paralelo o Maven vai baixar todos os artefatos `pom.xml`, baixar o Jackson MessagePack e produzir seu Fat-Jar para o executor final da máquina virtual `openjdk:11`.
3. O Broker zeroMQ e os 2 Servidores Python irão se apresentar no painel de console.
4. Consecutivamente, `client-1` (Alice) e `client-2` (Bob) darão *"Attempting login"* no painel REQ.
5. Em loop os Bots farão: Request *Login* ➔ Request *Channels List* ➔ Request *Create personal bot_channel* ➔ *Delay...* Request *Channels Update Infinity Loop*.
6. Todas as respostas provém da camada de Servidores com identificadores indicando qual Nó Servidor atendeu aquele Load Balancing.
