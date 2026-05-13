# Projeto de Sistemas Distribuídos - Chat com ZeroMQ e MessagePack

Este projeto foi desenvolvido para atender aos requisitos essenciais da disciplina de Sistemas Distribuídos e representa a criação de um sistema de bate-papo utilizando arquiteturas altamente desacopladas, focadas em comunicação via sockets orientada a mensagens.

## Visão Geral da Arquitetura Distribuída

Para lidar com os requisitos rigorosos do projeto (dois nós de clientes, dois nós de servidor e proibição de serialização em texto explícito como JSON/XML), a arquitetura adotada foi a combinação nativa em Python e Java associada com:
- **ZeroMQ** atuando como malha de mensagens via soquetes.
- **MessagePack** efetuando a serialização dinamicamente em formato binário minimizado sem esquema.
- **Docker Compose** empacotando os serviços de forma autônoma e executável sem interação externa.

### Abordagem ZeroMQ - Router / Dealer
Embora fosse possível criar conexões P2P puras interconectadas em malha (`mesh`), a inclusão de um **Message Broker intermediário** melhora a escalabilidade prevista. O serviço **`broker`** utiliza o padrão oficial ROUTER/DEALER do ZeroMQ:
- Os **Clientes/Bots (Java)** fazem REQ (Request) em direção à porta `5555` (ROUTER).
- Os **Servidores/Workers (Python)** utilizam REP (Reply) escutando na porta `5556` (DEALER).

O broker unifica, despacha as requisições (Load Balancing) e reencaminha a resposta certa para o Bot originário de forma assíncrona.

### Justificativa Tecnológica
1. **MessagePack**: Diferente de HTTP/JSON puro que envia tags repetitivas gerando *payload overhead*, o MessagePack envia dados em cadeia serializada de forma muito mais compacta. A comunicação binária atende à restrição de evitar "texto puro", com a vantagem de mapeamento direto para `Map`/`Dict` sem exigir esquema ou pré-compilação como o Protobuf exige.
2. **SQLite embarcado em Python**: Banco de dados leve armazenado em arquivo físico na camada dos servidores, separados por volumes Docker — cada instância tem seu próprio disco.
3. **Java/JeroMQ para Clientes**: O módulo JeroMQ é a implementação Pure-Java do ZeroMQ, sem necessidade de bibliotecas nativas C++. A serialização usa `msgpack-core` para conversão direta das mensagens binárias.

## Entregas

### Entrega 1 — REQ/REP, login e persistência

Ao iniciar, cada bot segue o fluxo:
1. Faz login no servidor via REQ/REP.
2. Lista os canais disponíveis.
3. Se existirem menos de 5 canais, cria um novo canal próprio.
4. Entra em loop enviando mensagens aleatórias com intervalo de 1 segundo.

### Entrega 2 — Pub/Sub

Para a publicação de mensagens nos canais, foi adicionado um proxy Pub/Sub separado do broker REQ/REP:
- O proxy utiliza o padrão **XSUB/XPUB** do ZeroMQ.
- Os **Servidores** conectam via PUB na porta `5557` (XSUB do proxy) e publicam mensagens usando o nome do canal como tópico.
- Os **Bots** conectam via SUB na porta `5558` (XPUB do proxy) e se inscrevem em até 3 canais aleatórios.

Toda mensagem recebida via Pub/Sub é exibida no terminal com canal, remetente, timestamp de envio e timestamp de recebimento. O servidor também passou a persistir todas as mensagens no SQLite a partir desta entrega.

### Entrega 3 — Serviço de referência e relógio de Lamport

Criado o `reference.py` que centraliza registro dos servidores ativos, heartbeat (enviado a cada 15 mensagens processadas) e relógio físico de referência.

Relógio de Lamport implementado tanto no servidor (Python) quanto nos bots (Java): toda mensagem carrega o campo `lamport_clock` e ambos os lados atualizam com `max(local, recebido) + 1`.

### Entrega 4 — Eleição de líder (Bully) e sincronização de relógios (Berkeley)

**Eleição Bully:** o servidor com maior `SERVER_ID` assume como coordenador inicial — neste caso, `server_2`. Se o coordenador não responde, qualquer servidor inicia uma eleição enviando mensagem para os peers com ID maior. Quem não recebe resposta se declara coordenador e anuncia via PubSub no tópico `servers`.

**Berkeley:** a cada 15 mensagens, o servidor não-coordenador consulta o coordenador e ajusta o offset local com correção de RTT (`offset = tempo_coordenador - (t_antes + rtt/2)`). Isso roda em thread daemon separada para não bloquear o loop ZMQ principal.

### Entrega 5 — Replicação de dados

**Método escolhido: replicação ativa com PUSH/PULL**

O problema é que o broker faz round-robin entre os servidores, então cada servidor recebe só metade das mensagens — o que quebraria o histórico para qualquer cliente que tentasse recuperá-lo.

A solução foi replicação ativa: sempre que um servidor grava uma mensagem ou cria um canal, ele manda imediatamente para os outros via PUSH. Cada servidor tem um socket PULL numa thread separada que fica recebendo e gravando no SQLite local.

Esse método foi escolhido por ser direto de implementar e suficiente para o contexto do projeto — rede Docker local sem falhas reais de rede. Por isso não foi necessário implementar confirmação de recebimento nem rollback: falhas de envio são apenas logadas. Foi usado `INSERT OR IGNORE` no receiver para evitar duplicatas no caso de dois servidores criarem o mesmo canal simultaneamente.
