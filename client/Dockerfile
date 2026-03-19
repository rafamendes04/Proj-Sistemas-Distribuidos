# Fase 1: Build da Aplicação em Java com Maven
FROM maven:3.8.7-eclipse-temurin-11 AS build
WORKDIR /app

# Necessitamos do diretório proto root onde o arquivo .proto está armazenado
COPY proto /app/proto

# Copia os metadados do projeto pom.xml e código fonte para dentro do container Build
COPY client/pom.xml /app/client/pom.xml
COPY client/src /app/client/src

WORKDIR /app/client

# Compila e roda o plugin que auto-gera as classes protobuf, e então faz build em um Fat-Jar contendo tudo
RUN mvn clean package -DskipTests

# Fase 2: Imagem minimalista para rodar
FROM eclipse-temurin:11-jre
WORKDIR /app

# Transfere somente o binário executável pra imagem final
COPY --from=build /app/client/target/client-1.0-SNAPSHOT-jar-with-dependencies.jar /app/bot-client.jar

# Impede encerramento automático em caso de falta de parâmetros e prevê execução default
CMD ["java", "-jar", "/app/bot-client.jar"]
