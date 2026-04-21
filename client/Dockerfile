# Fase 1: Build da Aplicação em Java com Maven
FROM maven:3.8.7-eclipse-temurin-11 AS build
WORKDIR /app


COPY client/pom.xml /app/client/pom.xml
COPY client/src /app/client/src

WORKDIR /app/client

RUN mvn clean package -DskipTests

FROM eclipse-temurin:11-jre
WORKDIR /app

COPY --from=build /app/client/target/client-1.0-SNAPSHOT-jar-with-dependencies.jar /app/bot-client.jar

CMD ["java", "-jar", "/app/bot-client.jar"]
