FROM maven:3.9.6-eclipse-temurin-21-alpine
WORKDIR /app
COPY . .
RUN mvn clean install
CMD ["java", "-jar", "target/rinha-transacao-particionada-1.0.0-SNAPSHOT.jar"]
