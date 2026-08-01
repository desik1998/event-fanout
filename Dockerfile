# Multi-stage build for Event Fanout (Java 21)
FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
RUN mkdir -p /data/batches \
 && useradd -r -u 10001 fanout \
 && chown -R fanout:fanout /data /app
COPY --from=build /app/target/event-fanout-*.jar /app/app.jar
USER fanout
ENV SPRING_DATASOURCE_URL=jdbc:sqlite:/data/fanout.db \
    FANOUT_BATCHES_DIR=/data/batches \
    WORKER_ID=do-1
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
