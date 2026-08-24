FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY central-station/pom.xml central-station/pom.xml
# Reactor sibling: its pom must exist for Maven to read the parent, even
# though -pl central-station never builds it.
COPY weather-station/pom.xml weather-station/pom.xml
COPY central-station/src central-station/src
RUN mvn -pl central-station -am clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/central-station/target/central-station.jar /app/central-station.jar
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75.0", "-jar", "/app/central-station.jar"]
