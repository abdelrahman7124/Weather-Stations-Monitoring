FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
COPY common/pom.xml common/pom.xml
COPY common/src common/src
COPY weather-station/pom.xml weather-station/pom.xml
COPY weather-station/src weather-station/src
RUN mvn -pl weather-station -am clean package -DskipTests

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/weather-station/target/weather-station.jar /app/weather-station.jar
ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092
ENTRYPOINT ["java", "-jar", "/app/weather-station.jar"]
