FROM eclipse-temurin:17-jdk-jammy AS build

WORKDIR /app
COPY pom.xml .
COPY .mvn ./.mvn
COPY mvnw .
COPY src ./src

RUN chmod +x mvnw && ./mvnw clean package -DskipTests -B

FROM eclipse-temurin:17-jre-jammy

WORKDIR /app
COPY --from=build /app/target/tarn-orchestrator-*.jar app.jar

ENTRYPOINT ["java", "-jar", "app.jar"]
