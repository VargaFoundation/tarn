FROM eclipse-temurin:17-jdk-jammy AS build

WORKDIR /app
COPY pom.xml .
COPY .mvn ./.mvn
COPY mvnw .
COPY src ./src

RUN chmod +x mvnw && ./mvnw clean package -DskipTests -B

FROM eclipse-temurin:17-jre-jammy

# Create a non-root user. Matches the runAsUser=10000 in helm/values.yaml so the K8s
# securityContext doesn't need to override the image default.
RUN useradd --uid 10000 --create-home --shell /sbin/nologin tarn

WORKDIR /app
COPY --from=build --chown=tarn:tarn /app/target/tarn-orchestrator-*.jar app.jar

USER 10000
ENTRYPOINT ["java", "-jar", "app.jar"]
