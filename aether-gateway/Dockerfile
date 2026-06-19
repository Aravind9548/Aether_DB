# Stage 1: Build
FROM maven:3.8.5-openjdk-17 AS builder
WORKDIR /app
COPY pom.xml ./
RUN mvn dependency:go-offline
COPY src/ ./src/
RUN mvn clean package -DskipTests

# Stage 2: Run
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/gateway-1.0.0.jar ./app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
