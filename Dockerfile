# Stage 1 — build
FROM gradle:8.5-jdk17 AS builder
WORKDIR /app
COPY . .
RUN gradle shadowJar --no-daemon

# Stage 2 — runtime
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/build/libs/*-all.jar app.jar
EXPOSE 8000
ENTRYPOINT ["java", "-jar", "app.jar"]
