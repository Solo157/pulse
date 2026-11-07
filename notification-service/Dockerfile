FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app
# Jar should be built locally: mvn clean package -DskipTests
COPY notification-service/target/*.jar app.jar
# Run as non-root for basic container hygiene.
RUN adduser -D -u 10001 appuser || true
USER appuser
# Default JVM / Spring options; can be overridden by env:
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"
# Expose default port (override in compose: METRIC_SERVER_PORT etc)


ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"]

EXPOSE 8080
