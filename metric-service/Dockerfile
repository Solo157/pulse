FROM eclipse-temurin:21-jre-alpine AS runtime
WORKDIR /app

# Jar собирается заранее командой: mvn clean package (тесты можно пропустить -DskipTests)
# либо всей сборкой "mvn clean install" — образ просто копирует готовый jar модуля.
COPY metric-service/target/*.jar app.jar

# Приложение работает не от root — базовая гигиена контейнера.
RUN adduser -D -u 10001 appuser || true
USER appuser

# Настройки JVM по умолчанию. Можно переопределить переменной JAVA_OPTS при запуске:
# -XX:+UseContainerSupport и MaxRAMPercentage — ограничить heap долей памяти контейнера,
# egd — не ждать данные из /dev/random при генерации случайных чисел.
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0 -Djava.security.egd=file:/dev/./urandom"

ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/app.jar"]

# Порт по умолчанию; фактический задаётся переменной METRIC_SERVER_PORT (см. application.yml).
EXPOSE 8080
