# Pulse — учебное DEMO событие-ориентированного мониторинга

Приём больших пакетов метрик, поиск инцидентов по пороговым правилам и доставка
уведомлений. Полное техническое описание (устройство, все настройки) — `doc.md`;
итоги, находки и порядок действий — `result.md`; план и решения —
`implementation.md`, `docs/decisions.md`.

## Архитектура

```
client ──> nginx :80 ──> metric-service ──> Kafka [metrics] ──> storage-service ──> ClickHouse
                                 │                   │
                    PostgreSQL ◄─┤                   └──> processing-service ──> [incidents]
               (пользователи,    │                            │                     │
                правила,         └── чтение: история и        ▼                     ▼
                инциденты,       инциденты владельцу      PostgreSQL ◄── notification-service
                уведомления)     (ClickHouse + PostgreSQL)  (инциденты)   └─> ДЕМО-канал (HTTP на себя)
```

| Компонент | Роль | Порт (внутри сети) |
|---|---|---|
| `nginx` | единственная точка входа клиентов (в DEMO — один экземпляр, SPOF стенда) | `:80` на хосте |
| `metric-service` | регистрация, приём пакетов метрик (код `202` только после подтверждения Kafka), выдача истории владельцу | 8080 |
| `storage-service` | читает топик `metrics` и пишет точки в ClickHouse | 8081 |
| `processing-service` | читает `metrics`, сравнивает с порогами, заводит инциденты, публикует топик `incidents` | 8082 |
| `notification-service` | читает `incidents`, доставляет уведомление и записывает результат в базу | 8083 |
| `kafka1`, `kafka2` | узлы Kafka, принимающие запросы; каждая копия топика хранится на двух узлах | 9092 |
| `kafka3` | узел только для управления кластером (данных метрик не хранит) | — |
| `postgres` | пользователи, правила, инциденты, состояния, уведомления | 5432 |
| `clickhouse` | история метрик | 8123 |
| `redis` | задел под будущие задачи; в текущем коде не используется | 6379 |
| `prometheus`, `grafana` | сбор метрик сервисов и готовые графики (профиль `obs`) | 9090 / 3000 |

## Требования

- Docker и Docker Compose v2.
- JDK 21 и Maven 3.9 — нужны только для сборки и тестов. Образы сервисов просто
  копируют уже собранные jar-файлы.

## Быстрый старт

Этот раздел (и раздел «Как пользоваться» ниже) — основное описание, как запускать
и настраивать приложение.

```bash
# 1. Собрать все модули и прогнать тесты (интеграционным тестам нужен Docker)
mvn clean install

# 2. Поднять весь стенд: базы, Kafka, сервисы, nginx.
#    Одноразовый контейнер kafka-init сам создаст топики metrics и incidents
#    (по 2 партиции, две копии каждой). Повторный запуск безопасен.
docker compose up -d

# 3. Дождаться состояния healthy у всех контейнеров
docker compose ps
```

> Если Maven в вашей среде не доверяет сертификату центрального репозитория
> (трафик перехватывает промежуточный прокси), укажите хранилище доверенных
> сертификатов:
> `export MAVEN_OPTS="-Djavax.net.ssl.trustStore=$HOME/m2-truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"`.

## Как пользоваться (сквозной сценарий)

Правило `temperature > 80` (`temperature-high`) создаётся при старте.

```bash
# 1) Регистрация пользователя (пароль не короче 8 символов) → вернёт {"userId": "..."}
curl -s -X POST localhost/auth/register \
  -H 'Content-Type: application/json' \
  -d '{"email":"demo@pulse.dev","password":"***"}'

# 2) Пакет метрик. Заголовок Idempotency-Key обязателен: если отправить пакет
#    с тем же ключом второй раз, второй приём не произойдёт — вернётся тот же ответ.
curl -s -X POST localhost/users/<userId>/metrics \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"metrics":[{"name":"temperature","value":85.0,"timestamp":"2026-11-06T10:00:00Z"}]}'
# → 202 {"eventId":"...","userId":"...","acceptedCount":1}

# 3) Значение выше порога → processing-service откроет инцидент
docker compose exec -T postgres psql -U pulse -d pulse -c \
  "SELECT id, status, opened_at, closed_at FROM metric.incidents ORDER BY opened_at DESC LIMIT 5"

# 4) notification-service создаст уведомление и доставит его в ДЕМО-канал
#    (HTTP-запрос на собственный адрес /internal/demo-receiver)
docker compose exec -T postgres psql -U pulse -d pulse -c \
  "SELECT source_event_id, status, destination, attempts FROM metric.notifications ORDER BY created_at DESC LIMIT 5"
docker compose logs notification-service --tail 20 | grep -i "ДЕМО-канал\|доставлено"

# 5) «Выздоровление» — значение ниже порога закроет инцидент
#    (уведомление при закрытии НЕ создаётся)
curl -s -X POST localhost/users/<userId>/metrics \
  -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" \
  -d '{"metrics":[{"name":"temperature","value":20.0,"timestamp":"2026-11-06T11:00:00Z"}]}'
```

История метрик лежит в ClickHouse — её пишет `storage-service`:

```bash
set -a && . .env   # учётные данные из .env
curl -s --data-binary \
  "SELECT metricName, value, timestamp FROM pulse.metric_points WHERE userId='<userId>' ORDER BY timestamp DESC LIMIT 10" \
  "http://localhost:8123/?user=$CLICKHOUSE_USER&password=$CLICKHOUSE_PASSWORD"
```

## Чтение своей истории (read API)

Данные видны только владельцу: Basic-аутентификация, где логин — `userId`,
пароль — пароль регистрации. Чужой `userId` в пути — `403`, нет или неверны
учётные данные — `401`.

```bash
# История метрик (из ClickHouse): параметры metric, from, to, limit (по умолчанию 100, максимум 1000)
curl -s "localhost/users/<userId>/metrics?metric=temperature&limit=10" -u "<userId>:<пароль-регистрации>"
# [{"name":"temperature","value":85.0,"timestamp":"2026-11-06T10:00:00Z"}, ...]

# Инциденты (из PostgreSQL)
curl -s "localhost/users/<userId>/incidents" -u "<userId>:<пароль-регистрации>"
# [{"userId":"...","ruleId":"temperature-high","status":"OPEN","value":85.0,...}]
```

Приём метрик (`POST /users/{userId}/metrics`) по согласованию для ДЕМО не
требует ключа доступа — только корректный `userId` в пути.

## Состав системы и точки роста

Моно-стенд поднят одной командой `docker compose`; это уменьшенная, но
функционально полная модель будущей распределённой системы. Все четыре сервиса
не хранят состояние у себя (stateless) — всё состояние в базах и Kafka, поэтому
любой сервис можно запускать в нескольких копиях.

### Микросервисы (Spring Boot 3.5 / Java 21)

| Сервис | Роль | Связи | Копирование |
|---|---|---|---|
| **nginx 1.27** | входная точка `:80`; на каждый запрос заново узнаёт адреса `metric-service` через DNS Docker (обновление раз в секунду), при ошибке/таймауте повторяет запрос на другой адрес; сквозной идентификатор запроса | клиентский HTTP → `metric-service:8080` | можно несколько, но DEMO — один |
| **metric-service** | регистрация, приём пакетов с защитой от повторной отправки, проверка владельца при чтении, выдача истории и инцидентов | HTTP; публикация в `metrics`; читает PostgreSQL и ClickHouse | `--scale` |
| **storage-service** | читает `metrics` пачками (до 500 событий за раз) и пишет их в ClickHouse одним запросом; продолжение с того же места после сбоя | Kafka `metrics`; ClickHouse | `--scale` (не больше числа партиций) |
| **processing-service** | читает `metrics`, по правилам определяет открытие/закрытие инцидентов, пишет их в PostgreSQL и публикует в `incidents` | Kafka `metrics` → `incidents`; PostgreSQL | `--scale` (не больше партиций `metrics`) |
| **notification-service** | читает `incidents`, доставляет уведомление с повторными попытками, записывает итог (`SENT`/`FAILED`) | Kafka `incidents`; PostgreSQL | `--scale` (не больше партиций `incidents`) |

### Хранилища

- **PostgreSQL 16** — рабочая база, схема `metric`: `users` (пароли в виде
  BCrypt-хешей), `ingest_idempotency` (ключи защиты от повторной отправки),
  `monitoring_rules` (пороги), `incident_state` и `incidents`,
  `notification.notifications` (статусы доставки). Рост: больше ресурсов и
  реплика для чтения.
- **ClickHouse 24.8** — история метрик, таблица `pulse.metric_points`, движок
  `ReplacingMergeTree`: метка события входит в ключ сортировки, поэтому
  повторная запись той же точки не размножает данные при чтении.
  Рост: шарды и реплики.
- **Redis 7** — развёрнут про запас, под ускорение защиты от повторов; текущий
  код его не трогает (ключи лежат в PostgreSQL).

### Kafka 3.9 (режим KRaft, без ZooKeeper)

- Два узла принимают запросы и хранят данные; третий участвует только в
  управлении кластером. **Топики** создаёт контейнер `kafka-init`:
  `metrics` и `incidents` — по 2 партиции, по 2 копии, подтверждение записи
  требуется от обеих копий (`min.insync.replicas=2`). Ключ сообщения — `userId`,
  поэтому точки одного пользователя идут по одной партиции по порядку.
- **Отправка**: ожидание подтверждения от всех копий (`acks=all`) плюс защита
  от появления дублей при повторных попытках (`enable.idempotence=true`).
- **Чтение**: с самого начала (`auto-offset-reset=earliest`), пачками, отметку о
  прочитанном сервис сдвигает только после успешной записи; при сбое повторит
  обработку пачки. Имена групп: `storage-group`, `processing-group`,
  `notification-group` (стабильные — после перезапуска сервис продолжает с
  своего места).
- **Что из этого следует для нагрузки и отказов**: число обработчиков топика
  растёт только до числа партиций (для `metrics` — две), дальше помогает только
  добавление партиций или более мощные узлы. Две copies + подтверждение от обеих
  означают: падение одного узла Kafka система переживает молча, но потеря обеих
  копий одного топика останавливает запись (ошибка `NOT_ENOUGH_REPLICAS`).
  Для боевого окружения: три и более копии, больше партиций, отдельный
  кластер Kafka.

### Настройки стенда (файл `.env`)

`.env` содержит только то, что реально подставляется в `docker-compose.yml`:
`POSTGRES_*`, `CLICKHOUSE_*`, учётные данные Grafana `GF_ADMIN_*`. Остальные
настройки — переменные с понятными именами внутри `application.yml` каждого
сервиса, полностью они перечислены в `doc.md`, раздел 6.

### Порты

Наружу на все интерфейсы опубликован только nginx `:80` — единственная точка
входа клиентов. Хранилища и мониторинг проброшены на хост только адресом
`127.0.0.1` — это для локальной разработки (psql, clickhouse-client, браузер):
PostgreSQL `127.0.0.1:5432`, ClickHouse `127.0.0.1:8123/9000`, Redis
`127.0.0.1:6379`, Prometheus `127.0.0.1:9090`, Grafana `127.0.0.1:3000`.
Kafka брокерам наружу не публикуется вовсе (клиенты — только контейнеры сети
стенда). При размещении на VPS наружу остаётся один nginx; доступ к
данным и мониторингу с рабочего хоста — через SSH-туннель
(`ssh -L 3000:localhost:3000 -L 9090:localhost:9090 хост`), дополнительно
ограничив basic-auth/prometheus-безопасность по вкусу. Сервисы слушают
8080–8083 внутри сети стенда.

### Что показывают графики (Grafana, дашборд `pulse-overview`)

По работе системы: сколько метрик принято и отклонено, сколько пакетов
повторно прислали по тому же ключу, ошибки публикации, записи в ClickHouse,
обработанные инциденты, отправленные и испорченные уведомления.
По машинам: время ответа HTTP (p95), память и CPU каждого сервиса, время
работы. Лаги Kafka штатным средством не собираются — их смотрят консольной
утилитой `kafka-consumer-groups.sh` (вынести в Prometheus — кандидат развития).

## Копирование сервисов под нагрузкой

```bash
docker compose up -d --scale metric-service=3 --scale storage-service=2
```

Kafka сама распределит партиции между читателями; nginx начнёт слать запросы
на новые копии `metric-service` без перезагрузки. Замеры и методика нагрузочных
прогонов — `docs/results-et7.md`; генератор нагрузки — `./loadgen.py`
(профили `baseline` / `step` / `burst`, интенсивность `LOAD_RPS` запросов/с,
одиночные события в payload; шаг ступени — `LOAD_STEP_SECONDS`, вершина
бу́рста — `LOAD_BURST_MULT`). Сценарии отказов и их результаты —
`docs/fault-injection.md`, шаги воспроизведения — `scripts/chaos.sh`.

## Мониторинг (профиль obs)

```bash
docker compose --profile obs up -d
```

- Prometheus: <http://localhost:9090> — собирает `/actuator/prometheus` со всех
  четырёх сервисов; новые копии сервисов находятся через DNS Docker сами.
- Consumer lag Kafka экспортирует `kafka-lag-exporter` (Admin API, в группы не
  вступает): лаг по группам/топикам/партициям и время лага — метрики
  `kafka_consumergroup_group_*`. Whitelist групп и топиков — в
  `monitoring/kafka-lag-exporter/application.conf`.
- Grafana: <http://localhost:3000> (вход из `.env`), папка **Pulse** →
  дашборд `pulse-overview` (в нём же панели лага: текущий лаг, скорость роста,
  прогресс групп, время лага до дренажа).

## Тесты и полная проверка

```bash
mvn clean install           # все модули: модульные + интеграционные тесты (Testcontainers)
```

Поднимать compose для тестов не нужно, нужен только Docker. Полный сброс
стенда с чистыми данными: `docker compose --profile obs down -v`, затем
`docker compose up -d` — сценарий выше повторяется без ручных шагов.

## Итог работ

Реализованы этапы 0–9 плана `implementation.md`: приём, хранение, инциденты,
уведомления со статусами, выдача истории владельцу, мониторинг, прогоны
нагрузки и отказов. Итоговая проверка с чистого состояния, находки и план
развития — `result.md`.
