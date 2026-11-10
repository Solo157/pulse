#!/usr/bin/env bash
# Нагрузочный прогон через hey с УНИКАЛЬНЫМ Idempotency-Key и свежим
# timestamp на каждое обращение: каждый запрос — новое событие, конвейер
# (Kafka → storage → processing) реально пишет, а не отдаёт дубликаты.
# hey сам один key на весь прогон поменять не может — поэтому прогон
# собирается из «одиночных» hey-процессов по плоскому темпу.
#
# Использование:  scripts/hey-load.sh <email> <пароль> <rps> <секунд>
# Пример:         scripts/hey-load.sh hey-1@pulse.dev 'Passw0rd!' 20 300
set -euo pipefail
EMAIL=${1:?email}; PASS=${2:?password}; RPS=${3:?rps}; DUR=${4:?seconds}
BASE=${LOAD_BASE:-http://localhost}

BODY=$(printf '{"email":"%s","password":"%s"}' "$EMAIL" "$PASS")
RESP=$(curl -sf -X POST "$BASE/auth/register" -H 'Content-Type: application/json' -d "$BODY")
USER_ID=$(printf '%s' "$RESP" | sed -n 's/.*"userId":"\([^"]*\)".*/\1/p')
[ -n "$USER_ID" ] || { echo "не удалось зарегистрировать пользователя: $RESP" >&2; exit 1; }
echo "userId=$USER_ID, rps=$RPS, длительность=${DUR}s"

OUT="results/hey-$(date +%Y%m%d).log"
mkdir -p results

for _sec in $(seq 1 "$DUR"); do
  for _j in $(seq 1 "$RPS"); do
    TS=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    { hey -n 1 -c 1 -m POST -a "$USER_ID:$PASS" \
        -H "Idempotency-Key: $(uuidgen)" -T application/json \
        -d "{\"metrics\":[{\"name\":\"temperature\",\"value\":42.0,\"timestamp\":\"$TS\"}]}" \
        "$BASE/users/$USER_ID/metrics" 2>>results/hey-errors.log \
      | sed -n 's/^ *\[\([0-9]*\)\].*/\1/p' >> "$OUT"; } &
  done
  sleep 1
done
wait
echo "готово; коды ответов в $OUT: $(sort "$OUT" | uniq -c | tr '\n' ' ')"
echo "сверка принятых: docker exec postgres psql -U pulse -d pulse -tAc \"select count(*) from metric.ingest_idempotency where user_id='$USER_ID' and status='COMPLETED'\""

