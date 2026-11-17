#!/usr/bin/env python3
"""Генератор нагрузки Pulse с управлением по двум параметрам.

Параметр 1: USERS_PER_SEC     — сколько пользователей шлют метрики в секунду.
Параметр 2: METRICS_PER_USER  — сколько метрик в одном пакете пользователя.
Добавка:    VIOLATION_USER_PCT — % пула пользователей, чьи метрики выходят за
                                  порог (value > 80) и открывают инцидент
                                  (правило temperature-high, processing-service).

Каждая отправка = 1 POST /users/{id}/metrics со своим Idempotency-Key.
Только стандартная библиотека Python.
"""
import base64
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("LOAD_BASE", "http://localhost")
PASSWORD = "Passw0rd!"

# --- три управляable параметра ---
USERS = int(os.environ.get("USERS", "100"))           # размер пула пользователей
USERS_PER_SEC = int(os.environ.get("USERS_PER_SEC", "20"))   # ПАРАМЕТР 1
METRICS_PER_USER = int(os.environ.get("METRICS_PER_USER", "50"))  # ПАРАМЕТР 2
VIOLATION_USER_PCT = float(os.environ.get("VIOLATION_USER_PCT", "10"))  # доля нарушителей
DURATION = int(os.environ.get("DURATION_SEC", "120"))
WORKERS = int(os.environ.get("WORKERS", "32"))
METRIC_NAME = os.environ.get("METRIC_NAME", "temperature")

NORMAL_RANGE = (30.0, 50.0)   # штатные значения (ниже порога 80)
VIOLATION_VALUE = 85.0        # > 80 => нарушение правила temperature-high

lat_lock = threading.Lock()
stats = {"sent": 0, "http_err": 0, "net_err": 0, "violation_requests": 0, "lat": []}


def register(email):
    body = json.dumps({"email": email, "password": PASSWORD}).encode()
    req = urllib.request.Request(BASE + "/auth/register", data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.load(r)["userId"]
    except urllib.error.HTTPError as e:
        if e.code == 409:  # занят — переиспользуем существующего, достанем id позже не нужно
            return None
        raise


def build_batch(violating):
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    n = METRICS_PER_USER
    vals = [round(random.uniform(*NORMAL_RANGE), 1) for _ in range(n)]
    if violating and n >= 1:
        vals[random.randrange(n)] = VIOLATION_VALUE  # одна точка за порогом
    return json.dumps({"metrics": [
        {"name": METRIC_NAME, "value": v, "timestamp": ts} for v in vals]}).encode()


def fire(user):
    uid, auth = user["uid"], user["auth"]
    payload = build_batch(user["violating"])
    if user["violating"]:
        with lat_lock:
            stats["violation_requests"] += 1
    t0 = time.perf_counter()
    req = urllib.request.Request(
        f"{BASE}/users/{uid}/metrics", data=payload,
        headers={"Content-Type": "application/json", "Authorization": auth,
                 "Idempotency-Key": str(uuid.uuid4())})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            r.read()
        with lat_lock:
            stats["sent"] += 1
            stats["lat"].append((time.perf_counter() - t0) * 1000)
    except urllib.error.HTTPError:
        with lat_lock:
            stats["http_err"] += 1
    except Exception:
        with lat_lock:
            stats["net_err"] += 1


def main():
    # 1) регистрируем/подбираем пул пользователей, размечаем нарушителей
    print(f"Подготовка пула: {USERS} пользователей, ~{VIOLATION_USER_PCT}% нарушителей...")
    pool = []
    for i in range(USERS):
        email = f"pulse-load-{i}@pulse.dev"
        uid = register(email)
        if uid is None:  # email занят — берём id из индекса не можем, зарегистрируем с суффиксом
            email = f"pulse-load-{i}-{uuid.uuid4().hex[:6]}@pulse.dev"
            uid = register(email)
        auth = "Basic " + base64.b64encode(f"{uid}:{PASSWORD}".encode()).decode()
        pool.append({"uid": uid, "auth": auth, "violating": (random.random() * 100) < VIOLATION_USER_PCT})

    violators = sum(1 for u in pool if u["violating"])
    print(f"Пул готов: {len(pool)} пользователей, {violators} помечено нарушителями "
          f"(ожидается инцидентов ~{violators}).")
    print(f"Профиль: {USERS_PER_SEC} польз./сек x {METRICS_PER_USER} метрик/пакет, {DURATION} сек. "
          f"Суммарно метрик ~ {USERS_PER_SEC * METRICS_PER_USER * DURATION:,}")

    # 2) планировщик: каждый секундный тик берём USERS_PER_SEC пользователей (round-robin) и шлём
    stop = time.monotonic() + DURATION
    cursor = 0
    q_lock = threading.Lock()
    work = []

    def sender():
        while True:
            with q_lock:
                if not work:
                    if time.monotonic() > stop + 2:
                        return
                    time.sleep(0.02)
                    continue
                user = work.pop()
            fire(user)

    th = [threading.Thread(target=sender, daemon=True) for _ in range(WORKERS)]
    for t in th:
        t.start()

    tick = 0
    while time.monotonic() < stop:
        t0 = time.monotonic()
        with q_lock:
            for _ in range(USERS_PER_SEC):
                work.append(pool[cursor % len(pool)])
                cursor += 1
        tick += 1
        if tick % 10 == 0:
            with lat_lock:
                print(f"  t={tick}c  sent={stats['sent']} http_err={stats['http_err']} "
                      f"net_err={stats['net_err']}viol_req={stats['violation_requests']}")
        time.sleep(max(0.0, 1.0 - (time.monotonic() - t0)))

    time.sleep(3)  # добить очередь
    lat = stats["lat"]
    lat.sort()
    p = lambda q: round(lat[int(q / 100 * (len(lat) - 1))], 1) if lat else None
    summary = {
        "users": len(pool), "violators_in_pool": violators,
        "users_per_sec": USERS_PER_SEC, "metrics_per_user": METRICS_PER_USER,
        "duration_sec": DURATION,
        "sent_ok": stats["sent"], "http_err": stats["http_err"], "net_err": stats["net_err"],
        "violation_requests": stats["violation_requests"],
        "metrics_emitted_est": stats["sent"] * METRICS_PER_USER,
        "p50_ms": p(50), "p95_ms": p(95), "p99_ms": p(99), "max_ms": round(lat[-1], 1) if lat else None,
        "how_to_verify_incidents":
            "docker exec postgres psql -U pulse -d pulse -c "
            "\"select count(*), status from metric.incidents where rule_id='temperature-high' group by status;\"",
    }
    print(json.dumps(summary, ensure_ascii=False, indent=1))


if __name__ == "__main__":
    main()
