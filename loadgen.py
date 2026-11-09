#!/usr/bin/env python3
"""Генератор нагрузки Pulse: измерение в точках метрик, не только в запросах.

Особенности: payload содержит LOAD_BATCH_SIZE точек; целевая
интенсивность задаётся в точках в секунду (LOAD_TARGET_POINTS_PER_SEC) либо
однозначно как LOAD_RPS × LOAD_BATCH_SIZE. В отчёте раздельно planned (offered
load), accepted (по acceptedCount ответа 202) и verified (сверка с ClickHouse);
HTTP RPS и точки/с не смешиваются.

Idempotency-Key стабилен на логический batch: сетевой retry того же batch
идёт с тем же ключом (журнал идемпотентности делает повтор безопасным).
Планировщик учитывает реальную отправку: очередь работ ограниченна, переполнение
считается явно (dropped_jobs), молча не теряется ничего.

Профили: baseline (плоский темп), step (ступени), burst (фон + пик).
После нагрузки — ожидание drain (ClickHouse догоняет принятые точки) и лаг Kafka,
если заданы LOAD_CH_COUNT_CMD / LOAD_KAFKA_LAG_CMD (в них {user} подставляется).

Примеры:
  LOAD_PROFILE=baseline LOAD_RPS=50 LOAD_BATCH_SIZE=1 LOAD_DURATION=60 \
      LOAD_EMAIL=u1@pulse.dev LOAD_PASSWORD=passw0rd! ./loadgen.py
  LOAD_PROFILE=step LOAD_BATCH_SIZE=100 LOAD_TARGET_POINTS_PER_SEC=20000 \
      LOAD_STEP_MULTS=1,1.5 LOAD_STEP_SECONDS=60 LOAD_EMAIL=u2@pulse.dev ./loadgen.py
"""
import base64
import json
import os
import queue
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("LOAD_BASE", "http://localhost")
EMAIL = os.environ["LOAD_EMAIL"]
PASSWORD = os.environ["LOAD_PASSWORD"]
PROFILE = os.environ.get("LOAD_PROFILE", "baseline")
DURATION = float(os.environ.get("LOAD_DURATION", "60"))
WORKERS = int(os.environ.get("LOAD_USERS", "8"))
BATCH_SIZE = int(os.environ.get("LOAD_BATCH_SIZE", "1"))
VIOLATION_SHARE = float(os.environ.get("LOAD_VIOLATION_SHARE", "0"))  # доля value > порога
METRIC_NAME = os.environ.get("LOAD_METRIC", "temperature")
HTTP_RETRIES = int(os.environ.get("LOAD_HTTP_RETRIES", "2"))
BURST_MULT = float(os.environ.get("LOAD_BURST_MULT", "5"))
BURST_SECONDS = float(os.environ.get("LOAD_BURST_SECONDS", "20"))
STEP_SECONDS = float(os.environ.get("LOAD_STEP_SECONDS", "60"))
STEP_MULTS = [float(m) for m in os.environ.get("LOAD_STEP_MULTS", "1,2,4").split(",")]
TARGET_PTS = os.environ.get("LOAD_TARGET_POINTS_PER_SEC")
RPS_MODE = os.environ.get("LOAD_RPS", "")
CH_COUNT_CMD = os.environ.get("LOAD_CH_COUNT_CMD")
KAFKA_LAG_CMD = os.environ.get("LOAD_KAFKA_LAG_CMD")
DRAIN_TIMEOUT = float(os.environ.get("LOAD_DRAIN_TIMEOUT", "300"))
OUT = os.environ.get("LOAD_OUT", f"loadgen-{PROFILE}.json")

# Единая цель: HTTP RPS выводится из точек/с строго по формуле RPS × BATCH_SIZE.
if TARGET_PTS:
    RPS = float(TARGET_PTS) / BATCH_SIZE
elif RPS_MODE:
    RPS = float(RPS_MODE)
else:
    RPS = 10.0

user_id = None
auth = None
work_q = None
counters = {"ok": 0, "http_errors": {}, "transport_errors": 0, "retries": 0,
            "accepted_points": 0}
latencies = []
counters_lock = threading.Lock()
results_file = None


def register():
    global user_id, auth
    body = json.dumps({"email": EMAIL, "password": PASSWORD}).encode()
    req = urllib.request.Request(BASE + "/auth/register", data=body,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            user_id = json.load(resp)["userId"]
    except urllib.error.HTTPError as e:
        if e.code == 409:
            raise RuntimeError(f"email {EMAIL} занят — используйте уникальный LOAD_EMAIL") from e
        raise
    auth = "Basic " + base64.b64encode(f"{user_id}:{PASSWORD}".encode()).decode()


def make_payload(sequence):
    """BATCH_SIZE точек со стабильно различными value/timestamp; доля превышений — share."""
    now = int(time.time())
    points = []
    for i in range(BATCH_SIZE):
        violation = (sequence * BATCH_SIZE + i) % 100 < VIOLATION_SHARE * 100
        value = (81.0 + (sequence % 17)) if violation else float(1 + (sequence % 40))
        points.append({"name": METRIC_NAME, "value": value,
                       "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - i))})
    return json.dumps({"metrics": points}).encode()


def sender():
    while True:
        key, payload = work_q.get()
        if key is None:
            work_q.task_done()
            return
        attempts = 0
        while True:
            attempts += 1
            t0 = time.perf_counter()
            req = urllib.request.Request(
                f"{BASE}/users/{user_id}/metrics", data=payload,
                headers={"Content-Type": "application/json", "Authorization": auth,
                         "Idempotency-Key": key})  # ключ постоянен на batch и его retry
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    accepted = json.load(resp).get("acceptedCount", 0)
                with counters_lock:
                    counters["ok"] += 1
                    counters["accepted_points"] += accepted
                    latencies.append((time.perf_counter() - t0) * 1000)
                break
            except urllib.error.HTTPError as e:
                if e.code >= 500 and attempts <= HTTP_RETRIES:
                    with counters_lock:
                        counters["retries"] += 1
                    continue
                with counters_lock:
                    counters["http_errors"][str(e.code)] = \
                        counters["http_errors"].get(str(e.code), 0) + 1
                break
            except Exception:
                if attempts <= HTTP_RETRIES:
                    with counters_lock:
                        counters["retries"] += 1
                    time.sleep(0.2)
                    continue
                with counters_lock:
                    counters["transport_errors"] += 1
                break
        work_q.task_done()


class CountingQueue(queue.Queue):
    def put(self, item, block=True, timeout=None):
        super().put(item, block, timeout)
        if self.qsize() > getattr(self, "high_water_mark", 0):
            self.high_water_mark = self.qsize()


def run_schedule(rps, duration, started):
    """Планирование по времени; переполнение очереди считается явно (dropped)."""
    planned = int(rps * duration)
    interval = 1.0 / rps if rps > 0 else 0.0
    dropped = 0
    for i in range(planned):
        target = started + i * interval
        delay = target - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        job = (str(uuid.uuid4()), make_payload(i))
        try:
            work_q.put(job, timeout=2)
        except queue.Full:
            dropped += 1
    return planned, dropped


def percentile(data, q):
    if not data:
        return None
    ordered = sorted(data)
    index = min(len(ordered) - 1, int(round(q * (len(ordered) - 1))))
    return round(ordered[index], 1)


def shell_out(cmd):
    # check=True обязателен: иначе упавшая команда (битый путь, сеть) молча
    # превращается в пустую строку/«0» и вводит проверку потерь в заблуждение.
    out = subprocess.run(cmd.replace("{user}", str(user_id)), shell=True,
                         capture_output=True, text=True, timeout=60, check=True)
    return out.stdout.strip()


def verify(accepted_points, load_finished_at):
    checks = {}
    if CH_COUNT_CMD:
        deadline = time.monotonic() + DRAIN_TIMEOUT
        while True:
            try:
                ch = int(shell_out(CH_COUNT_CMD))
            except Exception as failure:
                checks["clickhouse_error"] = str(failure)
                ch = None
                break
            if ch >= accepted_points or time.monotonic() >= deadline:
                break
            time.sleep(2)
        checks["clickhouse_points"] = ch
        checks["drain_complete"] = ch is not None and ch >= accepted_points
        checks["drain_seconds"] = round(time.monotonic() - load_finished_at, 1)
    if KAFKA_LAG_CMD:
        try:
            checks["kafka_lag_after_drain"] = shell_out(KAFKA_LAG_CMD)
        except Exception as failure:
            checks["kafka_lag_error"] = str(failure)
    return checks or None


def main():
    global work_q
    if not 1 <= BATCH_SIZE <= 100:
        raise SystemExit("LOAD_BATCH_SIZE вне разумного диапазона (1..100 для этого стенда)")
    if not 0.0 <= VIOLATION_SHARE <= 1.0:
        raise SystemExit("LOAD_VIOLATION_SHARE — доля 0..1")
    register()
    print(f"Пользователь {user_id}, профиль {PROFILE}, batch {BATCH_SIZE}, "
          f"{RPS:.1f} HTTP rps = {RPS * BATCH_SIZE:.0f} точек/с, воркеров {WORKERS}")

    work_q = CountingQueue(maxsize=max(int(RPS * 2), WORKERS * 4, 32))
    work_q.high_water_mark = 0
    workers = [threading.Thread(target=sender, daemon=True) for _ in range(WORKERS)]
    for w in workers:
        w.start()

    if PROFILE == "baseline":
        schedule = [(RPS, DURATION)]
    elif PROFILE == "step":
        schedule = [(RPS * m, STEP_SECONDS) for m in STEP_MULTS]
    elif PROFILE == "burst":
        schedule = [(RPS, DURATION), (RPS * BURST_MULT, BURST_SECONDS)]
    else:
        raise SystemExit("LOAD_PROFILE: baseline | step | burst")

    steps = []
    planned_requests = 0
    t_load = time.monotonic()
    for rps, duration in schedule:
        started = time.monotonic()
        planned, dropped = run_schedule(rps, duration, started)
        steps.append({"planned_http_rps": round(rps, 1),
                      "planned_points_per_sec": round(rps * BATCH_SIZE),
                      "duration_sec": duration, "planned_requests": planned,
                      "dropped_jobs": dropped,
                      "wall_sec": round(time.monotonic() - started, 1)})
        planned_requests += planned
    work_q.join()  # гарантированно дождаться реальной отправки всех поставленных batch
    load_seconds = time.monotonic() - t_load
    load_finished_at = time.monotonic()
    for _ in workers:
        work_q.put((None, None))
    for w in workers:
        w.join(timeout=30)

    errors_total = sum(counters["http_errors"].values()) + counters["transport_errors"]
    completed = counters["ok"] + sum(counters["http_errors"].values()) \
        + counters["transport_errors"]
    report = {
        "profile": PROFILE, "base": BASE, "user_id": user_id, "metric": METRIC_NAME,
        "violation_share": VIOLATION_SHARE,
        "batch_size": BATCH_SIZE, "concurrency": WORKERS,
        "steps": steps,
        # offered load и принятая отдача — разные величины, не путать
        "planned_requests": planned_requests,
        "planned_points": planned_requests * BATCH_SIZE,
        "dropped_jobs_not_scheduled": sum(s["dropped_jobs"] for s in steps),
        "queue_high_water_mark": work_q.high_water_mark,
        "completed_requests": completed,
        "ok_requests": counters["ok"],
        "http_errors_by_code": counters["http_errors"],
        "transport_errors": counters["transport_errors"],
        "http_retries": counters["retries"],
        "accepted_points": counters["accepted_points"],
        "load_duration_sec": round(load_seconds, 1),
        "achieved_requests_per_sec": round(completed / load_seconds, 1),
        "achieved_points_per_sec": round(counters["accepted_points"] / load_seconds, 1),
        "latency_ms": {"p50": percentile(latencies, 0.50), "p95": percentile(latencies, 0.95),
                       "p99": percentile(latencies, 0.99), "max": percentile(latencies, 1.0),
                       "samples": len(latencies)},
        "verification": verify(counters["accepted_points"], load_finished_at),
    }
    with open(OUT, "w") as f:
        json.dump(report, f, indent=2)
    summary = {k: report[k] for k in ("planned_requests", "planned_points",
               "dropped_jobs_not_scheduled", "completed_requests", "accepted_points",
               "achieved_requests_per_sec", "achieved_points_per_sec", "latency_ms",
               "verification")}
    print(json.dumps(summary, indent=2))
    print(f"Отчёт: {OUT}")


if __name__ == "__main__":
    main()
