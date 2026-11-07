#!/usr/bin/env python3
"""Генератор нагрузки Pulse (Этап 7).

Профили: baseline (плоский RPS), step (ступени), burst (короткий пик).
Только stdlib; RPS-шайт через Планировщик с окном; пишет summary JSON.

Пример:
  LOAD_PROFILE=baseline LOAD_RPS=10 LOAD_DURATION=60 LOAD_USERS=8 ./loadgen.py
"""
import base64
import json
import os
import queue
import statistics
import threading
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("LOAD_BASE", "http://localhost")
EMAIL = os.environ["LOAD_EMAIL"]
PASSWORD = os.environ["LOAD_PASSWORD"]
PROFILE = os.environ.get("LOAD_PROFILE", "baseline")
BASE_RPS = float(os.environ.get("LOAD_RPS", "10"))
DURATION = float(os.environ.get("LOAD_DURATION", "60"))
WORKERS = int(os.environ.get("LOAD_USERS", "8"))
BURST_MULT = float(os.environ.get("LOAD_BURST_MULT", "5"))
BURST_SECONDS = float(os.environ.get("LOAD_BURST_SECONDS", "20"))
STEP_SECONDS = float(os.environ.get("LOAD_STEP_SECONDS", "60"))
STEP_MULTS = [float(m) for m in os.environ.get("LOAD_STEP_MULTS", "1,2,4").split(",")]
OUT = os.environ.get("LOAD_OUT", f"loadgen-{PROFILE}.json")

user_id, auth = None, None


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


def sender():
    latencies = []
    codes = {"ok": 0, "err": 0}
    while True:
        item = work_q.get()
        if item is None:
            break
        _, payload = item
        t0 = time.perf_counter()
        req = urllib.request.Request(
            f"{BASE}/users/{user_id}/metrics", data=payload,
            headers={"Content-Type": "application/json", "Authorization": auth,
                     "Idempotency-Key": str(uuid.uuid4())})
        # Idempotency-Key должен быть стабильным для повторной отправки одного
        # пакета, но генератор каждый раз создаёт новый пакет — новый ключ корректен.
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                codes["ok"] += 1
        except urllib.error.HTTPError:
            codes["err"] += 1
            print("HTTP_ERROR")
            continue
        except Exception:
            codes["err"] += 1
            continue
        latencies.append((time.perf_counter() - t0) * 1000)
    results.append((latencies, codes))


work_q: queue.Queue = queue.Queue(maxsize=10000)
results: list = []


def schedule():
    if PROFILE == "baseline":
        mults = [(DURATION, 1.0)]
    elif PROFILE == "step":
        per = DURATION / len(STEP_MULTS)
        mults = [(per, m) for m in STEP_MULTS]
    elif PROFILE == "burst":
        base_seconds = max(DURATION - BURST_SECONDS, 0)
        mults = [(base_seconds, 1.0), (BURST_SECONDS, BURST_MULT)]
    else:
        raise SystemExit(f"неизвестный профиль {PROFILE}")
    t_end_total = time.monotonic() + sum(s for s, _ in mults if s > 0)
    idx = 0
    seq = 0
    for seconds, mult in mults:
        rps = BASE_RPS * mult
        interval = 1.0 / rps
        t_stop = time.monotonic() + seconds
        print(f"[{idx}] rps={rps:.1f} for {seconds:.0f}s", flush=True)
        idx += 1
        while time.monotonic() < t_stop:
            t_slot = time.monotonic()
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            payload = json.dumps({"metrics": [
                {"name": "temperature", "value": float(20 + seq % 60), "timestamp": ts}]}).encode()
            seq += 1
            while True:
                try:
                    work_q.put((t_slot, payload), timeout=1)
                    break
                except queue.Full:
                    if time.monotonic() > t_end_total + 5:
                        return
            time.sleep(max(0.0, t_slot + interval - time.monotonic()))


register()
threads = [threading.Thread(target=sender, daemon=True) for _ in range(WORKERS)]
for t in threads:
    t.start()
try:
    schedule()
finally:
    for _ in range(WORKERS):
        work_q.put(None)
for t in threads:
    t.join()
lat = [v for pair in results for v in pair[0]]
ok = sum(pair[1]["ok"] for pair in results)
err = sum(pair[1]["err"] for pair in results)
summary = {
    "profile": PROFILE, "base_rps": BASE_RPS, "duration_s": DURATION,
    "workers": WORKERS, "burst_mult": BURST_MULT if PROFILE == "burst" else None,
    "step_mults": STEP_MULTS if PROFILE == "step" else None,
    "sent_ok": ok, "sent_err": err,
    "p50_ms": round(statistics.median(lat), 2) if lat else None,
    "p95_ms": round(statistics.quantiles(lat, n=100)[94], 2) if len(lat) >= 20 else None,
    "max_ms": round(max(lat), 2) if lat else None,
    "user_id": user_id,
}
json.dump(summary, open(OUT, "w"), ensure_ascii=False, indent=1)
print(json.dumps(summary, ensure_ascii=False))
