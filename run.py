#!/usr/bin/env python3
"""
run.py  —  sensorflow-local continuous pipeline orchestrator

  Architecture
  ───────────────────────────────────────────────
  INGESTION TIER  (always-on, parallel, auto-restart on crash)
    consumer      — MQTT subscriber  →  60-s buffer  →  MinIO S3
    cdc_handler   — MQTT subscriber  →  DuckDB anomaly_log
    simulator     — MQTT publisher   (NORMAL or ANOMALY mode)

  BATCH TIER  (scheduled, sequential — only if MinIO has data)
    every BATCH_INTERVAL s after an initial WARMUP s:
      glue_etl_local  →  gold_writer  →  nightly_job
      →  grafana_export  →  superset_sync  →  row-count summary

  Per-process output  →  logs/.log
  Pipeline events     →  terminal + logs/pipeline.log
  Press Ctrl+C once to shut everything down cleanly.

  Usage
  ───────────────────────────────────────────────
  python run.py                        normal mode, 5-min batch
  python run.py --mode anomaly         fault-injection mode
  python run.py --batch-interval 120   batch every 2 minutes
  python run.py --warmup 30            first batch after 30 s
  python run.py --skip-etl             skip PySpark on every batch
  python run.py --once                 one batch then exit  (CI / testing)
"""
import os, sys, subprocess, time, threading, signal, logging, argparse, socket
from pathlib import Path

# ── UTF-8 fix for Windows consoles (CP1252 cannot encode → or ═) ──────────
if sys.stdout.encoding and sys.stdout.encoding.lower().replace("-", "") != "utf8":
    sys.stdout = open(sys.stdout.fileno(), mode="w", encoding="utf-8",
                      buffering=1, closefd=False)
if sys.stderr.encoding and sys.stderr.encoding.lower().replace("-", "") != "utf8":
    sys.stderr = open(sys.stderr.fileno(), mode="w", encoding="utf-8",
                      buffering=1, closefd=False)

ROOT    = Path(__file__).parent
VENV_PY = ROOT / ".venv" / "Scripts" / "python.exe"
PY      = str(VENV_PY) if VENV_PY.exists() else sys.executable
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ── logging: terminal (UTF-8) + logs/pipeline.log (UTF-8) ────────────────
_console_handler = logging.StreamHandler(sys.stdout)   # already UTF-8 above

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        _console_handler,
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("run")

_shutdown = threading.Event()

# ── env / data checks ────────────────────────────────────────────
def load_env():
    env_f = ROOT / ".env"
    if not env_f.exists():
        sys.exit("ERROR: .env not found — run 'python dev.py setup' first")
    for raw in env_f.read_text().splitlines():
        s = raw.strip()
        if s and not s.startswith("#") and "=" in s:
            k, v = s.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

def has_raw_data() -> bool:
    try:
        r = subprocess.run(
            ["aws", "s3", "ls", "s3://sensorflow-local/raw/",
             "--recursive", "--profile", "local"],
            capture_output=True, text=True, timeout=10,
        )
        return bool(r.stdout.strip())
    except Exception:
        return False

# ── ingestion service manager ───────────────────────────────────────
class Service:
    BACKOFF      = [5, 10, 20, 40, 60]
    MAX_RESTARTS = 5

    def __init__(self, name, script, extra_env=None):
        self.name     = name
        self.script   = script
        self.env_ext  = extra_env or {}
        self.proc     = None
        self.restarts = 0
        self._lk      = threading.Lock()
        self._logf    = open(LOG_DIR / f"{name}.log", "a", buffering=1,
                             encoding="utf-8")

    def start(self):
        if not self.script.exists():
            log.warning(f"[{self.name}] script not found: {self.script.relative_to(ROOT)}")
            return
        with self._lk:
            env = {**os.environ, "PYTHONPATH": str(ROOT),
                   "PYTHONIOENCODING": "utf-8", **self.env_ext}
            self.proc = subprocess.Popen(
                [PY, str(self.script)],
                env=env, stdout=self._logf, stderr=self._logf,
            )
        log.info(f"[{self.name}] started (PID {self.proc.pid}) -> logs/{self.name}.log")

    @property
    def alive(self):
        with self._lk:
            return self.proc is not None and self.proc.poll() is None

    def watchdog(self):
        if _shutdown.is_set() or self.alive:
            return
        if self.restarts >= self.MAX_RESTARTS:
            return   # gave up after MAX_RESTARTS — check logs/.log
        wait = self.BACKOFF[min(self.restarts, len(self.BACKOFF) - 1)]
        log.warning(
            f"[{self.name}] died — restart {self.restarts+1}/{self.MAX_RESTARTS} in {wait}s"
        )
        time.sleep(wait)
        if not _shutdown.is_set():
            self.restarts += 1
            self.start()

    def stop(self):
        with self._lk:
            if self.proc and self.proc.poll() is None:
                self.proc.terminate()
                try: self.proc.wait(timeout=5)
                except subprocess.TimeoutExpired: self.proc.kill()
        self._logf.close()
        log.info(f"[{self.name}] stopped")

    def status(self):
        if not self.script.exists():           return "missing"
        if self.alive:                         return f"running (PID {self.proc.pid})"
        if self.restarts >= self.MAX_RESTARTS: return "FAILED (max restarts)"
        return "down — restarting..."

# ── batch pipeline ────────────────────────────────────────────────────
def run_batch(skip_etl):
    if not has_raw_data():
        log.info("[batch] MinIO has no raw data yet — skipping this cycle")
        return

    env   = {**os.environ, "PYTHONPATH": str(ROOT), "PYTHONIOENCODING": "utf-8"}
    steps = (
        [] if skip_etl else
        [("etl",     ROOT/"src"/"etl"/"glue_etl_local.py")]
    ) + [
        ("gold",    ROOT/"src"/"gold"/"gold_writer.py"),
        ("grafana", ROOT/"src"/"analytics"/"export_for_grafana.py"),
    ]

    for label, script in steps:
        if _shutdown.is_set(): return
        if not script.exists():
            log.warning(f"[batch/{label}] SKIP — {script.relative_to(ROOT)} not found")
            continue
        t0 = time.time()
        r  = subprocess.run([PY, str(script)], env=env)
        ok = r.returncode == 0
        log.info(f"[batch/{label}] {'OK' if ok else 'FAIL'}  {time.time()-t0:.1f}s")

    # nightly aggregation (test_mode=True — aggregates today's timestamps)
    nj = ROOT / "src" / "aggregation" / "nightly_job.py"
    if nj.exists() and not _shutdown.is_set():
        t0 = time.time()
        r  = subprocess.run(
            [PY, "-c",
             "from src.aggregation.nightly_job import run_nightly; run_nightly(test_mode=True)"],
            env=env,
        )
        log.info(f"[batch/nightly] {'OK' if r.returncode==0 else 'FAIL'}  {time.time()-t0:.1f}s")

    # superset DB sync — copies updated sensorflow.db into the container
    db = ROOT / "data" / "sensorflow.db"
    rps = subprocess.run(["docker", "ps", "--format", "{{.Names}}"],
                         capture_output=True, text=True)
    if "sensorflow-superset" in rps.stdout and db.exists():
        subprocess.run(["docker", "cp", str(db),
                        "sensorflow-superset:/app/data/sensorflow.db"], capture_output=True)
        subprocess.run(["docker", "exec", "-u", "root", "sensorflow-superset",
                        "chmod", "644", "/app/data/sensorflow.db"], capture_output=True)
        log.info("[batch/superset] DB synced -> http://localhost:8088")

    # ensure grafana JSON file server is running on :8765
    try:
        with socket.create_connection(("localhost", 8765), timeout=1): pass
    except OSError:
        gdir = ROOT / "data" / "grafana"
        if gdir.exists():
            flags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0
            subprocess.Popen(
                [PY, "-m", "http.server", "8765"], cwd=str(gdir),
                creationflags=flags,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            log.info("[batch/grafana-server] started on :8765")

    # row-count summary after each batch — immediate freshness confirmation
    try:
        import duckdb
        con = duckdb.connect(str(ROOT/"data"/"sensorflow.db"), read_only=True)
        counts = {}
        for t in ["fact_sensor_readings", "anomaly_log", "hourly_machine_summary"]:
            try:    counts[t] = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            except: counts[t] = "n/a"
        con.close()
        log.info("[batch/db]  " + "  ".join(f"{k}={v}" for k, v in counts.items()))
    except Exception as e:
        log.debug(f"[batch/db] skipped: {e}")

# ── threads ────────────────────────────────────────────────────────────────
def _batch_thread(interval, warmup, skip_etl, once):
    log.info(f"[batch] first run in {warmup}s, then every {interval}s")
    _shutdown.wait(warmup)
    n = 0
    while not _shutdown.is_set():
        n += 1
        log.info(f"[batch] -- cycle {n} " + "-" * 35)
        t0 = time.time()
        run_batch(skip_etl)
        log.info(f"[batch] -- cycle {n} done  ({time.time()-t0:.1f}s) " + "-" * 28)
        if once:
            _shutdown.set(); return
        _shutdown.wait(interval)

def _watchdog_thread(services):
    while not _shutdown.is_set():
        for svc in services:
            if not _shutdown.is_set():
                svc.watchdog()
        _shutdown.wait(10)

def _ticker_thread(services):
    while not _shutdown.is_set():
        _shutdown.wait(30)
        if _shutdown.is_set(): break
        lines = ["-" * 52]
        for svc in services:
            lines.append(f"  {svc.name:<14}  {svc.status()}")
        lines.append("-" * 52)
        print("\n".join(lines), flush=True)

# ── main ──────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="sensorflow-local continuous pipeline")
    ap.add_argument("--mode",           default="NORMAL", metavar="NORMAL|anomaly",
                    help="simulator mode (default: NORMAL)")
    ap.add_argument("--batch-interval", type=int, default=300, metavar="SECS",
                    help="seconds between batch runs (default: 300)")
    ap.add_argument("--warmup",         type=int, default=60,  metavar="SECS",
                    help="seconds before first batch run (default: 60)")
    ap.add_argument("--skip-etl",       action="store_true",
                    help="skip PySpark step on every batch run")
    ap.add_argument("--once",           action="store_true",
                    help="run one batch then exit (CI / testing)")
    args = ap.parse_args()

    load_env()

    log.info("=" * 55)
    log.info("  sensorflow-local  |  continuous pipeline")
    log.info(f"  simulator     : {args.mode.upper()} mode")
    log.info(f"  warmup        : {args.warmup}s   batch interval: {args.batch_interval}s")
    log.info(f"  skip ETL      : {args.skip_etl}")
    log.info(f"  per-process logs -> logs/  |  pipeline log -> logs/pipeline.log")
    log.info("=" * 55)

    # ensure Docker Compose services are running
    r = subprocess.run(
        ["docker", "compose", "-f", str(ROOT/"docker"/"docker-compose.yml"), "ps", "-q"],
        capture_output=True, text=True,
    )
    if not r.stdout.strip():
        log.info("[init] Docker Compose not running — starting ...")
        subprocess.run(["docker", "compose", "-f",
                        str(ROOT/"docker"/"docker-compose.yml"), "up", "-d"])
        log.info("[init] waiting 15 s for services to become ready ...")
        time.sleep(15)

    # start ingestion in order: broker consumers before simulator
    # consumer + cdc_handler must be subscribed before simulator starts publishing
    services = [
        Service("consumer",    ROOT/"src"/"ingestion"/"stream_consumer.py"),
        Service("cdc_handler", ROOT/"src"/"cdc"/"cdc_handler.py"),
        Service("simulator",   ROOT/"src"/"simulator"/"sensor_simulator.py",
                extra_env={"SIM_MODE": args.mode.upper()}),
    ]
    for svc in services:
        svc.start()
        time.sleep(2)   # stagger: let broker accept subscriber before next process

    # daemon threads: batch scheduler, watchdog, status ticker
    threads = [
        threading.Thread(
            target=_batch_thread,
            args=(args.batch_interval, args.warmup, args.skip_etl, args.once),
            daemon=True, name="batch",
        ),
        threading.Thread(
            target=_watchdog_thread, args=(services,),
            daemon=True, name="watchdog",
        ),
        threading.Thread(
            target=_ticker_thread, args=(services,),
            daemon=True, name="ticker",
        ),
    ]
    for t in threads: t.start()

    # Ctrl+C handler
    def _sigint(sig, frame):
        print()
        log.info("Ctrl+C — shutting down ...")
        _shutdown.set()
    signal.signal(signal.SIGINT, _sigint)

    try:
        while not _shutdown.is_set():
            time.sleep(1)
    finally:
        _shutdown.set()
        log.info("Stopping ingestion processes ...")
        for svc in services: svc.stop()
        log.info("Shutdown complete. All logs -> logs/")


if __name__ == "__main__":
    main()