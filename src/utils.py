import os
from pathlib import Path
from loguru import logger
from datetime import datetime, timezone

N_ROWS = int(os.getenv("N_ROWS", "5000"))
SEED = int(os.getenv("SEED", "42"))
GEN_INTERVAL_SECONDS = int(os.getenv("GEN_INTERVAL_SECONDS", "0"))
SOURCE_NAME = os.getenv("SOURCE_NAME", "empresa-simulada")
LANDING_DIR = Path(os.getenv("LANDING_DIR", "/landing"))
OUTPUT_FORMAT = os.getenv("OUTPUT_FORMAT", "csv").lower()      # csv | parquet
CSV_COMPRESSION = os.getenv("CSV_COMPRESSION", "none").lower() # none | gzip

# --- NOVO: intervalo histórico e distribuição temporal ---
DATE_START = os.getenv("DATE_START", "2015-01-01")
DATE_END   = os.getenv("DATE_END",   "2025-12-31")
DATE_DISTRIBUTION = os.getenv("DATE_DISTRIBUTION", "recente").lower()  # uniforme | recente

def parse_date_utc(s: str) -> datetime:
    # Interpreta YYYY-MM-DD (local) e internaliza como UTC 00:00
    dt = datetime.strptime(s, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)

DATE_START_DT = parse_date_utc(DATE_START)
DATE_END_DT   = parse_date_utc(DATE_END)

def ensure_dirs():
    LANDING_DIR.mkdir(parents=True, exist_ok=True)

def info(msg: str):
    logger.info(f"[generator] {msg}")