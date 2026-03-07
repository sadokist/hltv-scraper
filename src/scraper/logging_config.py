"""Logging configuration for the HLTV scraper pipeline.

Provides console + file + database logging. Console shows INFO+ with concise
timestamps; the log file captures DEBUG+ with full timestamps and logger names;
the database handler stores INFO+ for dashboard viewing.
"""

import logging
from datetime import datetime
from pathlib import Path


class DbLogHandler(logging.Handler):
    """Logging handler that writes records into the scraper_logs table.

    Accepts a psycopg2 connection. Inserts are autocommit so they survive
    crashes. Silently drops records if the DB write fails.
    """

    def __init__(self, conn, level: int = logging.INFO) -> None:
        super().__init__(level)
        self._conn = conn
        # Enable autocommit on a separate connection for log writes
        self._conn.autocommit = True

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            with self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO scraper_logs (level, logger, message) VALUES (%s, %s, %s)",
                    (record.levelname, record.name, msg),
                )
        except Exception:
            pass  # never let logging break the scraper


def setup_logging(
    data_dir: str = "data", console_level: int = logging.INFO, db_conn=None
) -> Path:
    """Configure logging with console and file handlers.

    Creates a timestamped log file under ``{data_dir}/logs/`` and attaches
    two handlers to the root logger:

    * **Console** -- ``console_level`` (default INFO), short time format.
    * **File** -- DEBUG, full datetime with logger name.

    Existing handlers on the root logger are cleared first so that calling
    this function multiple times (e.g. in tests) does not produce duplicate
    output.

    Args:
        data_dir: Base data directory. ``logs/`` is created inside it.
        console_level: Minimum level for console output.

    Returns:
        Path to the newly created log file.
    """
    log_dir = Path(data_dir) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
    log_file = log_dir / f"run-{timestamp}.log"

    # Root logger -- capture everything; handlers decide what to emit.
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers.clear()

    # Console handler: concise format with short time.
    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)-5s %(message)s",
            datefmt="%H:%M:%S",
        )
    )
    root.addHandler(console)

    # File handler: full format with logger name for diagnostics.
    file_handler = logging.FileHandler(str(log_file), encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)-5s [%(name)s] %(message)s")
    )
    root.addHandler(file_handler)

    # Database handler: INFO+ for dashboard viewing.
    if db_conn is not None:
        try:
            db_handler = DbLogHandler(db_conn, level=logging.INFO)
            db_handler.setFormatter(logging.Formatter("%(message)s"))
            root.addHandler(db_handler)
        except Exception:
            root.warning("Failed to attach DB log handler")

    # Suppress noisy third-party loggers.
    logging.getLogger("nodriver").setLevel(logging.WARNING)
    logging.getLogger("uc").setLevel(logging.WARNING)

    return log_file
