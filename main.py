from __future__ import annotations

from src.core.config import config
from src.http_bridge import run_bridged_app
from src.main import app


if __name__ == "__main__":
    try:
        run_bridged_app(
            app,
            public_host=config.server_host,
            public_port=config.server_port,
            log_level=config.log_level,
        )
    except KeyboardInterrupt:
        pass
