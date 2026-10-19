"""Entry point: ``tuya-hub-proxy -c /etc/tuya-hub-proxy/config.yaml``."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal

from .config import load_config
from .health import start_health_server
from .logsetup import setup_logging
from .proxy import Proxy

log = logging.getLogger("tuya_hub_proxy")


async def run(config_path: str) -> None:
    cfg = load_config(config_path)
    setup_logging(cfg.log_level, cfg.log_format)
    proxy = Proxy(cfg)
    await proxy.start()
    health = await start_health_server(cfg.health_host, cfg.health_port, proxy.health)
    log.info("proxy running", extra={"children": len(cfg.children), "hub_ip": cfg.hub.ip})

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()

    log.info("shutting down")
    health.close()
    await proxy.stop()


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="tuya-hub-proxy", description=__doc__)
    ap.add_argument("-c", "--config", default="/etc/tuya-hub-proxy/config.yaml")
    args = ap.parse_args(argv)
    setup_logging()
    asyncio.run(run(args.config))


if __name__ == "__main__":
    main()
