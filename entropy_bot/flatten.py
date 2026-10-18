"""Emergency close: cancel this bot's orders and reduce-only IOC every position flat.

Same exit path the live bot uses on stop. Does not open anything and does not
arm the dead-man. Exit 0 only when a fresh clearinghouseState shows no position.
"""

from __future__ import annotations

import logging

from entropy_bot.config import Settings, require_signer
from entropy_bot.live import (
    LiveQuoter,
    acquire_instance_lock,
    check_signer_role,
    install_stop_handlers,
)
from entropy_bot.orders import LiveSigner
from entropy_bot.rest import InfoClient
from entropy_bot.status import load_io_markets

log = logging.getLogger("entropy_bot.flatten")

FLATTEN_CLI_TIMEOUT_S = 60.0


def run_flatten(settings: Settings) -> int:
    require_signer(settings)
    signer = LiveSigner(settings.private_key or "", settings.account)
    lock = acquire_instance_lock(signer.account)
    client = InfoClient(settings.api_url, pause_s=0.0)
    try:
        _meta, markets, _ctxs = load_io_markets(client, settings.coins)
        check_signer_role(client, signer)
        quoter = LiveQuoter(settings, markets, client, signer)
        stop = install_stop_handlers()
        quoter.reconcile(force=True)
        timeout = max(settings.stop_flatten_s, FLATTEN_CLI_TIMEOUT_S)
        flat = quoter.flatten_on_stop(None, timeout_s=timeout, stop=stop, deadman=False)
        for coin in markets:
            sides = [s for s in ("B", "A") if quoter.rests[coin][s].occupied()]
            extras = list(quoter.extra_oids.get(coin, []))
            if sides or extras:
                quoter._cancel(coin, sides, extras)
        if flat:
            log.info("FLATTEN done: no open position on %s, bot orders canceled", ",".join(markets))
            return 0
        log.error(
            "FLATTEN NOT CONFIRMED: position %s (%s); close it on the site",
            quoter.open_positions(),
            "unverified" if not quoter.position_confirmed() else "still open",
        )
        return 3
    finally:
        client.close()
        lock.close()
