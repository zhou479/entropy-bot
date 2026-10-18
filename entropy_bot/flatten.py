"""Emergency close: cancel this bot's orders and reduce-only IOC every position flat.

Same exit path the live bot uses on stop. Does not open anything and does not
arm the dead-man. Covers every supported coin (io:ANTH and io:SNDK), not just
COINS, so a position left by an older config is not missed. Exit 0 only when
a fresh clearinghouseState shows no position and every bot order is canceled.
"""

from __future__ import annotations

import logging

from entropy_bot.coins import ALLOWED_COINS
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
        _meta, markets, _ctxs = load_io_markets(client, ALLOWED_COINS)
        check_signer_role(client, signer)
        quoter = LiveQuoter(settings, markets, client, signer)
        stop = install_stop_handlers()
        # Learn this bot's resting orders first so each IOC cancels them before it goes out.
        quoter.reconcile(force=True)
        timeout = max(settings.stop_flatten_s, FLATTEN_CLI_TIMEOUT_S)
        flat = quoter.flatten_on_stop(None, timeout_s=timeout, stop=stop, deadman=False)
        # Re-read the book of orders after the IOCs, then cancel what is left.
        orders_known = quoter.reconcile(force=True)
        orders_clean = orders_known and all(quoter.cancel_coin(coin) for coin in markets)
        if flat and orders_clean:
            log.info("FLATTEN done: no open position on %s, bot orders canceled", ",".join(markets))
            return 0
        if not flat:
            log.error(
                "FLATTEN NOT CONFIRMED: position %s (%s); close it on the site",
                quoter.open_positions(),
                "unverified" if not quoter.position_confirmed() else "still open",
            )
        if not orders_clean:
            log.error(
                "FLATTEN: bot order cleanup unconfirmed (%s); check open orders",
                "open-order query failed" if not orders_known else "cancel failed",
            )
        return 3
    finally:
        client.close()
        lock.close()
