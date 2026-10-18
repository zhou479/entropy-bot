# entropy-bot

**This Python package is the real bot.** It talks only to official Hyperliquid HTTP + WebSocket. The Chrome Tampermonkey userscript (`entropy-desk.user.js`) is optional / legacy — do not depend on it for live MM.

EntropyIO HIP-3 perps on Hyperliquid: **`io:SNDK`** (current ops, default) and **`io:ANTH`** (supported, off by default). DEX name is **`io`** (`fullName` = EntropyIO). `xyz:SNDK`, `vntl:ANTHROPIC` are other venues; `io:OAI` / `io:IONQ` and other `io` listings are not traded by this bot. All refused.

---

## English

### What this is

A Python 3.11+ CLI on the official APIs only:

- HTTP: `https://api.hyperliquid.xyz` (`/info`, `/exchange`)
- WS: `wss://api.hyperliquid.xyz/ws`

No entropy.io scraping, no third-party order relays, no userscript bridge.

**Paper / shadow mode is the default.** Live signing happens only when `LIVE=1` **and** `HYPERLIQUID_PRIVATE_KEY` is set.

`paper` runs the **same** `LiveQuoter` as `live` (quotes, per-side updates, flatten ladder, throttles, risk gates, dead-man) against an in-process simulated exchange fed by the live WS book. Nothing is signed or sent. It ends with a summary (maker/taker fills, volume, realized PnL, number of signed actions it would have cost). Fill model: ALO that would cross is rejected; a rest fills when the live touch trades through its price (up to the touch size); IOC fills against the touch.

### Live MM (userscript 1.6.1 port)

Per coin: at most **1 buy + 1 sell**. Isolated-only. Builder = Entropy `0xcD254d2A328f7f67C7c6FEf930A4757516F7b601` fee **0**. Tick is inferred from the live bid/ask increment (0.1 at SNDK ~1295 / ANTH ~1890), cross-checked with the 5-sigfig `tick_size`; the smaller wins.

**FLAT (`szi==0`)** — two-sided ALO, `reduceOnly=false`:

- Spread ≤ 2 ticks: join exact bid/ask.
- Spread > 2 ticks: `buy=floorToTick(mid-tick)`, `sell=ceilToTick(mid+tick)`, clamp inside the book, `buy < sell`. Example `1985.00 / 1986.90 / tick 0.1` → buy **1985.8** sell **1986.1**.
- Rests within `REQUOTE_TOLERANCE_TICKS` (default 1) of the target stay put — no cancel, queue priority kept. Untouched flat rests are refreshed after `STALE_QUOTE_S` (default 120s, 0 = off).
- Each side is managed on its own: only the side that moved is canceled and re-placed.

**IN POSITION** — flatten only, never add. Size `min(notionalSz, |szi|)`. Age clock starts when `|szi|` first becomes nonzero; resets on flat.

- Long: only reduce-only sell. Short: only reduce-only buy.
- `<6s`: reduce-only ALO at the far touch (long sell@ask, short buy@bid).
- `6–15s`: reduce-only ALO improved to mid (or 1 tick inside the near touch).
- `≥15s`: cancel rests, reduce-only **IOC** take on the near touch (long sell@bid, short buy@ask).

- `|position| × mid ≥ MAX_POSITION_USD` (default 3 × `QUOTE_NOTIONAL_USD`): skip the ladder, IOC now.
- IOC crosses `TAKE_SLIPPAGE_TICKS` (default 2) past the touch. A miss doubles the retry gap (0.4s → 5s cap); progress resets it.

ALO / Bad Alo Px / post-only reject: log, cool that side down 2s, requote; the loop does not stop. A price-only change is **throttled** by `MIN_REPLACE_S` (default 12s) per coin. A stage / TIF / reduce-only change (e.g. a fill flips flat → long) is **not** throttled: the opening side is canceled at once so the bot never adds to a position. A requote only ever touches **its own coin**. Cancels go out by oid (by cloid only when no oid is known) — never both. Never place a side that already has an oid/cloid. HIP-3 oids exceed int32 — they are never truncated.

The rest cache follows the WS `orderUpdates` + `userFills` streams and is cross-checked against `frontendOpenOrders` (`dex:"io"`) every 20s. Unknown bot orders (EBOT cloid, e.g. from a previous run) are adopted; orders the bot did not place — your manual orders and TP/SL triggers — are never canceled (a warning is logged). An `/exchange` timeout marks the coin "unknown": nothing new is placed there until a reconcile succeeds.

### Request weight / write throttle

Hyperliquid address-level cumulative request weight (`nRequestsUsed` vs `nRequestsCap`) is 10000 + 1 per USDC of lifetime volume, and every order / cancel inside a batch counts. **Waiting does not restore the cap; fills do.**

- `userRateLimit` is polled every 120s and logged (`request budget used=… cap=… headroom=… writes_since_last=…`). Below `4 × REQUEST_BUDGET_RESERVE` headroom reprices slow down 3×; below `REQUEST_BUDGET_RESERVE` (default 500) no new opening quotes go out (resting quotes stay — their fills grow the cap).
- After `Too many cumulative requests` the bot logs **once** and pauses new / repriced quotes for `max(MIN_REPLACE_S, 30s)`. **Cancels, reduce-only flatten and the dead-man still go out** (cancels have a larger cap; while limited the exchange still allows one request per 10s, so IOC retries space out to 10s). It never buys `reserveRequestWeight` or places taker-unlock spam.

IP-level weight (1200/min) is kept low: inventory via `clearinghouseState` (weight 2) every 1s and right after a fill; fills come from WS with an incremental `userFillsByTime` REST backup every 60s; `frontendOpenOrders` every 20s; no courtesy sleeps in the live loop. The loop wakes on each WS book update instead of polling.

WS `l2Book` + `activeAssetCtx` per coin. REST book only if WS is stale >15s. Open-order queries always send `dex:"io"`. A null/429 response does **not** wipe the local rest cache.

### Risk gates

Opening (flat) quotes are canceled and not re-placed while any of these hold; an open position is still flattened:

- `RTH_ONLY_COINS` (default `io:SNDK`): outside NYSE regular hours. Static NYSE calendar (holidays + 13:00 early closes) in `entropy_bot/session.py`; extend it yearly.
- `ORACLE_MAX_DEV_BPS` (default 50): book mid more than N bp from `oraclePx`.
- `MAX_DAILY_LOSS_USD` (default 0 = off): realized `closedPnl − fee` for the UTC day (from `userFills`, today's history included at start) below −N.

New opening quotes also wait while the dead-man is not armed.

### Fill-rate / +3s markout (logging only)

Live prints greppable `FILL_DIAG {...}` JSON lines. Quote path, flatten ladder, sizes, and prices are unchanged.

Per fill (after 3s, using the already-subscribed `l2Book`; no extra WS):

- `coin` (`io:ANTH` / `io:SNDK`), `side` (buy/sell), `fill_px`, `mid_at_fill`, `mid_3s`
- `markout_bps`: buy `(mid_3s - fill_px)/fill_px * 1e4`; sell `(fill_px - mid_3s)/fill_px * 1e4`. If the book is stale >15s at +3s, `mid_3s` is null and markout is skipped.
- `spread` (ask−bid) and `spread_bps` vs mid at fill time
- `crossed` (taker), `fee`, `closed_pnl` from the fill; `day_net_pnl` for the UTC day
- counters on the same row: `quotes`, `fills`, `fill_rate` (`fills/quotes`, null if no quotes), `taker_fills`

`fills` counts distinct maker orders (a partially filled quote counts once), so `fill_rate ≤ 1`; IOC flatten fills are `taker_fills`. ANTH and SNDK never share a bucket. SNDK rows also have `session=rth` or `session=ah` (NYSE calendar, see Risk gates). After-hours markout is kept in the AH bucket only; it is never folded into an RTH average. Rows go to stdout once (not duplicated into the log); in-memory history is bounded.

Fills come from the official `userFills` WS on the same connection (snapshot ignored) with a REST `userFills` poll as backup. Quote counts increment only after an ALO rest is accepted, not after IOC flatten.

### Dead-man (account-wide)

While `live` is running the bot sends `scheduleCancel` at **now+`DEADMAN_AHEAD_S`** (default 60s) and renews **only when a third of it is left** (default <20s) — about 90 writes/hour instead of ~300 at +20s/<8s. Official minimum is 5s. On 429 it falls back to **+6s**. A failed renew (request weight, or Hyperliquid's **10 triggers per UTC day** cap) is retried every 10s and pauses new opening quotes until it arms again.

**This is account-wide.** When the timer fires, Hyperliquid cancels **all** open orders on the master account — not just `io:ANTH` / `io:SNDK`. It **only cancels orders; it never closes a position.**

### Stopping: flatten, then verify

On Ctrl-C, SIGTERM (`systemctl stop`) or the end of `--seconds`:

1. Opening quotes off and canceled.
2. Every open position is closed with a reduce-only IOC for the **whole** size (an exact close is exempt from the $10 minimum), crossing `TAKE_SLIPPAGE_TICKS`, retried until flat or `STOP_FLATTEN_S` (default 30s). The dead-man keeps renewing meanwhile.
3. Remaining bot orders canceled, then a **fresh** `clearinghouseState` must show no position.
4. Only then the dead-man is cleared and the process exits 0 (`STOP clean: flat, no bot orders, dead-man cleared`).

If the position could not be closed or read, the log says `STOP NOT FLAT` / `STOP NOT VERIFIED` and the process exits **3**. A second Ctrl-C skips the flatten wait (still cancels). `STOP_FLATTEN_S=0` keeps the old cancel-only stop, but an open position is still reported and exits 3.

A hard kill (`kill -9`, OOM, power loss) cannot flatten: the dead-man cancels orders after `DEADMAN_AHEAD_S`, and the next start sees the position and runs the flatten ladder first. `python -m entropy_bot flatten` closes everything by hand (same exit path, up to 60s, exit 0 only when verified flat).

### Startup guards

- `userRole` of the signing key must match `HYPERLIQUID_ACCOUNT` (an agent key without the right master, or a non-agent key with a different account, is refused — the bot would read one account and trade another).
- Local clock more than 15s off Hyperliquid is refused (>2s warns); dead-man deadlines are absolute.
- One `live` / `flatten` process per master account (lock file in the temp dir).
- Orders are sized to at least $10 (the exchange minimum); any other per-order reject (margin, size, …) backs that side off 2s → 4s → … 60s instead of retrying every loop.

On a clean stop the bot sends `scheduleCancel` **without** `time` (clears the timer) after cancelling its rests. If info is rate-limited it leaves a +6s cancel instead of assuming the book is empty.

### Agent / master signing

The userscript signs with Entropy's local IndexedDB **agent**, not `window.ethereum`. The VPS bot does the same job with env vars:

| Variable | Role |
| --- | --- |
| `HYPERLIQUID_PRIVATE_KEY` | Signing key. Master **or** an approved API/agent key. Never logged. |
| `HYPERLIQUID_ACCOUNT` | Master address that holds positions. Required when the key is an agent. |

Official SDK path (verified against `hyperliquid-python-sdk` + docs):

```text
Exchange(wallet=agent_key, account_address=master, vault_address=None)
sign_l1_action(..., vaultAddress=None)
```

`vaultAddress` is **only** for vault / subaccount trading and is hashed into the connection id. Putting the master there is wrong. An approved agent signs L1 actions with its own key; the exchange maps it to the master via `extraAgents`. Info queries (`clearinghouseState`, `frontendOpenOrders`) use `HYPERLIQUID_ACCOUNT`.

Approve the agent once on entropy.io / Hyperliquid (same as generating the userscript agent). The master must also have approved the Entropy builder at fee 0% (the userscript's `approveBuilderFee`).

### HIP-3 asset IDs

Looked up every run (do not hardcode forever):

```
asset_id = 100000 + perp_dex_index * 10000 + index_in_meta
```

- `perp_dex_index` = index of `{"name":"io"}` in `{"type":"perpDexs"}` (currently **10**).
- `index_in_meta` = index in `{"type":"meta","dex":"io"}` (ANTH=1, SNDK=2; skip delisted OAI/IONQ).
- Names are case-sensitive: `io:ANTH`, `io:SNDK`.

### Commands

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e ".[dev]"   # optional, for pytest

cp .env.example .env      # edit locally; never commit secrets

python -m entropy_bot status          # public API, no key (+ request budget if HYPERLIQUID_ACCOUNT set)
python -m entropy_bot paper           # full live logic vs simulated exchange, no signing
python -m entropy_bot paper --seconds 600
RTH_ONLY_COINS= python -m entropy_bot paper --seconds 600   # try SNDK outside NYSE hours
python -m entropy_bot live            # refuses unless LIVE=1 and a key
python -m entropy_bot cancel          # cancel this bot's cloIDs (needs key) — does NOT close positions
python -m entropy_bot flatten         # emergency: cancel bot orders + reduce-only IOC every position flat (needs key)
```

`status` prints io meta, books, Entropy Partner T4, self rebate, growth mode, default TIF and (with `HYPERLIQUID_ACCOUNT`) the address request budget — no key required.

### VPS

```bash
# on the VPS, as a dedicated user
git clone https://github.com/zhou479/entropy-bot
cd entropy-bot
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# set LIVE=1
# set HYPERLIQUID_PRIVATE_KEY to the API/agent key (0x…)
# set HYPERLIQUID_ACCOUNT to the master 0x… that holds ANTH/SNDK
# QUOTE_NOTIONAL_USD=50
# COINS=io:SNDK            (default)
# keep the clock synced (chrony / systemd-timesyncd): dead-man times are absolute

# first: a paper run against the live book
python -m entropy_bot paper --seconds 600

# tmux (simple)
tmux new -s entropy
python -m entropy_bot live
# detach: Ctrl-b d

# or systemd: deploy/entropy-bot.service (Restart=on-failure, SIGTERM clean stop);
# the dead-man (+60s, account-wide) covers a hard kill
```

Ctrl-C (or `systemctl stop`) closes the position before exiting; check the last line is `STOP clean`. If the process dies without a clean stop, `scheduleCancel` fires and cancels **every** resting order on that master — the position stays open until the bot restarts or you run `flatten`.

### Config (env / `.env`)

| Variable | Default | Notes |
| --- | --- | --- |
| `LIVE` | `0` | Live only if `1` **and** a private key is set |
| `HYPERLIQUID_PRIVATE_KEY` | unset | Agent or master key. Required for `live` / `cancel`. Never commit |
| `HYPERLIQUID_ACCOUNT` | derived from key | Master address. Set this when the key is an agent |
| `COINS` | `io:SNDK` | Case-sensitive; foreign venues rejected. `io:ANTH,io:SNDK` to add ANTH back |
| `RTH_ONLY_COINS` | `io:SNDK` | Only open quotes in NYSE regular hours; flatten any time. Empty = 24/7 |
| `MIN_REPLACE_S` | `12` | Per-coin seconds between price reprices of a live rest. Stage changes and the ≥15s IOC take are not gated |
| `REQUOTE_TOLERANCE_TICKS` | `1` | Keep a rest within N ticks of target |
| `STALE_QUOTE_S` | `120` | Refresh an untouched flat rest after N s. `0` = off |
| `DEADMAN_AHEAD_S` | `60` | Account-wide `scheduleCancel` horizon (min 10), renewed at a third left |
| `TAKE_SLIPPAGE_TICKS` | `2` | Flatten IOC crosses N ticks past the touch |
| `ORACLE_MAX_DEV_BPS` | `50` | No opening quotes while mid is N bp from oracle. `0` = off |
| `MAX_POSITION_USD` | 3 × notional | Position notional that goes straight to IOC |
| `MAX_DAILY_LOSS_USD` | `0` | Stop opening quotes for the UTC day below −N realized net PnL. `0` = off |
| `REQUEST_BUDGET_RESERVE` | `500` | Headroom below which no new opening quotes go out |
| `STOP_FLATTEN_S` | `30` | On stop, reduce-only IOC the position flat for up to N s, then verify. `0` = cancel only (still reported) |
| `QUOTE_NOTIONAL_USD` | `50` | Notional per side (live and paper), ≥ $10 |
| `QUOTE_OFFSET_TICKS` | `2` | Legacy. Live ignores it |
| `MAX_LEVERAGE` | `2` | Capped at each market's live meta max (currently ANTH 6 / SNDK 15) |
| `FEE_TIER` | `4` | Official HL perp volume tier (0.028% / 0%) |
| `ENTROPY_TIER` | `4` | Entropy Partner. Label only unless other rebate env overrides |
| `ENTROPY_SELF_REBATE` | `2.0` | 200% of Entropy's share of the HIP-3 fee (not the full user fee) |
| `ENTROPY_REFERRAL_REWARD` | `1.0` | Display / future invitee reward. Not applied to own fills |
| `ENTROPY_REFERRED_USER_BENEFIT` | `0` | Optional extra on own fills. Default off |
| `REFERRAL_DISCOUNT` | `0` | Optional HL-side gross taker multiplier |
| `MAKER_REBATE_BPS` | unset | Optional HL ALO maker-share rebate in bp |
| `HYPERLIQUID_API_URL` | official mainnet | `https://` on `*.hyperliquid.xyz` (hostname checked) |
| `HYPERLIQUID_WS_URL` | official mainnet | `wss://` on `*.hyperliquid.xyz` (hostname checked) |

Markets are **isolated-only**. Live sets isolated leverage per coin and never uses cross. Default TIF is ALO for two-sided quotes; flatten ≥15s may IOC take.

### Tests

```bash
pip install -e ".[dev]"
ruff check entropy_bot tests
pytest
```

CI (`.github/workflows/ci.yml`) runs both on 3.11–3.13.

---

## 中文

### 这是什么

**真正的实盘机器人是这个 Python 包**，只走官方接口。Chrome 油猴脚本是可选/遗留，实盘做市不要再依赖它。

只交易 Hyperliquid 上 **EntropyIO HIP-3**：

- `io:SNDK`（逐仓，当前 meta 最高杠杆 15）—— **当前默认只跑 SNDK**
- `io:ANTH`（逐仓，当前 meta 最高杠杆 6）—— 仍支持，`COINS=io:ANTH,io:SNDK` 即可加回

DEX 名是 **`io`**。不要用 `xyz` 或 `vntl`。

接口：`https://api.hyperliquid.xyz` 与 `wss://api.hyperliquid.xyz/ws`。

### 实盘策略（从 userscript 1.6.1 移植）

每币最多 1 买 + 1 卖。空仓双边 ALO：价差 ≤2 tick 贴买卖一；价差 >2 tick 往中间贴（例 1985.00/1986.90/tick 0.1 → 买 1985.8 卖 1986.1）。有仓只减仓：`<6s` 远档 ALO，`6–15s` 中间 ALO，`≥15s` 撤后 IOC 吃单（多穿 `TAKE_SLIPPAGE_TICKS`=2 tick，没吃到则重试间隔翻倍，最长 5 秒）。仓位名义超过 `MAX_POSITION_USD`（默认 3×单边名义）直接 IOC。ALO 被拒不停止。Builder 是 Entropy、费率 0。

省写请求：两侧分开管理，只撤换变动的那一侧；挂单离目标价 ≤`REQUOTE_TOLERANCE_TICKS`（默认 1 tick）就不动（保留排队位置）；纯价格变动每币至少隔 `MIN_REPLACE_S`（默认 12 秒）才换；空仓挂单 `STALE_QUOTE_S`（默认 120 秒）未成交才刷新。撤单只按 oid（没有 oid 才按 cloid），不再重复撤。一个币重挂只动自己的单。**成交后（空仓→有仓）立刻撤掉加仓方向的单**，不受 12 秒节流。

请求权重（`Too many cumulative requests`）等不等都不会把额度补回来，只有成交能补。每 120 秒查一次 `userRateLimit` 并打日志；余量 < 4×`REQUEST_BUDGET_RESERVE` 时换价放慢 3 倍，< `REQUEST_BUDGET_RESERVE`（默认 500）时不再挂新的开仓单（已挂的保留，成交能涨额度）。命中权重错误后只打一次日志，暂停新挂/换价 `max(MIN_REPLACE_S, 30s)`，**但撤单、减仓、dead-man 照常发**（撤单额度更大）。不买 `reserveRequestWeight`、不加 taker-unlock。

风控闸门（只停开仓单，有仓照样平）：`RTH_ONLY_COINS`（默认 SNDK，只在纽约 RTH 开仓，含 NYSE 节假日和 13:00 提前收盘，日历在 `entropy_bot/session.py`，每年需补充）；`ORACLE_MAX_DEV_BPS`（默认 50bp，盘口 mid 偏离 oracle 过大）；`MAX_DAILY_LOSS_USD`（默认关闭，按 UTC 日的已实现 `closedPnl − fee`）；dead-man 未成功设置时也不挂新开仓单。

挂单缓存跟随 WS `orderUpdates` / `userFills` 实时更新，每 20 秒用 `frontendOpenOrders` 对账；下单超时视为状态未知，对账成功前该币不再下新单。不是本机器人下的单（你手动挂的单、止盈止损触发单）**一律不撤**，只打警告。

下单数量保证 ≥ $10（交易所最低额，向下取整不够时进一个最小单位）；其它拒单（保证金不足等）按侧退避 2s → 4s → … 最长 60s，不会每轮重试烧额度。

启动检查：签名私钥的 `userRole` 必须和 `HYPERLIQUID_ACCOUNT` 对得上（否则会出现"查 A 账户的仓位、在 B 账户下单"）；本机时钟偏差 > 15 秒拒绝启动；同一主账户只允许一个 `live`/`flatten` 进程。

Tick 从盘口买卖价增量推断，不用 `tick_size(mid)`。

`FILL_DIAG` JSON 行：成交价、成交时 mid、+3s mid、方向化 markout（买后 mid 涨为正）、价差、`crossed`（是否吃单）、`fee`、`closed_pnl`、当日净盈亏 `day_net_pnl`，以及按币分开的报价次数 / 成交次数（同一挂单多次部分成交只算一次）/ fill rate / taker 成交数。SNDK 带 `session=rth|ah`（NYSE 日历）。AH markout 单独桶，不并进 RTH。

### 签名 / agent

`HYPERLIQUID_PRIVATE_KEY` 可以是主钱包，也可以是已批准的 API/agent 私钥（和网页 IndexedDB agent 同类）。`HYPERLIQUID_ACCOUNT` 是持仓的主地址。官方 SDK：用 agent 钥签名，`vaultAddress=None`；`vaultAddress` 只用于金库/子账户，**不要**填主地址。查询仓位和挂单用主地址。

### Dead-man（账户级）

运行中把 `scheduleCancel` 设到 **现在+`DEADMAN_AHEAD_S`**（默认 60 秒），剩余不到三分之一（默认 <20 秒）才续期，每小时约 90 次写（原先 +20s/<8s 约 300 次）。官方最短 5s；429 则 +6s。续期失败（权重耗尽，或官方**每 UTC 日最多触发 10 次**）每 10 秒重试一次，期间不挂新开仓单。到点后会撤销该主账户上的**全部**挂单，不限于本机器人的币。**dead-man 只撤单，不平仓。**

### 停止 = 先平仓、再核对

Ctrl-C、SIGTERM（`systemctl stop`）或 `--seconds` 到时：

1. 停止并撤掉开仓单；
2. 对每个仓位发**全量** reduce-only IOC（刚好平掉整仓不受 $10 下限限制），多穿 `TAKE_SLIPPAGE_TICKS`，反复直到平掉或 `STOP_FLATTEN_S`（默认 30 秒）超时，期间 dead-man 继续续期；
3. 撤掉剩余挂单，再用**新拉取**的 `clearinghouseState` 确认仓位为 0；
4. 确认后才清除 dead-man，退出码 0，日志最后一行是 `STOP clean: flat, no bot orders, dead-man cleared`。

平不掉或读不到仓位：日志 `STOP NOT FLAT` / `STOP NOT VERIFIED`，退出码 **3**，需要你手动处理。再按一次 Ctrl-C 会跳过等待（仍会撤单）。`STOP_FLATTEN_S=0` 恢复"只撤单"的旧行为，但有仓位仍会报错并以 3 退出。

`kill -9`、OOM、断电等硬杀无法平仓：dead-man 在 `DEADMAN_AHEAD_S` 后撤单，仓位保留到下次启动（启动时发现仓位会先走平仓阶梯），或手动执行 `python -m entropy_bot flatten`。

### 命令

```bash
python -m entropy_bot status   # 公共行情，不需要私钥（设了 HYPERLIQUID_ACCOUNT 会显示请求额度）
python -m entropy_bot paper    # 与实盘同一套逻辑 + 模拟撮合，不签名；结束时打印成交/盈亏/写请求数汇总
RTH_ONLY_COINS= python -m entropy_bot paper --seconds 600   # 非美股时段也想试跑 SNDK
python -m entropy_bot live     # 实盘；缺 LIVE=1 或私钥会直接拒绝
python -m entropy_bot cancel   # 撤销本机器人的 cloid（不平仓）
python -m entropy_bot flatten  # 紧急平仓：撤本机器人挂单 + reduce-only IOC 平掉所有仓位，核对为 0 才返回 0
```

VPS：`.env` 里设 `LIVE=1`、agent 私钥、主地址；先跑 `paper --seconds 600` 看汇总，再用 tmux 或 `deploy/entropy-bot.service`（systemd）跑 `python -m entropy_bot live`。服务器时钟要同步（chrony）。进程被硬杀时靠账户级 dead-man（60 秒）撤单。

### 安全

- 仓库里没有密钥。把私钥只放在本地 `.env`。
- 日志只打地址，不打印私钥。
- `live` 没有 `LIVE=1` + 密钥会退出。
- 载荷里不会出现 `xyz:SNDK` / `vntl:ANTHROPIC`。
