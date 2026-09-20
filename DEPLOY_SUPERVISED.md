# Ubuntu：每天人工运行 2～3 小时

此分支基于修复版 `475232e`，保留最初 `f861c696` 的 `quoting.py`、下单签名/数量精度、手续费计算和 paper 算法。日常仍为空仓双边 ALO；有仓位时只减仓，沿用 `<6s` 远档、`6–15s` 中档、`≥15s` IOC 的阶梯。网络延迟或限额下，15 秒不是成交保证。

新增 `--supervised-no-deadman` 人工值守模式：显式不申请交易所 `scheduleCancel`，因而不需要该功能的成交量资格，也不会伪造保护成功。必须指定 `--seconds`，大于 0 且不超过 10800 秒。省略新开关仍是原来的严格模式，交易所拒绝保护申请时不允许开仓。

## 一次运行如何结束

- 2 小时用 7200 秒，3 小时用 10800 秒。最后 30 秒停止新开仓；短于 60 秒的直接 CLI 会话最多提前一半时长。
- 先撤受管理挂单并查询确认，再沿用原有只减仓逻辑。交易所报告无挂单、仓位为零后退出，可以比设定时间略早。
- 正常定时结束最多使用结束时刻之后 60 秒的清理窗口。手动 Ctrl+C 或安全拒绝触发停止，最多使用当下之后 60 秒，且不超过前述总上限。清理失败、未知写入结果或残留仓位均非零退出。
- 第一次 Ctrl+C/SIGTERM 停止新开仓并进入上述收尾。第二个信号结束等待，仍尝试最终撤单核对，可能留仓，需要人工查看。
- 时间边界使用 monotonic 时钟，不依赖服务器时区。超时检查发生在网络请求之间；正在进行的请求和最终核对可能让进程退出晚于上述窗口。它不是强制杀进程的硬实时计时器。
- **服务器崩溃、断网、断电或强制 kill 时没有交易所端自动撤单保护。** tmux 只解决 SSH 断连，不解决服务器故障。这不是全天无人值守模式，也没有自动重启、每日定时启动或自动购买额度。
- 仍可能亏损，没有新增价格止损或亏损金额熔断；额度修复不代表请求额度永远不会耗尽。

## 一次性部署

以下使用已有 `entropybot` 用户和现有配置文件，不覆盖旧部署。先停止旧机器人，并在网页确认 io 市场没有遗留仓位和挂单。不要同时启动旧版和新版。

在 root 的 SSH 窗口逐条执行：

```bash
sudo install -d -o entropybot -g entropybot -m 750 /opt/entropy-bot-supervised
sudo -u entropybot -H git clone --branch fix/supervised-sessions --single-branch https://github.com/zhou479/entropy-bot.git /opt/entropy-bot-supervised/repo
sudo -u entropybot -H python3 -m venv /opt/entropy-bot-supervised/repo/.venv
sudo -u entropybot -H /bin/bash -c 'cd /opt/entropy-bot-supervised/repo && .venv/bin/python -m pip install -c constraints-tested.txt -e ".[dev]"'
sudo -u entropybot -H /opt/entropy-bot-supervised/repo/.venv/bin/python -m pip check
sudo -u entropybot -H /bin/bash -c 'cd /opt/entropy-bot-supervised/repo && PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B -m pytest -p no:cacheprovider'
sudo install -d -o entropybot -g entropybot -m 750 /var/log/entropy-bot /var/lib/entropy-bot
```

如果 clone 提示目标已存在，不要删除目录或覆盖，先检查里面是不是之前安装的版本。首次安装缺少 venv、tmux 或 flock 时再安装系统包：

```bash
sudo apt-get update
sudo apt-get install python3-venv tmux util-linux
```

关键是 clone 命令里的 `--branch fix/supervised-sessions`：默认 main 不是这个修复分支。

## 核对配置

```bash
sudo vim /etc/entropy-bot/entropy-live.env
```

保留已有 agent 私钥与主账户地址，不需要重新填写或发送私钥。核对下面这些非秘密字段：

```dotenv
LIVE=0
COINS=io:SNDK
QUOTE_NOTIONAL_USD=50
MAX_LEVERAGE=1
MIN_REPLACE_S=12
FEE_TIER=0
ENTROPY_TIER=1
ENTROPY_SELF_REBATE=0.6
ENTROPY_REFERRAL_REWARD=0.25
ENTROPY_REFERRED_USER_BENEFIT=0
```

这里按此前核对的账户费率配置，不代表所有人的等级。费率字段只参与费用估算，不改变平台实际费率或返佣资格；权益变化时需要重新核对。200 USDC 是你计划存入的账户本金，不是一个新的配置字段。每侧 50 USDC 是名义金额，不是亏损上限。

配置中的 `LIVE=0` 可以保留。启动脚本只有在你当次输入 RUN 后才对该进程设置 `LIVE=1`。源配置仍为 0，可减少误启动风险。

## 先测试，再延长

先运行不签名的 paper 检查：

```bash
sudo -u entropybot -H /bin/bash -c 'set -a; . /etc/entropy-bot/entropy-live.env; set +a; export LIVE=0 HYPERLIQUID_PRIVATE_KEY=; cd /opt/entropy-bot-supervised/repo; exec .venv/bin/python -m entropy_bot paper --seconds 60'
```

paper 通过只能说明行情连接和基本计算路径可用，不能验证成交、亏损或实盘清理。第一次新版实盘先做 300 秒检查。可以在 entropybot 的 tmux 会话中运行，避免笔记本关盖直接中断 SSH：

```bash
sudo -u entropybot -H tmux new-session -s entropy-live
```

进入 tmux 后运行：

```bash
bash /opt/entropy-bot-supervised/repo/scripts/run-supervised.sh 300
```

看清屏幕显示的币种、金额和杠杆，输入 `RUN` 回车，才会开始真实交易。没有输入、输入不匹配或 60 秒超时都不交易。Xterminal 重放命令时也会重新等待确认。

这次退出后，应看见 `SESSION DONE` 和 `SHUTDOWN confirmed flat`，再去网页核对持仓为零、挂单为空。若有 `SAFETY STOP` / `cleanup timed out` / `SHUTDOWN NOT CLEAN`，先处理残留，不要盲目重启。

检查正常后，每天手动选择其中一个命令（不要一起运行）：

```bash
# 2 小时
bash /opt/entropy-bot-supervised/repo/scripts/run-supervised.sh 7200

# 或 3 小时
bash /opt/entropy-bot-supervised/repo/scripts/run-supervised.sh 10800
```

每次仍要输入 RUN。运行期间每分钟有一条 `SESSION heartbeat`，表示程序循环仍在运行，不代表成交或盈利。不要用 `timeout`/自动 Restart 再包一层以免中断清理。

tmux 中 Ctrl+B 后按 D 只分离终端，机器人仍然运行。重新连接执行：

```bash
sudo -u entropybot -H tmux attach-session -t entropy-live
```

想提前停止时，在机器人所在终端按一次 Ctrl+C，然后等待清理结果，检查网页。程序正常结束后不会自行开始下一轮。

## 日志与运行锁

脚本会显示独立日志路径：`/var/log/entropy-bot/live-时间-随机值.log`，权限默认仅运行用户可读写。用实际显示的文件名查看：

```bash
sudo tail -n 100 /var/log/entropy-bot/实际显示的日志文件.log
```

所有使用默认目录的此脚本会话共用 `/var/lib/entropy-bot/live.lock`；已有会话运行时第二次启动会拒绝。不要删除锁文件来强行启动。这个锁不能阻止旧版程序、直接 Python 命令、其他服务器或手工交易，因此账户仍须独占。

## 改动依据与测试边界

- [Hyperliquid 的 scheduleCancel 和 IOC 文档](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint)：定时撤挂单与平仓不同，IOC 可以部分成交。
- [Hyperliquid 官方 SDK 示例](https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/examples/basic_order.py)：显式检查业务返回和订单状态，不能仅凭请求返回就认定成功。
- [NautilusTrader 的 Hyperliquid 集成](https://nautilustrader.io/docs/nightly/integrations/hyperliquid/)：参考其未知交易结果需核对、避免盲目重发的执行原则；本代码不是该项目的复制或认证。
- [Python signal 文档](https://docs.python.org/3/library/signal.html)：停止信号需要处理，SIGKILL 不能捕获。

回归测试完全模拟，网络被禁用；包括 2/3 小时虚拟计时、限额、部分成交、撤单成交竞争、信号、断网/未知状态和清理超时。仓库另带 Ubuntu 24.04 / Python 3.12 的离线 CI。测试通过不证明实盘收益、返佣到账或异常情况下必然平仓。

`SAFETY_REVIEW.md` 记录的是此前版本的历史审查；本文件描述新模式。前一轮手工修改说明不需要再应用到这个分支。

## 用于自检的五个问题

1. tmux 分离后，机器人会停止吗？（不会。）
2. 定时结束时还有仓位，程序会立即退出吗？（先尝试有界减仓，失败则报错，需要人工核对。）
3. 服务器直接断电后，会有交易所自动撤单保护吗？（人工值守模式没有。）
4. 50 USDC 的报价意味着最多亏多少已被锁定吗？（没有锁定亏损上限。）
5. `SESSION heartbeat` 能证明这一分钟赚了钱吗？（不能，只表示循环仍在运行。）
