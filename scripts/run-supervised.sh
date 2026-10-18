#!/usr/bin/env bash
# Ubuntu 人工启动入口；每次确认、有界时长、同用户同服务器互斥、日志私有。
set -euo pipefail
umask 077

session_seconds="${1:-7200}"
if (( $# > 1 )) || [[ ! "$session_seconds" =~ ^[1-9][0-9]{0,4}$ ]] ||
   (( session_seconds < 60 || session_seconds > 10800 )); then
    printf '%s\n' '用法：bash scripts/run-supervised.sh 秒数（60～10800；默认7200）' >&2
    exit 2
fi

repo_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
env_file="${ENTROPY_ENV_FILE:-/etc/entropy-bot/entropy-live.env}"
log_dir="${ENTROPY_LOG_DIR:-/var/log/entropy-bot}"
state_dir="${ENTROPY_STATE_DIR:-/var/lib/entropy-bot}"
python_bin="$repo_dir/.venv/bin/python"

if [[ ! -x "$python_bin" || ! -r "$env_file" || ! -w "$log_dir" || ! -w "$state_dir" ]]; then
    printf '%s\n' '缺少虚拟环境、配置权限或日志/锁目录；请先按 DEPLOY_SUPERVISED.md 安装。' >&2
    exit 2
fi
command -v flock >/dev/null || { printf '%s\n' '需要 Ubuntu 的 flock（util-linux）。' >&2; exit 2; }
exec 9>"$state_dir/live.lock"
flock -n 9 || { printf '%s\n' '已有会话持有运行锁，本次不启动。' >&2; exit 2; }

set -a
. "$env_file"
set +a
for setting in COINS QUOTE_NOTIONAL_USD MAX_LEVERAGE FEE_TIER ENTROPY_TIER ENTROPY_SELF_REBATE; do
    if [[ -z "${!setting:-}" ]]; then
        printf '配置必须明确填写 %s，避免误用仓库默认值。\n' "$setting" >&2
        exit 2
    fi
done
printf '人工值守：%s，每侧 %s USDC，杠杆设置 %s，时长 %s 秒。\n' \
    "${COINS:-未设置}" "${QUOTE_NOTIONAL_USD:-未设置}" "${MAX_LEVERAGE:-未设置}" "$session_seconds"
printf '%s\n' '没有交易所断线自动撤单保护；本脚本不会自动重启或购买额度。'
run_confirm=''
if ! read -r -t 60 -p '确认本次真实交易请输入 RUN（60秒内）：' run_confirm </dev/tty; then
    printf '\n%s\n' '没有收到确认，不启动交易。' >&2
    exit 2
fi
if [[ "$run_confirm" != RUN ]]; then
    printf '%s\n' '未确认，不启动交易。' >&2
    exit 2
fi

export LIVE=1 TZ=UTC
cd -- "$repo_dir"
run_log="$(mktemp "$log_dir/live-$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX.log")"
printf '日志文件：%s\n' "$run_log"
set +e
"$python_bin" -u -m entropy_bot live --supervised-no-deadman --seconds "$session_seconds" 2>&1 | tee -i "$run_log"
run_status=("${PIPESTATUS[@]}")
set -e
printf '程序退出码：%s；日志退出码：%s；日志：%s\n' "${run_status[0]}" "${run_status[1]}" "$run_log"
if (( run_status[0] != 0 || run_status[1] != 0 )); then
    printf '%s\n' '本次未正常完成：请核对网页上的持仓、挂单和日志，勿直接重启。' >&2
    exit 2
fi
