#!/usr/bin/env bash
# 任务 B、C、D 用：经 lbd 加压，逐秒记录客户端看到的结果；结束后把 lbd 的事件、scaler 的决策与实例记录、
# 注入记录复制进 runs/<名字>/，再用 lab3sum 汇总。
#
# 用法（在仓库根目录，lbd、Prometheus、scaler 已经启动）：
#   scripts/run3.sh <名字> <loadgen 的参数…>
# 例：
#   scripts/run3.sh b1-fixed -url http://127.0.0.1:8080/api/courses/1 -plan "$(scripts/plan.sh 60 500)"
#
# runs/<名字>/ 里有：
#   meta.txt                 主机、lbd 与 scaler 的参数和当时的实例、压测命令、仓库提交
#   steps.csv  timeline.csv  loadgen：每个阶段一行；每秒一行（按计划发送时刻归到所在的秒）
#   lbd-events.csv           lbd 的状态变化（加入、不健康、恢复、摘除、排空、移除）
#   decisions.csv            scaler 的每一次伸缩决策
#   instances.csv            scaler 记录的每个实例的一生（创建、启动、就绪、停止、死掉、重启）
#   marks.csv                scripts/inject.sh 记下的注入
#   lbd-metrics-end.txt      结束时 lbd 的 /metrics 原文
#   loadgen.log  summary.md  loadgen 的输出；lab3sum 的汇总
#
# 开始加压时把当前时刻写进 data/run-start，scaler 的定时表（-schedule）从这时算起。
#
# 环境变量：
#   GEN_CPUS  loadgen 绑定的核，默认 0
#   LB        lbd 的管理接口，默认 http://127.0.0.1:8090
#   SCALER    scaler 的管理接口，默认 http://127.0.0.1:8091（没有运行 scaler 时只是少记一些信息）
#   SLO       判断缺口与慢请求的延迟阈值（秒），默认 0.2
#   DATA      lbd、scaler 的数据目录，默认 data
set -euo pipefail

GEN_CPUS=${GEN_CPUS:-0}
LB=${LB:-http://127.0.0.1:8090}
SCALER=${SCALER:-http://127.0.0.1:8091}
SLO=${SLO:-0.2}
DATA=${DATA:-data}

die() { echo "run3.sh：$*" >&2; exit 1; }

name=${1:-}
if [ -z "$name" ] || [ $# -lt 2 ]; then die "用法：scripts/run3.sh <名字> <loadgen 参数…>"; fi
shift
[[ $name =~ ^[A-Za-z0-9._-]+$ ]] || die "名字只能用字母、数字和 . _ -"
if [ ! -x bin/loadgen ] || [ ! -x bin/lab3sum ]; then die "请在仓库根目录运行，并先 make build"; fi
command -v taskset >/dev/null || die "缺少命令 taskset"
dir=runs/$name
[ ! -e "$dir" ] || die "$dir 已经存在：换一个名字"
lbcfg=$(curl -fsS -m 5 "$LB/config") || die "连不上 lbd（$LB），它启动了吗？"

mkdir -p "$dir"
{
  echo "名字: $name"
  echo "开始: $(date -Iseconds)"
  echo "主机: $(uname -srm)"
  echo "CPU: $(nproc) 个逻辑核；$(lscpu 2>/dev/null | sed -n 's/^Model name: *//p' | head -n 1)"
  echo "内存: $(free -m | awk '/^Mem:/{print $2 " MiB，available " $7 " MiB"}')"
  echo "systemd: $(systemctl --version | head -n 1)"
  echo "loadgen: 绑定的核 $GEN_CPUS；缺口阈值 ${SLO}s"
  echo "仓库提交: $(git rev-parse --short HEAD 2>/dev/null || echo 无)，未提交的改动 $(git status --porcelain 2>/dev/null | wc -l) 处"
  echo "压测命令: bin/loadgen ${*@Q}"
  echo "lbd /config:"
  echo "$lbcfg"
  echo "lbd /backends:"
  curl -fsS -m 5 "$LB/backends" || true
  echo "scaler /config:"
  curl -fsS -m 5 "$SCALER/config" || echo "（没有运行 scaler）"
  echo "scaler /instances:"
  curl -fsS -m 5 "$SCALER/instances" || true
} >"$dir/meta.txt"

start_ms=$(date +%s%3N)
date +%s >"$DATA/run-start"
echo "== loadgen（结果写进 $dir/）"
status=0
taskset -c "$GEN_CPUS" bin/loadgen -slo "${SLO}s" -stop-below 0 -out "$dir/steps.csv" -timeline "$dir/timeline.csv" "$@" 2>&1 |
  tee "$dir/loadgen.log" || status=$?
sleep 2
[ "$status" -eq 0 ] || die "loadgen 退出码 $status，见 $dir/loadgen.log"

# 事件只取这次实验开始前 10 分钟以来的行；instances.csv 整个复制，汇总时按负载的时间段裁剪
from=$((start_ms - 600000))
for f in lbd-events.csv decisions.csv marks.csv; do
  [ -f "$DATA/$f" ] && awk -F, -v a="$from" 'NR == 1 || $1 >= a' "$DATA/$f" >"$dir/$f"
done
[ -f "$DATA/instances.csv" ] && cp "$DATA/instances.csv" "$dir/instances.csv"
curl -fsS -m 5 "${LB}/metrics" >"$dir/lbd-metrics-end.txt" || echo "警告：结束时取不到 lbd 的 /metrics" >&2
echo "结束: $(date -Iseconds)" >>"$dir/meta.txt"

bin/lab3sum -dir "$dir" -slo "$SLO" | tee "$dir/summary.md"
echo "完成：$dir"
