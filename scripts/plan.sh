#!/usr/bin/env bash
# 生成任务 B 的“开放日”到达曲线，输出 loadgen -plan 要的字符串。比例与 L-11 的场景相同：
# 平时低负载 2 分钟 → 30 秒内爬升到峰值 → 峰值维持 5 分钟 → 1 分钟降回平时 → 平时 2 分钟。
#
# 用法：scripts/plan.sh <平时到达率> <峰值到达率> [爬升秒数]
# 例：  scripts/plan.sh 60 500      → 60@120s,60~500@30s,500@300s,500~60@60s,60@120s
set -euo pipefail
base=${1:?用法：scripts/plan.sh <平时到达率> <峰值到达率> [爬升秒数]}
peak=${2:?用法：scripts/plan.sh <平时到达率> <峰值到达率> [爬升秒数]}
ramp=${3:-30}
for v in "$base" "$peak" "$ramp"; do [[ $v =~ ^[0-9]+$ ]] || { echo "参数应为正整数：$v" >&2; exit 1; }; done
echo "${base}@120s,${base}~${peak}@${ramp}s,${peak}@300s,${peak}~${base}@60s,${base}@120s"
