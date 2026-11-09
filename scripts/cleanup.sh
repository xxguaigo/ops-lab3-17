#!/usr/bin/env bash
# 停掉实验 3 启动的全部实例（scaler 启动的 lab3-inst-*、任务 A 的 lab3-a），清除 systemd 里的失败记录，
# 删除 scaler 写的目标文件和 data/run-start。不删除 runs/，也不删除 data/ 里的事件日志。
#
# 用法：sudo scripts/cleanup.sh
set -euo pipefail
DATA=${DATA:-data}
mapfile -t units < <(systemctl list-units --all --plain --no-legend 'lab3-*' | awk '{print $1}')
for u in "${units[@]}"; do
  [ -n "$u" ] || continue
  echo "停止 $u"
  systemctl stop "$u" 2>/dev/null || true
  systemctl reset-failed "$u" 2>/dev/null || true
done
rm -f "$DATA/targets.json" "$DATA/run-start"
echo "完成。仍在运行的 lab3 服务：$(systemctl list-units --plain --no-legend 'lab3-*' | wc -l) 个"
