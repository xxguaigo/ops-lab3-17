#!/usr/bin/env bash
# 任务 C 的故障注入。每次注入都在 data/marks.csv 里记一行（时刻、动作、对象），lab3sum 据此算出 lbd 多久发现、多久恢复。
#
# 用法（需要 root；实例是 scaler 启动的 lab3-inst-<端口>）：
#   sudo scripts/inject.sh kill <端口>              SIGKILL 一个实例（停止）
#   sudo scripts/inject.sh pause all|<端口> <秒>     SIGSTOP 暂停，过几秒再 SIGCONT（模拟一次网络抖动）
#   sudo scripts/inject.sh slow <端口> <配额%>       把一个实例的 CPU 配额临时调低（变慢，进程照常响应健康检查）
#   sudo scripts/inject.sh restore <端口> <配额%>    把配额调回去
#   scripts/inject.sh mark <说明>                    只记一笔，不做任何操作
set -euo pipefail
DATA=${DATA:-data}
die() { echo "inject.sh：$*" >&2; exit 1; }

mark() {
  local f=$DATA/marks.csv
  [ -s "$f" ] || echo "unix_ms,time,action,target" >"$f"
  echo "$(date +%s%3N),$(date +%H:%M:%S.%3N),$1,$2" >>"$f"
  echo "已记录：$1 $2"
}
unit() {
  [[ ${1:-} =~ ^[0-9]+$ ]] || die "端口应为数字，例如 8102"
  local u=lab3-inst-$1.service
  systemctl is-active --quiet "$u" || die "$u 没有在运行（systemctl list-units 'lab3-inst-*' 查看）"
  echo "$u"
}

action=${1:-}
case $action in
  kill)
    u=$(unit "${2:-}")
    mark kill "127.0.0.1:$2"
    systemctl kill --signal=SIGKILL "$u"
    ;;
  pause)
    secs=${3:-}
    [[ $secs =~ ^[0-9]+([.][0-9]+)?$ ]] || die "用法：inject.sh pause all|<端口> <秒>"
    if [ "${2:-}" = all ]; then
      mapfile -t units < <(systemctl list-units --plain --no-legend --state=active 'lab3-inst-*' | awk '{print $1}')
      [ ${#units[@]} -gt 0 ] || die "没有在运行的实例"
      target='*'
    else
      units=("$(unit "${2:-}")")
      target=127.0.0.1:$2
    fi
    mark pause "$target"
    for u in "${units[@]}"; do systemctl kill --signal=SIGSTOP "$u"; done
    sleep "$secs"
    for u in "${units[@]}"; do systemctl kill --signal=SIGCONT "$u"; done
    mark resume "$target"
    ;;
  slow | restore)
    q=${3:-}
    [[ $q =~ ^[0-9]+$ ]] || die "用法：inject.sh $action <端口> <配额%>"
    u=$(unit "${2:-}")
    mark "$action" "127.0.0.1:$2"
    systemctl set-property --runtime "$u" "CPUQuota=${q}%"
    ;;
  mark)
    shift
    mark mark "${*:-}"
    ;;
  *)
    die "用法：inject.sh kill|pause|slow|restore|mark …（见脚本开头的说明）"
    ;;
esac
