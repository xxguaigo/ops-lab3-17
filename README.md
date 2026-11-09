# enroll-lab：实验 3 的统一示例

> 实验要求、任务步骤、检查点和评分见 Canvas 上发布的《实验 3：资源管理与应用运维》指导书。本仓库只提供示例代码和工具，不含指导书。

在实验 2 的选课后端 `enrolld` 的基础上，加了一个负载均衡器 `lbd` 和一个伸缩控制环 `scaler`，在一台主机上组成一个缩小了的集群：每个 `enrolld` 实例由 systemd 启动，各占一个 cgroup（CPU 配额、内存上限），`lbd` 分发流量并做健康检查，`scaler` 按信号增减实例。**服务本身不是重点**，它的作用是提供一个结果可预测、也可测量的对象。

## 程序

| 程序 | 作用 |
|---|---|
| `enrolld` | 被测服务。比实验 2 多了：`/readyz` 就绪检查与预热（`-warmup`）；收到 SIGTERM 时先报未就绪、再停止（`-drain-delay`）；版本标签（`-version`）；`/api/report?mb=8&ms=500` 每个请求占一段内存，`-report-max` 限制并发；`-leak-kb` 每个读请求泄漏一点内存；`-burn N` 让 N 个线程跑满并打印 cgroup 的节流计数；`/metrics` 多了 `cgroup_*` 指标（读自身 cgroup 的 `cpu.stat`、`memory.*`） |
| `loadgen` | 开环压测。比实验 2 多了：`-plan` 写到达曲线（例如 `60@120s,60~600@30s,600@300s`）；`-timeline` 逐秒输出结果 |
| `lbd` | 负载均衡器：轮询或最少连接；主动健康检查（`-check-interval`、`-fall`、`-rise`）；被动摘除离群实例（`-outlier`）；管理接口可以加入、移除、排空实例；`/metrics` 按实例输出 RED；状态变化记进事件日志 |
| `scaler` | 伸缩控制环：`-mode fixed` 保持固定个数；`-mode reactive` 按一条 PromQL 信号伸缩（HPA 的算法）；`-schedule` 定时调整最少实例数；管理接口可以触发滚动发布；决策与实例的一生记进 CSV。需要 root |
| `promsteps` | 按 loadgen 的阶段查询 Prometheus，输出“阶段 × 查询”的表（任务 A 用） |
| `lab3sum` | 汇总一次经 lbd 的实验：缺口、实例·分钟、新实例各段耗时、伸缩决策、注入后的检出与恢复 |

`bin/<程序> -h` 列出全部参数。

## 端口

| 端口 | 谁 |
|---|---|
| 8080 | `lbd`：业务流量的入口 |
| 8090 | `lbd`：管理接口与 `/metrics` |
| 8091 | `scaler`：管理接口 |
| 8081 | 任务 A 单独启动的 `enrolld` |
| 8101 起 | `scaler` 启动的 `enrolld` 实例（systemd 服务 `lab3-inst-<端口>`） |
| 9090 | Prometheus |
| 9100 | node_exporter |

全部只监听本机地址，不要在安全组里放通。

## 构建

需要 Go 1.26 或更高版本、GNU make。没有第三方依赖，构建时无需联网。

```bash
make build test        # 生成 bin/ 下的程序，并运行测试
scripts/get-tools.sh   # 下载并校验 Prometheus 3.13.4、node_exporter 1.12.1，放到 tools/
```

## 文件

| 文件 | 说明 |
|---|---|
| `cmd/` | 上面的六个程序 |
| `internal/metrics/` | 仅用标准库输出 Prometheus 文本格式的指标 |
| `deploy/prometheus.yml` | Prometheus 配置；`scaler` 启动的实例经 `data/targets.json` 自动发现 |
| `deploy/queries-red.txt`、`deploy/queries-use.txt` | 任务 A 的服务层与 cgroup 查询、资源层查询；前者有一行 TODO 需自行编写 |
| `scripts/run.sh` | 任务 A：对单个实例分阶段加压，结束后查询 Prometheus，结果存入 `runs/<名字>/` |
| `scripts/run3.sh` | 任务 B、C、D：经 `lbd` 加压并逐秒记录，结束后复制事件日志并汇总，结果存入 `runs/<名字>/` |
| `scripts/plan.sh` | 生成“开放日”到达曲线 |
| `scripts/inject.sh` | 故障注入（停止、暂停、调低配额），并记下注入的时刻 |
| `scripts/cleanup.sh` | 停掉本实验启动的全部 systemd 服务 |
| `scripts/requery.sh`、`scripts/rates.sh`、`scripts/table.sh`、`scripts/get-tools.sh` | 同实验 2 |
| `LAB3.md` | 小组的数据与分析记录，需要填写并提交 |
