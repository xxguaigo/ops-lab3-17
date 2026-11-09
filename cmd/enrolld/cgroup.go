package main

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"enroll-lab/internal/metrics"
)

// cgroup.go：读本进程所在 cgroup（v2）的 CPU 与内存计数（L-09 第 11～14 页的“阀门”），
// 作为指标暴露在 /metrics 里；-burn 模式用同样的计数核对配额算术（指导书任务 A1）。
//
// 用 systemd-run 启动时，进程在自己的 cgroup 里，例如 /sys/fs/cgroup/system.slice/lab3-a.service；
// 直接在终端里启动时，读到的是终端会话所在的 cgroup，没有设限，节流计数为 0。

const cgroupRoot = "/sys/fs/cgroup"

// ownCgroup 返回本进程的 cgroup v2 目录；不是 cgroup v2 时返回空串。
func ownCgroup() string {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	return parseCgroupFile(string(b))
}

// parseCgroupFile 从 /proc/self/cgroup 的内容中取出 cgroup v2 的路径（"0::/…" 那一行）。
func parseCgroupFile(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if rest, ok := strings.CutPrefix(line, "0::"); ok {
			return filepath.Join(cgroupRoot, rest)
		}
	}
	return ""
}

// readKV 读 cpu.stat、memory.events 这种“键 值”每行一对的文件。
func readKV(path string) map[string]float64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return parseKV(string(b))
}

func parseKV(s string) map[string]float64 {
	m := map[string]float64{}
	for _, line := range strings.Split(s, "\n") {
		f := strings.Fields(line)
		if len(f) != 2 {
			continue
		}
		if v, err := strconv.ParseFloat(f[1], 64); err == nil {
			m[f[0]] = v
		}
	}
	return m
}

// parseCPUMax 解析 cpu.max（“配额 周期”，单位微秒；配额为 max 表示不限）。返回能用的核数与周期；不限时 cores 为 0。
func parseCPUMax(s string) (cores float64, period time.Duration, ok bool) {
	f := strings.Fields(s)
	if len(f) != 2 {
		return 0, 0, false
	}
	p, err := strconv.ParseFloat(f[1], 64)
	if err != nil || p <= 0 {
		return 0, 0, false
	}
	period = time.Duration(p) * time.Microsecond
	if f[0] == "max" {
		return 0, period, true
	}
	q, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return 0, 0, false
	}
	return q / p, period, true
}

// readBytes 读 memory.current、memory.max 这种只有一个数的文件；“max”（不限）返回 0。
func readBytes(path string) float64 {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	v, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if err != nil {
		return 0
	}
	return v
}

func cpuMax(dir string) (float64, time.Duration) {
	b, err := os.ReadFile(filepath.Join(dir, "cpu.max"))
	if err != nil {
		return 0, 0
	}
	c, p, _ := parseCPUMax(string(b))
	return c, p
}

// registerCgroupMetrics 把 cgroup 的计数注册成指标；每次抓取时现读文件。
func registerCgroupMetrics(reg *metrics.Registry, dir string) {
	stat := func(key string, scale float64) func() float64 {
		return func() float64 { return readKV(filepath.Join(dir, "cpu.stat"))[key] * scale }
	}
	event := func(key string) func() float64 {
		return func() float64 { return readKV(filepath.Join(dir, "memory.events"))[key] }
	}
	reg.NewCounterFunc("cgroup_cpu_usage_seconds_total", "本 cgroup 累计用掉的 CPU 时间（cpu.stat 的 usage_usec，秒）", stat("usage_usec", 1e-6))
	reg.NewCounterFunc("cgroup_cpu_periods_total", "经历的配额周期数（cpu.stat 的 nr_periods）；没有设配额时不增长", stat("nr_periods", 1))
	reg.NewCounterFunc("cgroup_cpu_throttled_periods_total", "被节流的周期数（cpu.stat 的 nr_throttled）", stat("nr_throttled", 1))
	reg.NewCounterFunc("cgroup_cpu_throttled_seconds_total",
		"被停的累计时间（cpu.stat 的 throttled_usec，秒）；按 CPU 分别累计，可以超过墙钟时间", stat("throttled_usec", 1e-6))
	reg.NewGaugeFunc("cgroup_cpu_limit_cores", "cpu.max 折合的核数（配额 ÷ 周期）；0 表示不限", func() float64 {
		c, _ := cpuMax(dir)
		return c
	})
	reg.NewGaugeFunc("cgroup_memory_current_bytes", "本 cgroup 当前占用的内存（memory.current，含页缓存）", func() float64 {
		return readBytes(filepath.Join(dir, "memory.current"))
	})
	reg.NewGaugeFunc("cgroup_memory_max_bytes", "内存上限（memory.max）；0 表示不限", func() float64 {
		return readBytes(filepath.Join(dir, "memory.max"))
	})
	reg.NewCounterFunc("cgroup_memory_max_events_total", "内存用量碰到 memory.max 的次数（memory.events 的 max）", event("max"))
	reg.NewCounterFunc("cgroup_memory_oom_kills_total", "本 cgroup 里被 OOM killer 杀掉的进程数（memory.events 的 oom_kill）", event("oom_kill"))
}

// burnResult 是一次 -burn 的 cpu.stat 增量。
type burnResult struct {
	n                         int
	wall                      time.Duration
	usage, throttled          float64 // 秒
	periods, throttledPeriods float64
	limitCores                float64
	period                    time.Duration
}

// stopPerThreadMs 是“每个线程每个周期被停多久”的实测值：throttled_usec 按 CPU 累计，除以周期数和线程数。
func (r burnResult) stopPerThreadMs() float64 {
	if r.periods == 0 || r.n == 0 {
		return 0
	}
	return r.throttled / r.periods / float64(r.n) * 1000
}

// expectedStopMs 是 L-09 第 12 页的算术：周期 P、配额 L 核、n 个线程同时忙，每个线程每周期运行 L × P ÷ n，其余时间被停。
func expectedStopMs(limitCores float64, period time.Duration, n int) float64 {
	if limitCores <= 0 || n <= 0 {
		return 0
	}
	p := float64(period) / float64(time.Millisecond)
	return math.Max(0, p-limitCores*p/float64(n))
}

// burn 启动 n 个忙线程跑 d 这么久，返回这段时间里 cpu.stat 的增量。
func burn(dir string, n int, d time.Duration) burnResult {
	read := func() map[string]float64 { return readKV(filepath.Join(dir, "cpu.stat")) }
	limit, period := cpuMax(dir)
	var stop atomic.Bool
	for i := 0; i < n; i++ {
		go func() {
			for !stop.Load() {
				spin(10000)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond) // 让线程都跑起来，再开始计数
	a, t0 := read(), time.Now()
	time.Sleep(d)
	b, t1 := read(), time.Now()
	stop.Store(true)
	return burnResult{n: n, wall: t1.Sub(t0), limitCores: limit, period: period,
		usage:            (b["usage_usec"] - a["usage_usec"]) / 1e6,
		periods:          b["nr_periods"] - a["nr_periods"],
		throttledPeriods: b["nr_throttled"] - a["nr_throttled"],
		throttled:        (b["throttled_usec"] - a["throttled_usec"]) / 1e6}
}

func printBurn(w io.Writer, dir string, r burnResult) {
	fmt.Fprintf(w, "cgroup：%s\n", dir)
	if r.limitCores > 0 {
		fmt.Fprintf(w, "cpu.max：配额 %.2f 核，周期 %v（每个周期最多 %.0f ms CPU 时间）\n", r.limitCores, r.period,
			r.limitCores*float64(r.period)/float64(time.Millisecond))
	} else {
		fmt.Fprintln(w, "cpu.max：不限（不在设了配额的 cgroup 里，节流计数为 0）")
	}
	fmt.Fprintf(w, "GOMAXPROCS=%d，忙线程 %d 个，计数 %.1f 秒\n", runtime.GOMAXPROCS(0), r.n, r.wall.Seconds())
	ratio := 0.0
	if r.periods > 0 {
		ratio = r.throttledPeriods / r.periods
	}
	fmt.Fprintf(w, "实际用核 %.2f；周期 %.0f 个，被节流 %.0f 个（%.0f%%）；被停共 %.2f 秒（按 CPU 累计）\n",
		r.usage/r.wall.Seconds(), r.periods, r.throttledPeriods, ratio*100, r.throttled)
	fmt.Fprintf(w, "每个线程每个周期被停：实测 %.1f ms，算术 %.1f ms\n", r.stopPerThreadMs(), expectedStopMs(r.limitCores, r.period, r.n))
}
