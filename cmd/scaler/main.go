// scaler 是实验 3 的伸缩控制环：按信号增减 enrolld 实例，把实例登记到 lbd，并记下每一次决策和每个实例的一生。
//
// 实例：用 systemd-run 启动的临时服务 lab3-inst-<端口>，每个一个 cgroup（-quota、-memory、-cpus）；
// 同时设 MemorySwapMax=0，内存超限时直接被杀，而不是换出到 swap 里慢慢变慢。
// 新建一个实例要经过：创建（模拟拉镜像 -pull）→ 启动（进程起来，预热中）→ 就绪（lbd 的健康检查连续成功）。
// 只有就绪的实例才分到流量；控制环的延迟就是这几段加上指标延迟与等待决策的时间（L-11 第 10 页）。
//
// 两种模式：
//
//	-mode fixed     保持 -replicas 个实例；有实例死掉就补一个（L-10 第 16 页的补建）
//	-mode reactive  每 -sync 做一次决策：期望数 = ⌈当前数 × 信号 ÷ -target⌉，与目标相差不到 -tolerance 时不动；
//	                夹在 [最少, -max] 之间；每次最多扩到当前的 2 倍或多 4 个（取大者）；
//	                缩容取最近 -down-window 内期望数的最大值（稳定窗口）。这是 Kubernetes HPA 的默认做法（L-11 第 13 页）。
//
// 定时（-schedule）：负载开始后到了某个时刻，把最少副本数调到某个值，例如 +20s=4,+400s=2。
// “负载开始”取 scripts/run3.sh 写入的 data/run-start；没有这个文件时定时不生效。
//
// 管理接口（-admin）：GET /instances、GET /config、POST /rollout?version=v2&mode=graceful|naive（任务 D1 的滚动发布）。
// 需要 root：systemd-run 要在系统实例里创建服务。退出时（Ctrl-C）停止自己启动的全部实例。
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type options struct {
	Mode       string  `json:"mode"`
	Replicas   int     `json:"replicas"`
	Min        int     `json:"min"`
	Max        int     `json:"max"`
	Target     float64 `json:"target"`
	Tolerance  float64 `json:"tolerance"`
	Signal     string  `json:"signal"`
	Sync       string  `json:"sync"`
	DownWindow string  `json:"down_window"`
	Schedule   string  `json:"schedule"`
	Pull       string  `json:"pull"`
	Quota      int     `json:"quota_percent"`
	QuotaPer   string  `json:"quota_period"`
	Memory     string  `json:"memory"`
	CPUs       string  `json:"cpus"`
	Restart    string  `json:"restart"`
	Bin        string  `json:"bin"`
	Args       string  `json:"args"`
	Env        string  `json:"env"`
	Version    string  `json:"version"`
	BasePort   int     `json:"base_port"`
	Data       string  `json:"data"`
	LB         string  `json:"lb"`
	Prom       string  `json:"prom"`
	Admin      string  `json:"admin"`
	Started    string  `json:"started"`

	sync, downWindow, pull time.Duration
	schedule               []schedEntry
}

type schedEntry struct {
	at  time.Duration
	min int
}

// 实例的状态
const (
	stPulling  = "pulling"  // 已创建，模拟拉镜像
	stStarting = "starting" // 进程已启动，等就绪
	stReady    = "ready"    // lbd 判为健康，接流量
	stStopping = "stopping" // 排空、停止中
)

type instance struct {
	port      int
	addr      string
	unit      string
	version   string
	state     string
	created   time.Time
	pullUntil time.Time
	assumeUp  bool // 登记到 lbd 时直接视为健康（任务 D1 的“直接替换”）
	restarts  int
	startedAt time.Time
}

type scaler struct {
	opt   options
	mu    sync.Mutex
	insts []*instance
	hist  []histEntry // 最近的期望数，用于缩容的稳定窗口
	busy  bool        // 正在滚动发布，暂停决策

	runStart time.Time
	runMtime time.Time
	decLog   *csvLog
	instLog  *csvLog
	http     *http.Client
}

type histEntry struct {
	t time.Time
	n int
}

func main() {
	var o options
	flag.StringVar(&o.Mode, "mode", "reactive", "fixed：固定 -replicas 个；reactive：按 -signal 伸缩")
	flag.IntVar(&o.Replicas, "replicas", 3, "fixed 模式的实例数")
	flag.IntVar(&o.Min, "min", 2, "reactive 模式的最少实例数（定时表可以临时调高它）")
	flag.IntVar(&o.Max, "max", 4, "reactive 模式的最多实例数")
	flag.Float64Var(&o.Target, "target", 0.7, "信号的目标值，例如 CPU 利用率 0.7")
	flag.Float64Var(&o.Tolerance, "tolerance", 0.1, "信号与目标之比偏离 1 不超过它时不动（HPA 默认 0.1）")
	flag.StringVar(&o.Signal, "signal", "", "伸缩信号：一条 PromQL，返回一个数（每个实例的平均值）；${quota} 换成每个实例的配额核数；空表示只按定时表")
	flag.DurationVar(&o.sync, "sync", 15*time.Second, "决策周期（HPA 默认 15 s）")
	flag.DurationVar(&o.downWindow, "down-window", 300*time.Second, "缩容稳定窗口（HPA 默认 300 s）")
	flag.StringVar(&o.Schedule, "schedule", "", "定时表：负载开始后多久把最少实例数调到几，例如 +20s=4,+400s=2")
	flag.DurationVar(&o.pull, "pull", 20*time.Second, "模拟拉镜像的时间：创建实例后等这么久才启动进程")
	flag.IntVar(&o.Quota, "quota", 25, "每个实例的 CPU 配额（%，100 表示一个核）")
	flag.StringVar(&o.QuotaPer, "quota-period", "", "配额周期（systemd 的 CPUQuotaPeriodSec，例如 10ms）；空表示内核默认的 100 ms")
	flag.StringVar(&o.Memory, "memory", "128M", "每个实例的内存上限（MemoryMax）")
	flag.StringVar(&o.CPUs, "cpus", "1", "实例能用的核（AllowedCPUs），与 loadgen 的核分开")
	flag.StringVar(&o.Restart, "restart", "", "实例退出后由 systemd 重启的策略，例如 on-failure（任务 D2）；空表示不重启，由 scaler 补建")
	flag.StringVar(&o.Bin, "bin", "", "enrolld 的路径；默认与 scaler 在同一目录")
	flag.StringVar(&o.Args, "args", "-warmup 60s -work 1000000", "传给每个 enrolld 的其他参数（默认：预热 60 秒；每个读请求的计算量是 enrolld 默认值的 5 倍，使单个实例的上限约为几十请求/秒）")
	flag.StringVar(&o.Env, "env", "", "传给每个 enrolld 的环境变量，逗号分隔，例如 GOGC=100")
	flag.StringVar(&o.Version, "version", "v1", "新建实例的版本标签")
	flag.IntVar(&o.BasePort, "base-port", 8101, "实例端口从这里开始分配")
	flag.StringVar(&o.Data, "data", "data", "数据目录：实例的数据、决策与实例日志、Prometheus 的目标文件都放在这里")
	flag.StringVar(&o.LB, "lb", "http://127.0.0.1:8090", "lbd 的管理接口")
	flag.StringVar(&o.Prom, "prom", "http://127.0.0.1:9090", "Prometheus 的地址")
	flag.StringVar(&o.Admin, "admin", "127.0.0.1:8091", "scaler 自己的管理接口")
	flag.Parse()
	o.Sync, o.DownWindow, o.Pull = o.sync.String(), o.downWindow.String(), o.pull.String()
	o.Started = time.Now().Format(time.RFC3339)
	if err := o.check(); err != nil {
		log.Fatal(err)
	}
	if os.Geteuid() != 0 {
		log.Fatal("scaler 需要 root：systemd-run 要在系统实例里创建服务。请用 sudo 运行")
	}

	s, err := newScaler(o)
	if err != nil {
		log.Fatal(err)
	}
	s.cleanupLeftovers()
	adm := &http.Server{Addr: o.Admin, Handler: s.adminRoutes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := adm.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("scaler：%s；实例 %d%% CPU、%s 内存、核 %s；拉镜像 %v；决策周期 %v；日志在 %s", s.describe(), o.Quota, o.Memory, o.CPUs,
		o.pull, o.sync, o.Data)
	s.run(ctx)
	log.Print("停止全部实例…")
	s.stopAll()
	adm.Close()
}

func (o *options) check() error {
	switch o.Mode {
	case "fixed":
		if o.Replicas < 1 {
			return errors.New("-replicas 至少为 1")
		}
	case "reactive":
		if o.Min < 1 || o.Max < o.Min {
			return errors.New("需要 1 ≤ -min ≤ -max")
		}
		if o.Target <= 0 {
			return errors.New("-target 必须大于 0")
		}
	default:
		return errors.New("-mode 只能是 fixed 或 reactive")
	}
	if o.Quota < 1 {
		return errors.New("-quota 至少为 1（%）")
	}
	sched, err := parseSchedule(o.Schedule)
	if err != nil {
		return err
	}
	o.schedule = sched
	if o.Bin == "" {
		exe, err := os.Executable()
		if err != nil {
			return err
		}
		o.Bin = filepath.Join(filepath.Dir(exe), "enrolld")
	}
	if o.Bin, err = filepath.Abs(o.Bin); err != nil {
		return err
	}
	if _, err := os.Stat(o.Bin); err != nil {
		return fmt.Errorf("找不到 enrolld：%v（先 make build，或用 -bin 指定）", err)
	}
	if o.Data, err = filepath.Abs(o.Data); err != nil {
		return err
	}
	return os.MkdirAll(o.Data, 0o755)
}

// parseSchedule 解析 +20s=4,+400s=2：负载开始后多久，把最少实例数调到几。
func parseSchedule(s string) ([]schedEntry, error) {
	var out []schedEntry
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	for _, part := range strings.Split(s, ",") {
		at, n, ok := strings.Cut(strings.TrimSpace(part), "=")
		d, err := time.ParseDuration(strings.TrimPrefix(at, "+"))
		v, err2 := strconv.Atoi(n)
		if !ok || err != nil || err2 != nil || d < 0 || v < 1 {
			return nil, fmt.Errorf("定时表 %q 应写成 +时长=实例数，例如 +20s=4", part)
		}
		out = append(out, schedEntry{at: d, min: v})
	}
	slices.SortFunc(out, func(a, b schedEntry) int { return int(a.at - b.at) })
	return out, nil
}

// minAt 是负载开始后 since 时刻生效的最少实例数：定时表里最近一条已到时刻的值；还没有到任何一条时用 base。
func minAt(base int, sched []schedEntry, since time.Duration, running bool) int {
	m := base
	if !running {
		return m
	}
	for _, e := range sched {
		if since >= e.at {
			m = e.min
		}
	}
	return m
}

// desiredReplicas 是 HPA 的算法（L-11 第 13 页）：期望数 = ⌈当前数 × 信号 ÷ 目标⌉，相差在容差内不动；
// 每次最多扩到当前的 2 倍或多 4 个（取大者）；夹在 [lo, hi] 之间。
func desiredReplicas(current int, signal, target, tolerance float64, lo, hi int) int {
	want := current
	if current > 0 && !math.IsNaN(signal) && target > 0 {
		ratio := signal / target
		if math.Abs(ratio-1) > tolerance {
			want = int(math.Ceil(float64(current)*ratio - 1e-9))
		}
	}
	if limit := max(2*current, current+4); want > limit {
		want = limit
	}
	return max(lo, min(want, hi))
}

// stabilized 是缩容的稳定窗口：要缩时，取窗口内期望数的最大值；要扩时不等。
func stabilized(current, want int, hist []histEntry, now time.Time, window time.Duration) int {
	if want >= current {
		return want
	}
	m := want
	for _, h := range hist {
		if now.Sub(h.t) <= window {
			m = max(m, h.n)
		}
	}
	return min(m, current)
}

func newScaler(o options) (*scaler, error) {
	dl, err := openCSV(filepath.Join(o.Data, "decisions.csv"),
		[]string{"unix_ms", "time", "t_run", "signal", "current", "ready", "min", "max", "computed", "desired", "action"})
	if err != nil {
		return nil, err
	}
	il, err := openCSV(filepath.Join(o.Data, "instances.csv"),
		[]string{"unix_ms", "time", "t_run", "instance", "version", "event", "detail"})
	if err != nil {
		return nil, err
	}
	return &scaler{opt: o, decLog: dl, instLog: il, http: &http.Client{Timeout: 3 * time.Second}}, nil
}

func (s *scaler) describe() string {
	if s.opt.Mode == "fixed" {
		return fmt.Sprintf("固定 %d 个实例", s.opt.Replicas)
	}
	return fmt.Sprintf("按信号伸缩，目标 %g，%d～%d 个，定时表 %q", s.opt.Target, s.opt.Min, s.opt.Max, s.opt.Schedule)
}

func (s *scaler) tRun(now time.Time) string {
	if s.runStart.IsZero() || now.Before(s.runStart.Add(-time.Hour)) {
		return ""
	}
	return strconv.FormatFloat(now.Sub(s.runStart).Seconds(), 'f', 1, 64)
}

func (s *scaler) logInst(in *instance, ev, detail string) {
	now := time.Now()
	s.instLog.write(strconv.FormatInt(now.UnixMilli(), 10), now.Format("15:04:05.000"), s.tRun(now), in.addr, in.version, ev, detail)
	log.Printf("%s %s %s %s", in.addr, in.version, ev, detail)
}

// readRunStart 读 scripts/run3.sh 写的 data/run-start（负载开始的 Unix 秒）。
func (s *scaler) readRunStart() {
	path := filepath.Join(s.opt.Data, "run-start")
	st, err := os.Stat(path)
	if err != nil || st.ModTime().Equal(s.runMtime) {
		return
	}
	s.runMtime = st.ModTime()
	b, _ := os.ReadFile(path)
	if v, err := strconv.ParseInt(strings.TrimSpace(string(b)), 10, 64); err == nil {
		s.runStart = time.Unix(v, 0)
		log.Printf("负载开始于 %s；定时表从这时算起", s.runStart.Format("15:04:05"))
	}
}

func (s *scaler) run(ctx context.Context) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	nextDecision := time.Now()
	for {
		s.readRunStart()
		s.reconcile()
		if s.opt.Mode == "reactive" && !time.Now().Before(nextDecision) {
			s.decide()
			nextDecision = nextDecision.Add(s.opt.sync)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// reconcile 每秒运行一次：推进各实例的状态，发现死掉的实例，把 lbd 里缺少的实例重新登记（lbd 重启过）；
// fixed 模式下补足实例数。这是一个小的控制环：不断把实际状态拉回期望状态（L-10 第 21 页）。
func (s *scaler) reconcile() {
	up := s.lbStates()
	s.mu.Lock()
	var dead []*instance
	now := time.Now()
	for _, in := range s.insts {
		switch in.state {
		case stPulling:
			if !now.Before(in.pullUntil) {
				if err := s.startUnit(in); err != nil {
					s.logInst(in, "failed", err.Error())
					dead = append(dead, in)
					continue
				}
				in.state = stStarting
				s.logInst(in, "started", "进程已启动，预热中")
			}
		case stStarting, stReady:
			st := unitState(in.unit)
			if st.restarts > in.restarts {
				in.restarts = st.restarts
				s.logInst(in, "restarted", fmt.Sprintf("systemd 第 %d 次重启；上一次结果 %s", st.restarts, st.result))
			}
			if st.active == "failed" || st.active == "inactive" {
				s.logInst(in, "died", fmt.Sprintf("结果 %s，退出状态 %s", st.result, st.status))
				dead = append(dead, in)
				continue
			}
			v, ok := up[in.addr]
			if !ok && up != nil && now.Sub(in.startedAt) > 3*time.Second {
				s.logInst(in, "registered", "lbd 里没有它（lbd 重启过？），重新登记，等健康检查")
				go s.lbCall("POST", "/backends?init=down&addr="+url.QueryEscape(in.addr))
			}
			if ok {
				if v && in.state == stStarting && in.assumeUp {
					// 直接替换：lbd 的登记没动，仍按健康分流量。等 lbd 判为不健康、再判为健康时才记 ready。
					in.state, in.assumeUp = stReady, false
					s.logInst(in, "assumed-up", "直接替换，未等就绪，lbd 仍按健康分流量")
				} else if v && in.state == stStarting {
					in.state = stReady
					s.logInst(in, "ready", "lbd 判为健康")
				} else if !v && in.state == stReady {
					in.state = stStarting
					s.logInst(in, "unready", "lbd 判为不健康")
				}
			}
		}
	}
	for _, in := range dead {
		s.forget(in)
		go s.lbCall("DELETE", "/backends?addr="+url.QueryEscape(in.addr))
		exec.Command("systemctl", "reset-failed", in.unit).Run()
	}
	if s.opt.Mode == "fixed" && !s.busy {
		for n := s.activeCount(); n < s.opt.Replicas; n++ {
			s.create(s.opt.Version, false, "补足到 "+strconv.Itoa(s.opt.Replicas)+" 个")
		}
	}
	changed := len(dead) > 0
	s.mu.Unlock()
	if changed {
		s.writeTargets()
	}
}

func (s *scaler) activeCount() int {
	n := 0
	for _, in := range s.insts {
		if in.state != stStopping {
			n++
		}
	}
	return n
}

func (s *scaler) forget(in *instance) {
	s.insts = slices.DeleteFunc(s.insts, func(x *instance) bool { return x == in })
}

// decide 是一次伸缩决策。
func (s *scaler) decide() {
	now := time.Now()
	sig := math.NaN()
	detail := ""
	if s.opt.Signal != "" {
		v, err := s.query(strings.ReplaceAll(s.opt.Signal, "${quota}", strconv.FormatFloat(float64(s.opt.Quota)/100, 'g', -1, 64)))
		if err != nil {
			detail = "取不到信号：" + err.Error()
		} else {
			sig = v
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.busy {
		return
	}
	current := s.activeCount()
	ready := 0
	for _, in := range s.insts {
		if in.state == stReady {
			ready++
		}
	}
	running := !s.runStart.IsZero() && now.After(s.runStart) && now.Sub(s.runStart) < 6*time.Hour
	lo := minAt(s.opt.Min, s.opt.schedule, now.Sub(s.runStart), running)
	hi := max(s.opt.Max, lo)
	computed := desiredReplicas(current, sig, s.opt.Target, s.opt.Tolerance, lo, hi)
	desired := stabilized(current, computed, s.hist, now, s.opt.downWindow)
	s.hist = append(s.hist, histEntry{now, computed})
	s.hist = slices.DeleteFunc(s.hist, func(h histEntry) bool { return now.Sub(h.t) > s.opt.downWindow })
	action := "none"
	switch {
	case desired > current:
		action = fmt.Sprintf("create %d", desired-current)
		for i := current; i < desired; i++ {
			s.create(s.opt.Version, false, "扩容")
		}
	case desired < current:
		action = fmt.Sprintf("remove %d", current-desired)
		for i := desired; i < current; i++ {
			if in := s.pickVictim(); in != nil {
				go s.retire(in, true)
			}
		}
	}
	if detail != "" {
		action += "；" + detail
	}
	sigS := ""
	if !math.IsNaN(sig) {
		sigS = strconv.FormatFloat(sig, 'f', 3, 64)
	}
	s.decLog.write(strconv.FormatInt(now.UnixMilli(), 10), now.Format("15:04:05.000"), s.tRun(now), sigS,
		strconv.Itoa(current), strconv.Itoa(ready), strconv.Itoa(lo), strconv.Itoa(hi), strconv.Itoa(computed), strconv.Itoa(desired), action)
}

// pickVictim 选一个要缩掉的实例：先缩还没就绪的、后创建的。调用者持有锁。
func (s *scaler) pickVictim() *instance {
	rank := map[string]int{stPulling: 0, stStarting: 1, stReady: 2}
	var best *instance
	for _, in := range s.insts {
		if in.state == stStopping {
			continue
		}
		if best == nil || rank[in.state] < rank[best.state] || (rank[in.state] == rank[best.state] && in.created.After(best.created)) {
			best = in
		}
	}
	if best != nil {
		best.state = stStopping
	}
	return best
}

// create 新建一个实例（先模拟拉镜像）。调用者持有锁。
func (s *scaler) create(version string, assumeUp bool, why string) *instance {
	port := s.opt.BasePort
	for slices.ContainsFunc(s.insts, func(in *instance) bool { return in.port == port }) {
		port++
	}
	now := time.Now()
	in := &instance{port: port, addr: "127.0.0.1:" + strconv.Itoa(port), unit: fmt.Sprintf("lab3-inst-%d", port),
		version: version, state: stPulling, created: now, pullUntil: now.Add(s.opt.pull), assumeUp: assumeUp}
	s.insts = append(s.insts, in)
	s.logInst(in, "created", why)
	return in
}

// startUnit 用 systemd-run 启动实例，并登记到 lbd。调用者持有锁。
func (s *scaler) startUnit(in *instance) error {
	exec.Command("systemctl", "reset-failed", in.unit).Run()
	data := filepath.Join(s.opt.Data, "inst-"+strconv.Itoa(in.port))
	os.RemoveAll(data)
	args := []string{"--unit=" + in.unit, "--quiet", "--property=CPUQuota=" + strconv.Itoa(s.opt.Quota) + "%",
		"--property=MemoryMax=" + s.opt.Memory, "--property=MemorySwapMax=0"}
	if s.opt.CPUs != "" {
		args = append(args, "--property=AllowedCPUs="+s.opt.CPUs)
	}
	if s.opt.QuotaPer != "" {
		args = append(args, "--property=CPUQuotaPeriodSec="+s.opt.QuotaPer)
	}
	if s.opt.Restart != "" {
		args = append(args, "--property=Restart="+s.opt.Restart, "--property=RestartSec=2s")
	}
	for _, kv := range strings.Split(s.opt.Env, ",") {
		if kv = strings.TrimSpace(kv); kv != "" {
			args = append(args, "--setenv="+kv)
		}
	}
	args = append(args, s.opt.Bin, "-addr", in.addr, "-data", data, "-version", in.version)
	args = append(args, strings.Fields(s.opt.Args)...)
	if out, err := exec.Command("systemd-run", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("systemd-run 失败：%v %s", err, bytes.TrimSpace(out))
	}
	in.startedAt = time.Now()
	state := "down"
	if in.assumeUp {
		state = "up"
	}
	go func() {
		s.lbCall("POST", "/backends?init="+state+"&addr="+url.QueryEscape(in.addr))
		s.writeTargets()
	}()
	return nil
}

// retire 停掉一个实例。graceful 为真时先在 lbd 里排空、等在途请求做完，再 SIGTERM；否则直接 SIGKILL。
func (s *scaler) retire(in *instance, graceful bool) {
	s.mu.Lock()
	in.state = stStopping
	pulling := in.pullUntil.After(time.Now()) && in.restarts == 0
	s.mu.Unlock()
	if pulling && unitState(in.unit).active == "" {
		s.mu.Lock()
		s.logInst(in, "stopped", "还在拉镜像，直接取消")
		s.forget(in)
		s.mu.Unlock()
		return
	}
	if graceful {
		s.lbCall("POST", "/drain?addr="+url.QueryEscape(in.addr))
		s.mu.Lock()
		s.logInst(in, "draining", "lbd 不再分新请求")
		s.mu.Unlock()
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && s.lbInflight(in.addr) > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		s.lbCall("DELETE", "/backends?addr="+url.QueryEscape(in.addr))
		exec.Command("systemctl", "stop", in.unit).Run()
	} else {
		exec.Command("systemctl", "kill", "--signal=SIGKILL", in.unit).Run()
		exec.Command("systemctl", "stop", in.unit).Run()
		s.lbCall("DELETE", "/backends?addr="+url.QueryEscape(in.addr))
	}
	exec.Command("systemctl", "reset-failed", in.unit).Run()
	s.mu.Lock()
	how := "SIGTERM（优雅停止）"
	if !graceful {
		how = "SIGKILL"
	}
	s.logInst(in, "stopped", how)
	s.forget(in)
	s.mu.Unlock()
	s.writeTargets()
}

// rollout 把所有实例换成 version（任务 D1）。
//
//	graceful：先起一个新版本的实例，等它就绪（lbd 健康检查通过）再排空并停掉一个旧的，逐个进行
//	naive：   逐个直接杀掉旧实例，在同一个端口上马上起新版本，并立即视为健康；每个之间只隔 5 秒，不等就绪
func (s *scaler) rollout(version, mode string) {
	s.mu.Lock()
	if s.busy {
		s.mu.Unlock()
		return
	}
	s.busy = true
	var old []*instance
	for _, in := range s.insts {
		if in.version != version && in.state != stStopping {
			old = append(old, in)
		}
	}
	s.mu.Unlock()
	log.Printf("滚动发布到 %s（%s），共 %d 个实例", version, mode, len(old))
	for _, in := range old {
		if mode == "graceful" {
			s.mu.Lock()
			nw := s.create(version, false, "滚动发布")
			s.mu.Unlock()
			deadline := time.Now().Add(5 * time.Minute)
			for time.Now().Before(deadline) {
				s.mu.Lock()
				ok := nw.state == stReady
				gone := !slices.Contains(s.insts, nw)
				s.mu.Unlock()
				if ok || gone {
					break
				}
				time.Sleep(500 * time.Millisecond)
			}
			s.retire(in, true)
		} else {
			s.mu.Lock()
			in.state = stStopping
			s.mu.Unlock()
			exec.Command("systemctl", "kill", "--signal=SIGKILL", in.unit).Run()
			exec.Command("systemctl", "stop", in.unit).Run()
			exec.Command("systemctl", "reset-failed", in.unit).Run()
			s.mu.Lock()
			s.logInst(in, "stopped", "SIGKILL（直接替换）")
			in.version, in.state, in.restarts, in.pullUntil = version, stStarting, 0, time.Now()
			in.assumeUp = true
			err := s.startUnitSamePort(in)
			if err != nil {
				s.logInst(in, "failed", err.Error())
			} else {
				s.logInst(in, "started", "同一端口上的新版本，不等就绪")
			}
			s.mu.Unlock()
			time.Sleep(5 * time.Second)
		}
	}
	s.mu.Lock()
	s.busy = false
	s.opt.Version = version
	s.mu.Unlock()
	log.Printf("滚动发布到 %s 完成", version)
}

// startUnitSamePort 在原端口上重新启动实例；lbd 里的登记保留不动（“直接替换”的做法）。调用者持有锁。
func (s *scaler) startUnitSamePort(in *instance) error {
	in.created = time.Now()
	s.logInst(in, "created", "直接替换")
	args := []string{"--unit=" + in.unit, "--quiet", "--property=CPUQuota=" + strconv.Itoa(s.opt.Quota) + "%",
		"--property=MemoryMax=" + s.opt.Memory, "--property=MemorySwapMax=0"}
	if s.opt.CPUs != "" {
		args = append(args, "--property=AllowedCPUs="+s.opt.CPUs)
	}
	if s.opt.QuotaPer != "" {
		args = append(args, "--property=CPUQuotaPeriodSec="+s.opt.QuotaPer)
	}
	data := filepath.Join(s.opt.Data, "inst-"+strconv.Itoa(in.port))
	os.RemoveAll(data)
	args = append(args, s.opt.Bin, "-addr", in.addr, "-data", data, "-version", in.version)
	args = append(args, strings.Fields(s.opt.Args)...)
	if out, err := exec.Command("systemd-run", args...).CombinedOutput(); err != nil {
		return fmt.Errorf("systemd-run 失败：%v %s", err, bytes.TrimSpace(out))
	}
	in.startedAt = time.Now()
	return nil
}

func (s *scaler) stopAll() {
	s.mu.Lock()
	all := slices.Clone(s.insts)
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, in := range all {
		wg.Add(1)
		go func() { defer wg.Done(); s.retire(in, false) }()
	}
	wg.Wait()
}

// cleanupLeftovers 停掉上一次运行留下的实例（scaler 被强行结束时会留下）。
func (s *scaler) cleanupLeftovers() {
	out, _ := exec.Command("systemctl", "list-units", "--all", "--plain", "--no-legend", "lab3-inst-*").Output()
	for _, line := range strings.Split(string(out), "\n") {
		if f := strings.Fields(line); len(f) > 0 {
			log.Printf("停掉上次留下的 %s", f[0])
			exec.Command("systemctl", "stop", f[0]).Run()
			exec.Command("systemctl", "reset-failed", f[0]).Run()
		}
	}
	s.writeTargets()
}

type unitStatus struct {
	active, result, status string
	restarts               int
}

// unitState 读 systemd 记录的服务状态：ActiveState、Result（例如 oom-kill）、退出状态、systemd 重启过几次。
func unitState(unit string) unitStatus {
	out, err := exec.Command("systemctl", "show", "-p", "ActiveState,Result,ExecMainStatus,NRestarts,LoadState", unit).Output()
	if err != nil {
		return unitStatus{}
	}
	return parseUnitShow(string(out))
}

func parseUnitShow(s string) unitStatus {
	var st unitStatus
	sc := bufio.NewScanner(strings.NewReader(s))
	loaded := true
	for sc.Scan() {
		k, v, _ := strings.Cut(sc.Text(), "=")
		switch k {
		case "ActiveState":
			st.active = v
		case "Result":
			st.result = v
		case "ExecMainStatus":
			st.status = v
		case "NRestarts":
			st.restarts, _ = strconv.Atoi(v)
		case "LoadState":
			loaded = v != "not-found"
		}
	}
	if !loaded {
		st.active = ""
	}
	return st
}

// lbStates 返回 lbd 里各实例的健康状态。
func (s *scaler) lbStates() map[string]bool {
	resp, err := s.http.Get(s.opt.LB + "/backends")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var views []struct {
		Addr string `json:"addr"`
		Up   bool   `json:"up"`
	}
	if json.NewDecoder(resp.Body).Decode(&views) != nil {
		return nil
	}
	m := map[string]bool{}
	for _, v := range views {
		m[v.Addr] = v.Up
	}
	return m
}

func (s *scaler) lbInflight(addr string) int64 {
	resp, err := s.http.Get(s.opt.LB + "/backends")
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var views []struct {
		Addr     string `json:"addr"`
		Inflight int64  `json:"inflight"`
	}
	json.NewDecoder(resp.Body).Decode(&views)
	for _, v := range views {
		if v.Addr == addr {
			return v.Inflight
		}
	}
	return 0
}

func (s *scaler) lbCall(method, path string) {
	req, _ := http.NewRequest(method, s.opt.LB+path, nil)
	resp, err := s.http.Do(req)
	if err != nil {
		log.Printf("调用 lbd 失败：%s %s：%v", method, path, err)
		return
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// query 执行一条 PromQL，要求结果是一个数（标量，或只有一条序列的向量）。
func (s *scaler) query(q string) (float64, error) {
	resp, err := s.http.Get(s.opt.Prom + "/api/v1/query?query=" + url.QueryEscape(q))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var r struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Data   struct {
			ResultType string          `json:"resultType"`
			Result     json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return 0, err
	}
	if r.Status != "success" {
		return 0, errors.New(r.Error)
	}
	return parsePromValue(r.Data.ResultType, r.Data.Result)
}

func parsePromValue(typ string, raw json.RawMessage) (float64, error) {
	var pair []any
	switch typ {
	case "scalar":
		if err := json.Unmarshal(raw, &pair); err != nil {
			return 0, err
		}
	case "vector":
		var vec []struct {
			Value []any `json:"value"`
		}
		if err := json.Unmarshal(raw, &vec); err != nil {
			return 0, err
		}
		if len(vec) != 1 {
			return 0, fmt.Errorf("查询返回了 %d 条序列，应当聚合成一条（用 sum、avg）", len(vec))
		}
		pair = vec[0].Value
	default:
		return 0, fmt.Errorf("不支持的结果类型 %s", typ)
	}
	if len(pair) != 2 {
		return 0, errors.New("结果格式不对")
	}
	str, _ := pair[1].(string)
	return strconv.ParseFloat(str, 64)
}

// writeTargets 写 Prometheus 的 file_sd 目标文件：已经启动的实例都要抓（含预热中的）。
func (s *scaler) writeTargets() {
	s.mu.Lock()
	targets := []string{}
	for _, in := range s.insts {
		if in.state != stPulling {
			targets = append(targets, in.addr)
		}
	}
	s.mu.Unlock()
	b, _ := json.MarshalIndent([]map[string]any{{"targets": targets, "labels": map[string]string{"group": "lab3"}}}, "", "  ")
	path := filepath.Join(s.opt.Data, "targets.json")
	tmp := path + ".tmp"
	if os.WriteFile(tmp, b, 0o644) == nil {
		os.Rename(tmp, path)
	}
}

type instanceView struct {
	Addr     string `json:"addr"`
	Version  string `json:"version"`
	State    string `json:"state"`
	Created  string `json:"created"`
	Restarts int    `json:"restarts"`
}

func (s *scaler) adminRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, s.opt) })
	mux.HandleFunc("GET /instances", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		views := []instanceView{}
		for _, in := range s.insts {
			views = append(views, instanceView{in.addr, in.version, in.state, in.created.Format("15:04:05"), in.restarts})
		}
		s.mu.Unlock()
		writeJSON(w, views)
	})
	mux.HandleFunc("POST /rollout", func(w http.ResponseWriter, r *http.Request) {
		v, mode := r.URL.Query().Get("version"), r.URL.Query().Get("mode")
		if v == "" || (mode != "graceful" && mode != "naive") {
			http.Error(w, "用法：POST /rollout?version=v2&mode=graceful|naive", http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		busy := s.busy
		s.mu.Unlock()
		if busy {
			http.Error(w, "上一次滚动发布还没有完成", http.StatusConflict)
			return
		}
		go s.rollout(v, mode)
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, "开始滚动发布\n")
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

type csvLog struct {
	mu sync.Mutex
	f  *os.File
	w  *csv.Writer
}

func openCSV(path string, header []string) (*csvLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	l := &csvLog{f: f, w: csv.NewWriter(f)}
	if st, err := f.Stat(); err == nil && st.Size() == 0 {
		l.w.Write(header)
		l.w.Flush()
	}
	return l, nil
}

func (l *csvLog) write(fields ...string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.w.Write(fields)
	l.w.Flush()
}
