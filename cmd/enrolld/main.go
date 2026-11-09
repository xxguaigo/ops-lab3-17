// enrolld 是实验 3 的被测服务：在实验 2 的选课后端上增加了就绪、预热、优雅停止、版本、内存占用与泄漏、
// 以及读 cgroup 计数的功能。实验 3 用多个 enrolld 实例组成一个小集群，由 lbd 分发流量、由 scaler 增减实例。
//
//	GET  /api/courses/{id}   读路径：一段纯计算（-work），再到“数据库连接池”（-pool 个连接）里占一个连接 -db-ms 毫秒
//	POST /api/enroll         写路径：把一条选课记录追加到数据目录下的 enroll.log，按 -sync 的方式落盘
//	GET  /api/report         每个请求另占 mb MB 内存，保持 ms 毫秒（任务 A3 的 OOM）；-report-max 限制同时处理的个数
//	GET  /healthz            存活检查：进程还能响应就返回 200
//	GET  /readyz             就绪检查：预热结束、没有在停止中才返回 200（L-05 第 23 页）
//	GET  /metrics            Prometheus 指标（RED、连接池、fsync、进程、cgroup）
//	GET  /config             当前参数（JSON）
//	GET  /debug/pprof/       Go 的性能剖析接口
//
// 预热（-warmup）期间 /readyz 返回 503，业务接口也返回 503，模拟“进程起来了，但还不能提供服务”（L-05 第 22 页）。
// 收到 SIGTERM 时：先把就绪置为失败，等 -drain-delay（让负载均衡发现并摘除），再不接新请求、等在途请求做完，然后退出（L-05 第 24 页）。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"enroll-lab/internal/metrics"
)

type config struct {
	Addr        string  `json:"addr"`
	Version     string  `json:"version"`
	Work        int     `json:"work"`
	Pool        int     `json:"pool"`
	DBMs        float64 `json:"db_ms"`
	PoolTimeout string  `json:"pool_timeout"`
	Sync        string  `json:"sync"`
	Interval    string  `json:"sync_interval"`
	Data        string  `json:"data"`
	Warmup      string  `json:"warmup"`
	DrainDelay  string  `json:"drain_delay"`
	ReportMax   int     `json:"report_max"`
	LeakKB      int     `json:"leak_kb"`
	Cgroup      string  `json:"cgroup"`
	GoVersion   string  `json:"go_version"`
	GOMAXPROCS  int     `json:"gomaxprocs"`
	GOGC        string  `json:"gogc"`
	GOMEMLIMIT  string  `json:"gomemlimit"`
	PID         int     `json:"pid"`
	Started     string  `json:"started"`

	warmup time.Duration
}

type server struct {
	cfg     config
	reg     *metrics.Registry
	pool    *pool
	log     commitLog
	seq     atomic.Uint64
	exports []byte

	warm     atomic.Bool // 预热已结束
	draining atomic.Bool // 收到了 SIGTERM，正在停止
	reports  chan struct{}
	leakMu   sync.Mutex
	leaked   [][]byte

	requests *metrics.CounterVec
	duration *metrics.HistogramVec
	inflight *metrics.GaugeVec
	commits  *metrics.Counter
	leakB    *metrics.Gauge
}

func main() {
	var cfg config
	var poolTimeout, interval, drainDelay time.Duration
	flag.StringVar(&cfg.Addr, "addr", "127.0.0.1:8081", "监听地址；实例由 scaler 启动时用 127.0.0.1:81xx")
	flag.StringVar(&cfg.Version, "version", "v1", "版本标签：写进 enrolld_info 和响应头 X-Version，任务 D1 的滚动发布用")
	flag.IntVar(&cfg.Work, "work", 200000, "每个读请求的计算量（循环次数，固定不变）")
	flag.IntVar(&cfg.Pool, "pool", 32, "数据库连接池大小")
	flag.Float64Var(&cfg.DBMs, "db-ms", 2, "每次查询占用一个连接的时间（毫秒）")
	flag.DurationVar(&poolTimeout, "pool-timeout", time.Second, "等连接的最长时间，超过返回 503")
	flag.StringVar(&cfg.Sync, "sync", "each", "写路径落盘方式：each 每笔 fsync；group 组提交；interval 定期 fsync")
	flag.DurationVar(&interval, "sync-interval", time.Second, "-sync interval 时多久 fsync 一次")
	flag.StringVar(&cfg.Data, "data", "data", "数据目录，必须在真实的磁盘上（不能是 tmpfs）")
	flag.DurationVar(&cfg.warmup, "warmup", 0, "预热时长：启动后这么久之内未就绪，业务接口返回 503")
	flag.DurationVar(&drainDelay, "drain-delay", 2*time.Second, "收到 SIGTERM 后，先报未就绪、继续服务这么久，再停止接收新请求")
	flag.IntVar(&cfg.ReportMax, "report-max", 0, "/api/report 同时处理的上限，超出的立即返回 503；0 表示不限")
	flag.IntVar(&cfg.LeakKB, "leak-kb", 0, "每个读请求泄漏这么多 KB 内存（永不释放），任务 D2 用；0 表示不泄漏")
	benchN := flag.Int("bench-fsync", 0, "不启动服务，只在数据目录里做 N 次“追加 + fsync”并打印耗时分布")
	burnN := flag.Int("burn", 0, "不启动服务，只让 N 个线程忙 -for 这么久，打印 cgroup 的节流计数（任务 A1）")
	burnFor := flag.Duration("for", 10*time.Second, "-burn 的时长")
	flag.Parse()
	cfg.PoolTimeout = poolTimeout.String()
	cfg.Interval = interval.String()
	cfg.Warmup = cfg.warmup.String()
	cfg.DrainDelay = drainDelay.String()
	cfg.Cgroup = ownCgroup()

	if *burnN > 0 {
		printBurn(os.Stdout, cfg.Cgroup, burn(cfg.Cgroup, *burnN, *burnFor))
		return
	}
	if err := os.MkdirAll(cfg.Data, 0o755); err != nil {
		log.Fatal(err)
	}
	warnIfTmpfs(cfg.Data)
	if *benchN > 0 {
		if err := benchFsync(cfg.Data, *benchN, os.Stdout); err != nil {
			log.Fatal(err)
		}
		return
	}
	if cfg.Pool < 1 || cfg.Work < 0 || cfg.DBMs < 0 || cfg.ReportMax < 0 || cfg.LeakKB < 0 {
		log.Fatal("-pool 至少为 1，-work、-db-ms、-report-max、-leak-kb 不能为负")
	}

	cfg.GoVersion = runtime.Version()
	cfg.GOMAXPROCS = runtime.GOMAXPROCS(0)
	cfg.GOGC = os.Getenv("GOGC")
	cfg.GOMEMLIMIT = os.Getenv("GOMEMLIMIT")
	cfg.PID = os.Getpid()
	cfg.Started = time.Now().Format(time.RFC3339)

	s, err := newServer(cfg, poolTimeout, interval)
	if err != nil {
		log.Fatal(err)
	}
	srv := &http.Server{Addr: cfg.Addr, Handler: s.routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	stopped := make(chan struct{})
	go func() {
		<-ctx.Done()
		// 优雅停止（L-05 第 24 页）：先报未就绪，给负载均衡留出摘除的时间；再不接新请求，等在途请求做完。
		s.draining.Store(true)
		log.Printf("收到停止信号：/readyz 改为 503，%v 后停止接收新请求", drainDelay)
		time.Sleep(drainDelay)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("等在途请求超时：%v", err)
		}
		close(stopped)
	}()

	log.Printf("enrolld %s 监听 %s；GOMAXPROCS=%d；cgroup %s；预热 %v；读：work=%d、pool=%d、db-ms=%g；写：sync=%s",
		cfg.Version, cfg.Addr, cfg.GOMAXPROCS, cfg.Cgroup, cfg.warmup, cfg.Work, cfg.Pool, cfg.DBMs, cfg.Sync)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	<-stopped
	if err := s.log.Close(); err != nil {
		log.Printf("关闭日志：%v", err)
	}
	log.Print("已停止")
}

func newServer(cfg config, poolTimeout, interval time.Duration) (*server, error) {
	reg := metrics.NewRegistry()
	s := &server{cfg: cfg, reg: reg}

	reg.NewGaugeVec("enrolld_info", "服务参数，值恒为 1；参数在标签里", "version", "work", "pool", "db_ms", "sync", "go_version").
		With(cfg.Version, strconv.Itoa(cfg.Work), strconv.Itoa(cfg.Pool), strconv.FormatFloat(cfg.DBMs, 'g', -1, 64), cfg.Sync, cfg.GoVersion).
		Set(1)
	s.requests = reg.NewCounterVec("http_requests_total", "处理完的请求数，按接口和状态码分", "route", "code")
	s.duration = reg.NewHistogramVec("http_request_duration_seconds",
		"服务端耗时：从进入处理函数到处理函数返回（不含内核里的排队和网络）", metrics.LatencyBuckets, "route", "code")
	s.inflight = reg.NewGaugeVec("http_inflight_requests", "正在处理的请求数（在途）", "route")
	s.commits = reg.NewCounter("enroll_commits_total", "已确认落盘方式完成、返回给客户端的选课记录数")
	reg.NewGaugeFunc("enrolld_ready", "就绪检查的结果：1 就绪，0 未就绪（预热中或停止中）", func() float64 {
		if s.ready() {
			return 1
		}
		return 0
	})
	s.leakB = reg.NewGauge("enrolld_leaked_bytes", "-leak-kb 累计泄漏、再也不会释放的内存（字节）")

	s.pool = newPool(reg, cfg.Pool, time.Duration(cfg.DBMs*float64(time.Millisecond)), poolTimeout)
	lm := newLogMetrics(reg)
	var err error
	s.log, err = openCommitLog(cfg.Sync, cfg.Data, interval, lm)
	if err != nil {
		return nil, err
	}
	registerProcessMetrics(reg)
	if cfg.Cgroup != "" {
		registerCgroupMetrics(reg, cfg.Cgroup)
	}
	if cfg.ReportMax > 0 {
		s.reports = make(chan struct{}, cfg.ReportMax)
	}

	s.exports = make([]byte, 1<<20)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range s.exports {
		s.exports[i] = byte(rng.Uint32())
	}
	if cfg.warmup > 0 {
		time.AfterFunc(cfg.warmup, func() { s.warm.Store(true) })
	} else {
		s.warm.Store(true)
	}
	return s, nil
}

func (s *server) ready() bool { return s.warm.Load() && !s.draining.Load() }

func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/courses/{id}", s.instrument("/api/courses/{id}", s.handleCourse))
	mux.HandleFunc("POST /api/enroll", s.instrument("/api/enroll", s.handleEnroll))
	mux.HandleFunc("GET /api/report", s.instrument("/api/report", s.handleReport))
	mux.HandleFunc("GET /api/export", s.instrument("/api/export", s.handleExport))
	mux.Handle("GET /metrics", s.reg.Handler())
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		enc.Encode(s.cfg)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok\n") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		switch {
		case s.draining.Load():
			http.Error(w, "停止中", http.StatusServiceUnavailable)
		case !s.warm.Load():
			http.Error(w, "预热中", http.StatusServiceUnavailable)
		default:
			io.WriteString(w, "ready\n")
		}
	})
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// instrument 给一个接口加上 RED：请求数（按状态码）、服务端耗时直方图、在途请求数。预热未结束时直接返回 503。
func (s *server) instrument(route string, h http.HandlerFunc) http.HandlerFunc {
	inflight := s.inflight.With(route)
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		inflight.Inc()
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		rec.Header().Set("X-Version", s.cfg.Version)
		if !s.warm.Load() {
			http.Error(rec, "预热中，还不能提供服务", http.StatusServiceUnavailable)
		} else {
			h(rec, r)
		}
		inflight.Dec()
		code := strconv.Itoa(rec.code)
		s.duration.With(route, code).Observe(time.Since(start).Seconds())
		s.requests.With(route, code).Inc()
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.code = code
	r.ResponseWriter.WriteHeader(code)
}

// 客户端已经断开时记的状态码（nginx 的习惯）；这时响应已经发不出去了。
const statusClientClosed = 499

type course struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	Teacher   string   `json:"teacher"`
	SeatsLeft int      `json:"seats_left"`
	Schedule  []string `json:"schedule"`
	Checksum  uint64   `json:"checksum"`
	Version   string   `json:"version"`
}

// handleCourse 是读路径：先做一段计算（占本机 CPU），再查一次“数据库”（占一个连接）。
func (s *server) handleCourse(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sum := spin(s.cfg.Work)
	if s.cfg.LeakKB > 0 {
		s.leak(s.cfg.LeakKB << 10)
	}
	if err := s.pool.query(r.Context()); err != nil {
		if errors.Is(err, errPoolTimeout) {
			http.Error(w, "等数据库连接超时", http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(statusClientClosed)
		}
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(course{
		ID: id, Name: "互联网产品开发与运维", Teacher: "任锐", SeatsLeft: int(sum % 120),
		Schedule: []string{"周二 3-4 节 单双周", "周五 3-4 节 单双周"}, Checksum: sum, Version: s.cfg.Version,
	})
}

// leak 分配 n 字节并永远留着（每页写一下，让它真正占用物理内存）。模拟一个越积越多的缓存或泄漏。
func (s *server) leak(n int) {
	b := make([]byte, n)
	for i := 0; i < len(b); i += 4096 {
		b[i] = 1
	}
	s.leakMu.Lock()
	s.leaked = append(s.leaked, b)
	s.leakMu.Unlock()
	s.leakB.Add(float64(n))
}

type enrollReq struct {
	Student string `json:"student"`
	Course  string `json:"course"`
}

type record struct {
	Seq     uint64 `json:"seq"`
	T       int64  `json:"t"`
	Student string `json:"student"`
	Course  string `json:"course"`
}

// handleEnroll 是写路径：记录按 -sync 指定的方式落盘之后，才告诉客户端“选上了”。
func (s *server) handleEnroll(w http.ResponseWriter, r *http.Request) {
	var req enrollReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil || req.Student == "" || req.Course == "" {
		http.Error(w, `请求体应为 {"student":"…","course":"…"}`, http.StatusBadRequest)
		return
	}
	seq := s.seq.Add(1)
	line, _ := json.Marshal(record{Seq: seq, T: time.Now().UnixNano(), Student: req.Student, Course: req.Course})
	if err := s.log.Commit(append(line, '\n')); err != nil {
		http.Error(w, "写入失败："+err.Error(), http.StatusInternalServerError)
		return
	}
	s.commits.Inc()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"ok":true,"seq":%d}`+"\n", seq)
}

// handleReport：每个请求分配 mb MB 内存（默认 4），保持 ms 毫秒（默认 0）后释放。
// 同时在途的请求越多，占用越多：RSS ≈ 基线 + 在途数 × mb（再加上 Go 堆的回收余量）。任务 A3 用它触发 OOM。
// 设了 -report-max 时，已有这么多个在处理，新的请求立即返回 503，不再占内存（有界并发，L-06 第 17 页）。
func (s *server) handleReport(w http.ResponseWriter, r *http.Request) {
	if s.reports != nil {
		select {
		case s.reports <- struct{}{}:
			defer func() { <-s.reports }()
		default:
			http.Error(w, "报表并发已达上限", http.StatusServiceUnavailable)
			return
		}
	}
	mb := intParam(r, "mb", 4, 1, 256)
	hold := intParam(r, "ms", 0, 0, 10000)
	buf := make([]byte, mb<<20)
	for i := 0; i < len(buf); i += 4096 {
		buf[i] = byte(i) // 每页写一下，让内存真正被用到
	}
	if hold > 0 {
		time.Sleep(time.Duration(hold) * time.Millisecond)
	}
	var sum uint64
	for i := 0; i < len(buf); i += 4096 {
		sum += uint64(buf[i])
	}
	fmt.Fprintf(w, `{"bytes":%d,"sum":%d}`+"\n", len(buf), sum)
}

// handleExport 返回 kb KB 的数据（默认 256），选做用大响应把带宽打满。
func (s *server) handleExport(w http.ResponseWriter, r *http.Request) {
	kb := intParam(r, "kb", 256, 1, 1<<20)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(kb<<10))
	for left := kb << 10; left > 0; {
		n := min(left, len(s.exports))
		if _, err := w.Write(s.exports[:n]); err != nil {
			return
		}
		left -= n
	}
}

func intParam(r *http.Request, name string, def, lo, hi int) int {
	v, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil {
		return def
	}
	return max(lo, min(v, hi))
}

// spin 是读路径上的“业务计算”：固定次数的整数运算，只占 CPU，不分配内存、不做 I/O。
func spin(n int) uint64 {
	x := uint64(88172645463325252)
	for i := 0; i < n; i++ {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
	}
	return x
}
