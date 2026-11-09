// lbd 是实验 3 的负载均衡器：把请求分给一组 enrolld 实例，并用健康检查决定哪些实例能接流量。
//
// 选择实例：只在“健康、没有在排空、没有被摘除”的实例里选；-policy rr 轮询，least 选在途请求最少的。
// 主动健康检查：每隔 -check-interval 请求一次 -check-path（默认 /readyz），超时或非 200 记一次失败；
// 连续失败 -fall 次判为不健康，连续成功 -rise 次恢复（L-05 第 23 页、思考题 1 第四幕）。
// 被动摘除（-outlier，默认关）：按最近 -outlier-window 内真实请求的 p99 和错误率，把明显比其他实例差的那一台
// 暂时摘除 -outlier-eject 这么久；同时被摘除的不超过总数的 -outlier-max-frac（L-12 第 14 页）。
//
// 端口：-addr 接业务流量；-admin 是管理接口和 /metrics：
//
//	GET    /backends                      各实例的状态（JSON）
//	POST   /backends?addr=…&init=up|down  加入一个实例；init=down 表示先不接流量，等健康检查连续成功 -rise 次
//	DELETE /backends?addr=…               移除一个实例
//	POST   /drain?addr=…                  排空：不再给它新请求，在途的照常做完
//	GET    /metrics、/config
//
// 每个状态变化（加入、移除、不健康、恢复、摘除、排空……）记一行事件，写到 -events 文件（CSV）。
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"enroll-lab/internal/metrics"
)

type options struct {
	Addr          string  `json:"addr"`
	Admin         string  `json:"admin"`
	Policy        string  `json:"policy"`
	CheckPath     string  `json:"check_path"`
	CheckInterval string  `json:"check_interval"`
	CheckTimeout  string  `json:"check_timeout"`
	Fall          int     `json:"fall"`
	Rise          int     `json:"rise"`
	Timeout       string  `json:"timeout"`
	Outlier       bool    `json:"outlier"`
	OutWindow     string  `json:"outlier_window"`
	OutFactor     float64 `json:"outlier_factor"`
	OutMinMs      float64 `json:"outlier_min_ms"`
	OutMinReq     int     `json:"outlier_min_requests"`
	OutErrRate    float64 `json:"outlier_error_rate"`
	OutMaxFrac    float64 `json:"outlier_max_frac"`
	OutEject      string  `json:"outlier_eject"`
	Events        string  `json:"events"`
	Started       string  `json:"started"`

	checkInterval, checkTimeout, timeout, outWindow, outEject time.Duration
}

type backend struct {
	addr     string
	proxy    *httputil.ReverseProxy
	up       atomic.Bool
	draining atomic.Bool
	ejected  atomic.Int64 // 摘除到这个时刻（UnixNano）；0 表示没有被摘除
	inflight atomic.Int64
	served   atomic.Uint64
	stop     chan struct{}

	mu  sync.Mutex
	win []sample // 最近 -outlier-window 内的请求
}

type sample struct {
	t   time.Time
	ms  float64
	bad bool
}

func (b *backend) eligible(now time.Time) bool {
	return b.up.Load() && !b.draining.Load() && b.ejected.Load() < now.UnixNano()
}

type balancer struct {
	opt      options
	mu       sync.RWMutex
	backends []*backend
	next     atomic.Uint64
	tr       *http.Transport
	client   *http.Client
	events   *eventLog

	reg       *metrics.Registry
	requests  *metrics.CounterVec
	duration  *metrics.HistogramVec
	inflight  *metrics.GaugeVec
	upG       *metrics.GaugeVec
	eligibleG *metrics.GaugeVec
	noBackend *metrics.Counter
	eventsC   *metrics.CounterVec
}

func main() {
	var o options
	var backends string
	flag.StringVar(&o.Addr, "addr", "127.0.0.1:8080", "业务流量的监听地址")
	flag.StringVar(&o.Admin, "admin", "127.0.0.1:8090", "管理接口与 /metrics 的监听地址")
	flag.StringVar(&backends, "backends", "", "启动时就加入的实例，逗号分隔，例如 127.0.0.1:8101,127.0.0.1:8102（视为健康）")
	flag.StringVar(&o.Policy, "policy", "rr", "选择实例的方式：rr 轮询；least 在途请求最少")
	flag.StringVar(&o.CheckPath, "check-path", "/readyz", "健康检查请求的路径")
	flag.DurationVar(&o.checkInterval, "check-interval", time.Second, "健康检查的间隔")
	flag.DurationVar(&o.checkTimeout, "check-timeout", 500*time.Millisecond, "一次健康检查的超时")
	flag.IntVar(&o.Fall, "fall", 3, "连续失败几次判为不健康")
	flag.IntVar(&o.Rise, "rise", 2, "连续成功几次恢复为健康")
	flag.DurationVar(&o.timeout, "timeout", 2*time.Second, "转发给实例的请求的超时（等响应头）")
	flag.BoolVar(&o.Outlier, "outlier", false, "打开被动摘除：按真实请求的 p99 与错误率摘除明显变差的实例")
	flag.DurationVar(&o.outWindow, "outlier-window", 10*time.Second, "被动摘除看最近多长时间的请求")
	flag.Float64Var(&o.OutFactor, "outlier-factor", 3, "某台的 p99 超过其他实例 p99 中位数的这么多倍，判为离群")
	flag.Float64Var(&o.OutMinMs, "outlier-min-ms", 50, "p99 低于这个值（毫秒）时不判为离群，免得在都很快时误判")
	flag.IntVar(&o.OutMinReq, "outlier-min-requests", 50, "窗口内请求少于这个数的实例不参与判断")
	flag.Float64Var(&o.OutErrRate, "outlier-error-rate", 0.2, "窗口内 5xx 与转发失败的比例超过它，也判为离群")
	flag.Float64Var(&o.OutMaxFrac, "outlier-max-frac", 0.34, "同时被摘除的实例最多占总数的比例（至少允许摘除 1 台）")
	flag.DurationVar(&o.outEject, "outlier-eject", 30*time.Second, "被动摘除一次持续多久")
	flag.StringVar(&o.Events, "events", "data/lbd-events.csv", "事件日志（CSV，追加写）")
	flag.Parse()
	o.CheckInterval, o.CheckTimeout, o.Timeout = o.checkInterval.String(), o.checkTimeout.String(), o.timeout.String()
	o.OutWindow, o.OutEject = o.outWindow.String(), o.outEject.String()
	o.Started = time.Now().Format(time.RFC3339)
	if o.Policy != "rr" && o.Policy != "least" {
		log.Fatal("-policy 只能是 rr 或 least")
	}
	if o.Fall < 1 || o.Rise < 1 {
		log.Fatal("-fall、-rise 至少为 1")
	}

	ev, err := openEventLog(o.Events)
	if err != nil {
		log.Fatal(err)
	}
	lb := newBalancer(o, ev)
	for _, a := range strings.Split(backends, ",") {
		if a = strings.TrimSpace(a); a != "" {
			if err := lb.add(a, true); err != nil {
				log.Fatal(err)
			}
		}
	}
	if o.Outlier {
		go lb.outlierLoop()
	}

	srv := &http.Server{Addr: o.Addr, Handler: lb, ReadHeaderTimeout: 5 * time.Second}
	adm := &http.Server{Addr: o.Admin, Handler: lb.adminRoutes(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := adm.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(sctx)
		adm.Shutdown(sctx)
	}()
	log.Printf("lbd 监听 %s（管理接口 %s）；%s；健康检查 %s 每 %v，超时 %v，fall %d、rise %d；被动摘除 %v；事件写入 %s",
		o.Addr, o.Admin, o.Policy, o.CheckPath, o.checkInterval, o.checkTimeout, o.Fall, o.Rise, o.Outlier, o.Events)
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
	ev.Close()
}

func newBalancer(o options, ev *eventLog) *balancer {
	reg := metrics.NewRegistry()
	tr := &http.Transport{
		MaxIdleConns:          0,
		MaxIdleConnsPerHost:   10000,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: o.timeout,
		DialContext:           (&net.Dialer{Timeout: time.Second, KeepAlive: 30 * time.Second}).DialContext,
	}
	lb := &balancer{opt: o, tr: tr, events: ev, reg: reg,
		client:    &http.Client{Timeout: o.checkTimeout, Transport: &http.Transport{DisableKeepAlives: true}},
		requests:  reg.NewCounterVec("lb_requests_total", "lbd 转发完的请求数，按实例和状态码分；502 表示转发失败，503 表示没有可用的实例", "backend", "code"),
		duration:  reg.NewHistogramVec("lb_request_duration_seconds", "lbd 看到的耗时：从收到请求到转发的响应写完", metrics.LatencyBuckets, "backend"),
		inflight:  reg.NewGaugeVec("lb_inflight_requests", "正在转发给该实例的请求数", "backend"),
		upG:       reg.NewGaugeVec("lb_backend_up", "健康检查的结论：1 健康，0 不健康", "backend"),
		eligibleG: reg.NewGaugeVec("lb_backend_eligible", "能否接新请求：健康、没有排空、没有被摘除时为 1", "backend"),
		noBackend: reg.NewCounter("lb_no_backend_total", "没有可用实例、直接返回 503 的请求数"),
		eventsC:   reg.NewCounterVec("lb_events_total", "状态变化的次数，按事件分", "event"),
	}
	reg.NewGaugeFunc("lb_backends", "当前登记的实例数（含不健康、排空中的）", func() float64 {
		lb.mu.RLock()
		defer lb.mu.RUnlock()
		return float64(len(lb.backends))
	})
	reg.NewGaugeFunc("lb_backends_eligible", "当前能接新请求的实例数", func() float64 {
		n := 0
		now := time.Now()
		for _, b := range lb.list() {
			if b.eligible(now) {
				n++
			}
		}
		return float64(n)
	})
	return lb
}

func (lb *balancer) list() []*backend {
	lb.mu.RLock()
	defer lb.mu.RUnlock()
	return slices.Clone(lb.backends)
}

func (lb *balancer) find(addr string) *backend {
	for _, b := range lb.list() {
		if b.addr == addr {
			return b
		}
	}
	return nil
}

func (lb *balancer) event(b, ev, detail string) {
	lb.eventsC.With(ev).Inc()
	lb.events.write(b, ev, detail)
	log.Printf("%s %s %s", b, ev, detail)
}

func (lb *balancer) add(addr string, up bool) error {
	u, err := url.Parse("http://" + addr)
	if err != nil || u.Host == "" {
		return fmt.Errorf("实例地址 %q 不合法，应写成 主机:端口", addr)
	}
	b := &backend{addr: addr, stop: make(chan struct{})}
	b.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(u)
			pr.Out.Host = pr.In.Host
		},
		Transport: lb.tr,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			http.Error(w, "转发失败："+err.Error(), http.StatusBadGateway)
		},
	}
	b.up.Store(up)
	lb.mu.Lock()
	for _, x := range lb.backends {
		if x.addr == addr {
			lb.mu.Unlock()
			return fmt.Errorf("%s 已经在列表里", addr)
		}
	}
	lb.backends = append(lb.backends, b)
	lb.mu.Unlock()
	lb.gauges(b)
	state := "up"
	if !up {
		state = "down"
	}
	lb.event(addr, "added", "init="+state)
	go lb.healthLoop(b)
	return nil
}

func (lb *balancer) remove(addr string) bool {
	lb.mu.Lock()
	i := slices.IndexFunc(lb.backends, func(b *backend) bool { return b.addr == addr })
	if i < 0 {
		lb.mu.Unlock()
		return false
	}
	b := lb.backends[i]
	lb.backends = slices.Delete(lb.backends, i, i+1)
	lb.mu.Unlock()
	close(b.stop)
	lb.upG.With(addr).Set(0)
	lb.eligibleG.With(addr).Set(0)
	lb.event(addr, "removed", fmt.Sprintf("inflight=%d", b.inflight.Load()))
	return true
}

func (lb *balancer) gauges(b *backend) {
	lb.upG.With(b.addr).Set(boolf(b.up.Load()))
	lb.eligibleG.With(b.addr).Set(boolf(b.eligible(time.Now())))
}

func boolf(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// pick 在能接新请求的实例里选一个；一个都没有时返回 nil。
func (lb *balancer) pick() *backend {
	now := time.Now()
	all := lb.list()
	var cands []*backend
	for _, b := range all {
		if b.eligible(now) {
			cands = append(cands, b)
		}
	}
	if len(cands) == 0 {
		return nil
	}
	start := int(lb.next.Add(1) % uint64(len(cands)))
	if lb.opt.Policy == "rr" {
		return cands[start]
	}
	best := cands[start]
	for k := 1; k < len(cands); k++ {
		c := cands[(start+k)%len(cands)]
		if c.inflight.Load() < best.inflight.Load() {
			best = c
		}
	}
	return best
}

func (lb *balancer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	b := lb.pick()
	if b == nil {
		lb.noBackend.Inc()
		lb.requests.With("none", "503").Inc()
		http.Error(w, "没有可用的实例", http.StatusServiceUnavailable)
		return
	}
	b.inflight.Add(1)
	lb.inflight.With(b.addr).Set(float64(b.inflight.Load()))
	rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
	b.proxy.ServeHTTP(rec, r)
	b.inflight.Add(-1)
	lb.inflight.With(b.addr).Set(float64(b.inflight.Load()))
	b.served.Add(1)
	d := time.Since(start)
	lb.duration.With(b.addr).Observe(d.Seconds())
	lb.requests.With(b.addr, strconv.Itoa(rec.code)).Inc()
	if lb.opt.Outlier {
		b.mu.Lock()
		b.win = append(b.win, sample{t: start, ms: float64(d) / float64(time.Millisecond), bad: rec.code >= 500})
		b.mu.Unlock()
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

// healthLoop 是一个实例的主动健康检查。
func (lb *balancer) healthLoop(b *backend) {
	t := time.NewTicker(lb.opt.checkInterval)
	defer t.Stop()
	var okRun, failRun int
	for {
		select {
		case <-b.stop:
			return
		case <-t.C:
		}
		err := lb.check(b.addr)
		if err == nil {
			okRun, failRun = okRun+1, 0
			if !b.up.Load() && okRun >= lb.opt.Rise {
				b.up.Store(true)
				lb.event(b.addr, "up", fmt.Sprintf("连续成功 %d 次", okRun))
			}
		} else {
			okRun, failRun = 0, failRun+1
			if b.up.Load() && failRun >= lb.opt.Fall {
				b.up.Store(false)
				lb.event(b.addr, "down", fmt.Sprintf("连续失败 %d 次：%v", failRun, err))
			}
		}
		lb.gauges(b)
	}
}

func (lb *balancer) check(addr string) error {
	resp, err := lb.client.Get("http://" + addr + lb.opt.CheckPath)
	if err != nil {
		return err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("状态码 %d", resp.StatusCode)
	}
	return nil
}

// windowStats 返回一个实例最近 window 内的请求数、p99（毫秒）和错误率，并丢掉更早的样本。
func (b *backend) windowStats(now time.Time, window time.Duration) (n int, p99, errRate float64) {
	b.mu.Lock()
	cut := now.Add(-window)
	i := 0
	for i < len(b.win) && b.win[i].t.Before(cut) {
		i++
	}
	b.win = b.win[i:]
	lat := make([]float64, 0, len(b.win))
	bad := 0
	for _, s := range b.win {
		lat = append(lat, s.ms)
		if s.bad {
			bad++
		}
	}
	b.mu.Unlock()
	n = len(lat)
	if n == 0 {
		return 0, 0, 0
	}
	slices.Sort(lat)
	k := (99*n + 99) / 100 // 最近秩法：第 ceil(0.99 × n) 个
	return n, lat[k-1], float64(bad) / float64(n)
}

type outlierInput struct {
	addr    string
	n       int
	p99     float64
	errRate float64
}

// outliers 按 p99 与其他实例 p99 中位数之比、以及错误率，挑出离群的实例；allowed 是这次最多还能摘除几台。
func outliers(in []outlierInput, o options, allowed int) []string {
	var out []string
	for _, x := range in {
		if allowed <= 0 {
			break
		}
		if x.n < o.OutMinReq {
			continue
		}
		var others []float64
		for _, y := range in {
			if y.addr != x.addr && y.n >= o.OutMinReq {
				others = append(others, y.p99)
			}
		}
		slow := false
		if len(others) > 0 {
			slices.Sort(others)
			med := others[len(others)/2]
			if len(others)%2 == 0 {
				med = (others[len(others)/2-1] + others[len(others)/2]) / 2
			}
			slow = x.p99 > o.OutMinMs && x.p99 > o.OutFactor*med
		}
		if slow || x.errRate > o.OutErrRate {
			out = append(out, x.addr)
			allowed--
		}
	}
	return out
}

func (lb *balancer) outlierLoop() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		all := lb.list()
		var in []outlierInput
		ejected := 0
		for _, b := range all {
			until := b.ejected.Load()
			if until > 0 && until < now.UnixNano() {
				b.ejected.Store(0)
				lb.event(b.addr, "restored", "被动摘除到期")
				lb.gauges(b)
			}
			if b.ejected.Load() > 0 {
				ejected++
				continue
			}
			if !b.up.Load() || b.draining.Load() {
				continue
			}
			n, p99, er := b.windowStats(now, lb.opt.outWindow)
			in = append(in, outlierInput{addr: b.addr, n: n, p99: p99, errRate: er})
		}
		limit := max(1, int(lb.opt.OutMaxFrac*float64(len(all))))
		for _, addr := range outliers(in, lb.opt, limit-ejected) {
			b := lb.find(addr)
			if b == nil {
				continue
			}
			_, p99, er := b.windowStats(now, lb.opt.outWindow)
			b.ejected.Store(now.Add(lb.opt.outEject).UnixNano())
			b.mu.Lock()
			b.win = nil
			b.mu.Unlock()
			lb.event(addr, "ejected", fmt.Sprintf("p99=%.1fms err=%.3f，摘除 %v", p99, er, lb.opt.outEject))
			lb.gauges(b)
		}
	}
}

type backendView struct {
	Addr     string `json:"addr"`
	Up       bool   `json:"up"`
	Draining bool   `json:"draining"`
	Ejected  bool   `json:"ejected"`
	Eligible bool   `json:"eligible"`
	Inflight int64  `json:"inflight"`
	Served   uint64 `json:"served"`
}

func (lb *balancer) adminRoutes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", lb.reg.Handler())
	mux.HandleFunc("GET /config", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, lb.opt) })
	mux.HandleFunc("GET /backends", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		views := []backendView{}
		for _, b := range lb.list() {
			views = append(views, backendView{Addr: b.addr, Up: b.up.Load(), Draining: b.draining.Load(),
				Ejected: b.ejected.Load() > now.UnixNano(), Eligible: b.eligible(now), Inflight: b.inflight.Load(), Served: b.served.Load()})
		}
		writeJSON(w, views)
	})
	mux.HandleFunc("POST /backends", func(w http.ResponseWriter, r *http.Request) {
		addr := r.URL.Query().Get("addr")
		state := r.URL.Query().Get("init")
		if state != "" && state != "up" && state != "down" {
			http.Error(w, "init 只能是 up 或 down", http.StatusBadRequest)
			return
		}
		if err := lb.add(addr, state != "down"); err != nil {
			http.Error(w, err.Error(), http.StatusConflict)
			return
		}
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("DELETE /backends", func(w http.ResponseWriter, r *http.Request) {
		if !lb.remove(r.URL.Query().Get("addr")) {
			http.Error(w, "没有这个实例", http.StatusNotFound)
			return
		}
		io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("POST /drain", func(w http.ResponseWriter, r *http.Request) {
		addr := r.URL.Query().Get("addr")
		b := lb.find(addr)
		if b == nil {
			http.Error(w, "没有这个实例", http.StatusNotFound)
			return
		}
		if !b.draining.Swap(true) {
			lb.event(addr, "draining", fmt.Sprintf("inflight=%d", b.inflight.Load()))
		}
		lb.gauges(b)
		io.WriteString(w, "ok\n")
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// eventLog 把事件追加到 CSV：unix_ms,time,backend,event,detail。
type eventLog struct {
	mu sync.Mutex
	f  *os.File
	w  *csv.Writer
}

func openEventLog(path string) (*eventLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	e := &eventLog{f: f, w: csv.NewWriter(f)}
	if st, err := f.Stat(); err == nil && st.Size() == 0 {
		e.w.Write([]string{"unix_ms", "time", "backend", "event", "detail"})
		e.w.Flush()
	}
	return e, nil
}

func (e *eventLog) write(b, ev, detail string) {
	now := time.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.w.Write([]string{strconv.FormatInt(now.UnixMilli(), 10), now.Format("15:04:05.000"), b, ev, detail})
	e.w.Flush()
}

func (e *eventLog) Close() error { return e.f.Close() }
