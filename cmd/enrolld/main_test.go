package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func testServer(t *testing.T, cfg config) (*server, *httptest.Server) {
	t.Helper()
	if cfg.Data == "" {
		cfg.Data = t.TempDir()
	}
	if cfg.Sync == "" {
		cfg.Sync = "each"
	}
	if cfg.Pool == 0 {
		cfg.Pool = 4
	}
	s, err := newServer(cfg, time.Second, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(s.routes())
	t.Cleanup(func() {
		ts.Close()
		s.log.Close()
	})
	return s, ts
}

func scrape(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		b.WriteString(sc.Text() + "\n")
	}
	return b.String()
}

// metricValue 从 /metrics 原文里取一行的值；找不到返回 -1。
func metricValue(text, series string) float64 {
	for _, line := range strings.Split(text, "\n") {
		if v, ok := strings.CutPrefix(line, series+" "); ok {
			f, _ := strconv.ParseFloat(v, 64)
			return f
		}
	}
	return -1
}

func TestCourse(t *testing.T) {
	_, ts := testServer(t, config{Work: 1000, DBMs: 1})
	resp, err := http.Get(ts.URL + "/api/courses/42")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var c course
	if err := json.NewDecoder(resp.Body).Decode(&c); err != nil || resp.StatusCode != 200 || c.ID != "42" {
		t.Fatalf("status %d, course %+v, err %v", resp.StatusCode, c, err)
	}
	m := scrape(t, ts)
	if v := metricValue(m, `http_requests_total{route="/api/courses/{id}",code="200"}`); v != 1 {
		t.Errorf("请求计数 = %v，期望 1\n%s", v, m)
	}
	if v := metricValue(m, `db_query_duration_seconds_count`); v != 1 {
		t.Errorf("查询计数 = %v", v)
	}
}

// 池大小为 1、每次查询 60 ms 时，同时来的 3 个请求只能一个一个过：最后一个至少等 120 ms。
func TestPoolSerializes(t *testing.T) {
	_, ts := testServer(t, config{Pool: 1, DBMs: 60})
	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(ts.URL + "/api/courses/1")
			if err == nil {
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	if d := time.Since(start); d < 180*time.Millisecond {
		t.Fatalf("3 个请求用了 %v，池没有起到限制作用", d)
	}
	m := scrape(t, ts)
	if v := metricValue(m, `db_pool_wait_seconds_bucket{le="0.05"}`); v != 1 {
		t.Errorf("应当只有 1 个请求几乎不用等连接，得到 %v\n%s", v, m)
	}
	if v := metricValue(m, "db_pool_in_use"); v != 0 {
		t.Errorf("结束后占用数应为 0，得到 %v", v)
	}
}

func TestPoolTimeoutGives503(t *testing.T) {
	s, ts := testServer(t, config{Pool: 1, DBMs: 300})
	s.pool.timeout = 50 * time.Millisecond
	done := make(chan struct{})
	go func() {
		resp, err := http.Get(ts.URL + "/api/courses/1")
		if err == nil {
			resp.Body.Close()
		}
		close(done)
	}()
	time.Sleep(30 * time.Millisecond)
	resp, err := http.Get(ts.URL + "/api/courses/2")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("池被占满且等待超时，应返回 503，得到 %d", resp.StatusCode)
	}
	<-done
	if v := metricValue(scrape(t, ts), `http_requests_total{route="/api/courses/{id}",code="503"}`); v != 1 {
		t.Errorf("503 计数 = %v", v)
	}
}

func enroll(t *testing.T, url string, i int) {
	t.Helper()
	body := fmt.Sprintf(`{"student":"s%d","course":"c1"}`, i)
	resp, err := http.Post(url+"/api/enroll", "application/json", strings.NewReader(body))
	if err != nil {
		t.Error(err)
		return
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("enroll 返回 %d", resp.StatusCode)
	}
}

func TestEnrollModes(t *testing.T) {
	for _, mode := range []string{"each", "group", "interval"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			s, ts := testServer(t, config{Sync: mode, Data: dir})
			const n = 40
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); enroll(t, ts.URL, i) }()
			}
			wg.Wait()
			if mode == "interval" {
				time.Sleep(120 * time.Millisecond) // 等后台 fsync（测试里间隔为 50 ms）
			}
			m := scrape(t, ts)
			commits := metricValue(m, "enroll_commits_total")
			fsyncs := metricValue(m, "enroll_fsync_duration_seconds_count")
			records := metricValue(m, "enroll_commit_batch_size_sum")
			if commits != n || records != n {
				t.Fatalf("commits=%v，batch 合计=%v，期望都是 %d", commits, records, n)
			}
			switch mode {
			case "each":
				if fsyncs != n {
					t.Errorf("each 应当每笔一次 fsync：%v 次", fsyncs)
				}
			default:
				if fsyncs < 1 || fsyncs > n {
					t.Errorf("%s 的 fsync 次数 %v 不合理", mode, fsyncs)
				}
			}
			t.Logf("%s：%d 笔提交，%v 次 fsync", mode, n, fsyncs)
			s.log.Close()
			b, err := os.ReadFile(filepath.Join(dir, "enroll.log"))
			if err != nil {
				t.Fatal(err)
			}
			if lines := bytes.Count(b, []byte("\n")); lines != n {
				t.Errorf("日志有 %d 行，期望 %d", lines, n)
			}
		})
	}
}

// 组提交时，后台正在 fsync 的那段时间里到达的提交会合并进下一次 fsync。
func TestGroupCommitBatches(t *testing.T) {
	// tmpfs 上 fsync 几乎不花时间，提交来不及排队，偶尔一次也合并不了；这里让每次 fsync 多花 2 ms，接近云盘。
	orig := syncFile
	syncFile = func(f *os.File) error { time.Sleep(2 * time.Millisecond); return orig(f) }
	t.Cleanup(func() { syncFile = orig })
	_, ts := testServer(t, config{Sync: "group"})
	const n = 200
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); enroll(t, ts.URL, i) }()
	}
	wg.Wait()
	m := scrape(t, ts)
	fsyncs := metricValue(m, "enroll_fsync_duration_seconds_count")
	if fsyncs >= n {
		t.Errorf("200 个并发提交用了 %v 次 fsync，没有合并", fsyncs)
	}
	t.Logf("200 个并发提交，%v 次 fsync", fsyncs)
}

func TestEnrollBadRequest(t *testing.T) {
	_, ts := testServer(t, config{})
	resp, err := http.Post(ts.URL+"/api/enroll", "application/json", strings.NewReader(`{"student":""}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("缺字段应返回 400，得到 %d", resp.StatusCode)
	}
}

func TestBadSyncMode(t *testing.T) {
	if _, err := openCommitLog("never", t.TempDir(), time.Second, nil); err == nil {
		t.Fatal("未知的 -sync 应当报错")
	}
}

func TestExportAndReport(t *testing.T) {
	_, ts := testServer(t, config{})
	resp, err := http.Get(ts.URL + "/api/export?kb=1500")
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	buf.ReadFrom(resp.Body)
	resp.Body.Close()
	if buf.Len() != 1500<<10 {
		t.Errorf("export 返回 %d 字节，期望 %d", buf.Len(), 1500<<10)
	}
	resp, err = http.Get(ts.URL + "/api/report?mb=2")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("report 返回 %d", resp.StatusCode)
	}
}

func TestMetricsAndConfig(t *testing.T) {
	_, ts := testServer(t, config{Version: "v9", Work: 5, Pool: 7, DBMs: 1})
	http.Get(ts.URL + "/api/courses/1")
	m := scrape(t, ts)
	for _, want := range []string{
		`enrolld_info{version="v9",work="5",pool="7",db_ms="1",sync="each",go_version="`,
		"enrolld_ready 1",
		"process_start_time_seconds ",
		`http_request_duration_seconds_bucket{route="/api/courses/{id}",code="200",le="0.05"}`,
		`http_inflight_requests{route="/api/courses/{id}"} 0`,
		"db_pool_size 7",
		"# TYPE process_cpu_seconds_total counter",
		"process_resident_memory_bytes ",
		"process_threads ",
		"go_gc_cycles_total ",
	} {
		if !strings.Contains(m, want) {
			t.Errorf("/metrics 缺少 %q", want)
		}
	}
	if v := metricValue(m, "process_threads"); v < 1 {
		t.Errorf("process_threads = %v", v)
	}
	resp, err := http.Get(ts.URL + "/config")
	if err != nil {
		t.Fatal(err)
	}
	var c config
	json.NewDecoder(resp.Body).Decode(&c)
	resp.Body.Close()
	if c.Pool != 7 || c.Work != 5 {
		t.Errorf("/config = %+v", c)
	}
}

func TestBenchFsync(t *testing.T) {
	var out bytes.Buffer
	dir := t.TempDir()
	if err := benchFsync(dir, 20, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "p99") {
		t.Errorf("输出缺少分位数：%s", out.String())
	}
	if _, err := os.Stat(filepath.Join(dir, "fsync-bench.tmp")); !os.IsNotExist(err) {
		t.Error("临时文件没有删除")
	}
}

func TestNearestRank(t *testing.T) {
	for _, c := range []struct {
		p    float64
		n    int
		want int
	}{{50, 10, 4}, {99, 100, 98}, {99, 1000, 989}, {100, 7, 6}, {1, 7, 0}, {99, 1, 0}} {
		if got := nearestRank(c.p, c.n); got != c.want {
			t.Errorf("nearestRank(%v, %d) = %d，期望 %d", c.p, c.n, got, c.want)
		}
	}
}

func TestWarmupAndDrain(t *testing.T) {
	s, ts := testServer(t, config{warmup: 300 * time.Millisecond})
	get := func(path string) int {
		resp, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if get("/healthz") != 200 || get("/readyz") != 503 || get("/api/courses/1") != 503 {
		t.Fatal("预热中：存活应为 200，就绪与业务接口应为 503")
	}
	time.Sleep(400 * time.Millisecond)
	if get("/readyz") != 200 || get("/api/courses/1") != 200 {
		t.Fatal("预热结束后应当就绪")
	}
	s.draining.Store(true)
	if get("/readyz") != 503 || get("/api/courses/1") != 200 {
		t.Fatal("停止中：就绪应为 503，在途与新到的业务请求照常处理")
	}
}

func TestReportMaxAndHold(t *testing.T) {
	_, ts := testServer(t, config{ReportMax: 2})
	var wg sync.WaitGroup
	codes := make([]int, 5)
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := http.Get(ts.URL + "/api/report?mb=1&ms=300")
			if err == nil {
				codes[i] = resp.StatusCode
				resp.Body.Close()
			}
		}()
	}
	wg.Wait()
	ok, busy := 0, 0
	for _, c := range codes {
		switch c {
		case 200:
			ok++
		case 503:
			busy++
		}
	}
	if ok != 2 || busy != 3 {
		t.Errorf("并发上限 2：应有 2 个成功、3 个 503，得到 %v", codes)
	}
}

func TestLeak(t *testing.T) {
	_, ts := testServer(t, config{LeakKB: 64})
	for i := 0; i < 4; i++ {
		resp, err := http.Get(ts.URL + "/api/courses/1")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	if v := metricValue(scrape(t, ts), "enrolld_leaked_bytes"); v != 4*64*1024 {
		t.Errorf("enrolld_leaked_bytes = %v，应为 %d", v, 4*64*1024)
	}
}

func TestCgroupParsing(t *testing.T) {
	if got := parseCgroupFile("0::/system.slice/lab3-a.service\n"); got != "/sys/fs/cgroup/system.slice/lab3-a.service" {
		t.Errorf("parseCgroupFile = %q", got)
	}
	if got := parseCgroupFile("12:cpu:/x\n"); got != "" {
		t.Errorf("cgroup v1 应返回空串，得到 %q", got)
	}
	kv := parseKV("usage_usec 1500000\nnr_periods 40\nnr_throttled 30\nthrottled_usec 3000000\n")
	if kv["nr_periods"] != 40 || kv["throttled_usec"] != 3e6 {
		t.Errorf("parseKV = %v", kv)
	}
	for _, c := range []struct {
		in    string
		cores float64
		ok    bool
	}{{"50000 100000", 0.5, true}, {"max 100000", 0, true}, {"bad", 0, false}} {
		cores, period, ok := parseCPUMax(c.in)
		if cores != c.cores || ok != c.ok || (ok && period != 100*time.Millisecond) {
			t.Errorf("parseCPUMax(%q) = %v %v %v", c.in, cores, period, ok)
		}
	}
}

func TestQuotaArithmetic(t *testing.T) {
	// L-09 第 12 页：周期 100 ms、配额 3 核，3、6、12 个线程每周期被停 0、50、75 ms；配额 0.5 核、2 个线程被停 75 ms
	for _, c := range []struct {
		cores float64
		n     int
		want  float64
	}{{3, 3, 0}, {3, 6, 50}, {3, 12, 75}, {0.5, 1, 50}, {0.5, 2, 75}, {3, 2, 0}} {
		if got := expectedStopMs(c.cores, 100*time.Millisecond, c.n); math.Abs(got-c.want) > 1e-9 {
			t.Errorf("配额 %v 核、%d 个线程：被停 %v ms，应为 %v", c.cores, c.n, got, c.want)
		}
	}
	r := burnResult{n: 2, periods: 100, throttled: 15} // 100 个周期按 CPU 累计被停 15 秒
	if got := r.stopPerThreadMs(); math.Abs(got-75) > 1e-9 {
		t.Errorf("每线程每周期被停 %v ms，应为 75", got)
	}
}
