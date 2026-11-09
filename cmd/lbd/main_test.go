package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testLB(t *testing.T, mutate func(*options)) *balancer {
	t.Helper()
	o := options{Policy: "rr", CheckPath: "/readyz", Fall: 2, Rise: 2, OutFactor: 3, OutMinMs: 20, OutMinReq: 20,
		OutErrRate: 0.2, OutMaxFrac: 0.34, checkInterval: 20 * time.Millisecond, checkTimeout: 100 * time.Millisecond,
		timeout: time.Second, outWindow: 5 * time.Second, outEject: time.Second}
	if mutate != nil {
		mutate(&o)
	}
	ev, err := openEventLog(filepath.Join(t.TempDir(), "events.csv"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ev.Close() })
	return newBalancer(o, ev)
}

// fakeBackend 是一个最小的实例：/readyz 按 ready 返回，业务请求返回自己的名字。
func fakeBackend(t *testing.T, name string, ready *atomic.Bool, delay time.Duration) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			if !ready.Load() {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
			io.WriteString(w, "ok")
			return
		}
		time.Sleep(delay)
		io.WriteString(w, name)
	}))
	t.Cleanup(ts.Close)
	return strings.TrimPrefix(ts.URL, "http://")
}

func get(t *testing.T, h http.Handler) (int, string) {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/api/courses/1", nil))
	return rec.Code, rec.Body.String()
}

func TestRoundRobinAndNoBackend(t *testing.T) {
	lb := testLB(t, nil)
	if code, _ := get(t, lb); code != 503 {
		t.Fatalf("没有实例时应返回 503，得到 %d", code)
	}
	var r1, r2 atomic.Bool
	r1.Store(true)
	r2.Store(true)
	lb.add(fakeBackend(t, "a", &r1, 0), true)
	lb.add(fakeBackend(t, "b", &r2, 0), true)
	seen := map[string]int{}
	for i := 0; i < 10; i++ {
		_, body := get(t, lb)
		seen[body]++
	}
	if seen["a"] != 5 || seen["b"] != 5 {
		t.Errorf("轮询应当各 5 次，得到 %v", seen)
	}
}

func TestHealthFallRiseAndDrain(t *testing.T) {
	lb := testLB(t, nil)
	var ready atomic.Bool
	addr := fakeBackend(t, "a", &ready, 0)
	lb.add(addr, false) // init=down：等健康检查连续成功 rise 次
	b := lb.find(addr)
	if b.eligible(time.Now()) {
		t.Fatal("init=down 时不应立即接流量")
	}
	ready.Store(true)
	waitFor(t, func() bool { return b.up.Load() }, "就绪后应恢复为健康")
	ready.Store(false)
	waitFor(t, func() bool { return !b.up.Load() }, "连续失败 fall 次后应判为不健康")
	ready.Store(true)
	waitFor(t, func() bool { return b.up.Load() }, "再次恢复")
	b.draining.Store(true)
	if code, _ := get(t, lb); code != 503 {
		t.Errorf("唯一的实例在排空中，应返回 503，得到 %d", code)
	}
	if !lb.remove(addr) || lb.find(addr) != nil {
		t.Error("移除失败")
	}
}

func TestLeastConnections(t *testing.T) {
	lb := testLB(t, func(o *options) { o.Policy = "least" })
	var r atomic.Bool
	r.Store(true)
	slow, fast := fakeBackend(t, "slow", &r, 200*time.Millisecond), fakeBackend(t, "fast", &r, 0)
	lb.add(slow, true)
	lb.add(fast, true)
	lb.find(slow).inflight.Store(5) // 假装慢的那台已经有 5 个在途
	for i := 0; i < 4; i++ {
		if _, body := get(t, lb); body != "fast" {
			t.Fatalf("最少连接应选 fast，得到 %q", body)
		}
	}
}

func TestOutliers(t *testing.T) {
	o := options{OutFactor: 3, OutMinMs: 50, OutMinReq: 50, OutErrRate: 0.2}
	in := []outlierInput{{"a", 500, 20, 0}, {"b", 500, 22, 0}, {"c", 400, 240, 0}, {"d", 10, 900, 0}}
	got := outliers(in, o, 1)
	if len(got) != 1 || got[0] != "c" {
		t.Errorf("应摘除 c（p99 为其他实例中位数的 10 倍以上）；d 请求太少不参与；得到 %v", got)
	}
	if got := outliers(in, o, 0); len(got) != 0 {
		t.Errorf("已达摘除上限时不应再摘：%v", got)
	}
	in = []outlierInput{{"a", 500, 20, 0}, {"b", 500, 30, 0.5}}
	if got := outliers(in, o, 1); len(got) != 1 || got[0] != "b" {
		t.Errorf("错误率 50%% 应被摘除：%v", got)
	}
	in = []outlierInput{{"a", 500, 10, 0}, {"b", 500, 40, 0}}
	if got := outliers(in, o, 1); len(got) != 0 {
		t.Errorf("p99 低于 outlier-min-ms 时不应摘除：%v", got)
	}
}

func TestWindowStatsAndEventLog(t *testing.T) {
	b := &backend{}
	now := time.Now()
	b.win = append(b.win, sample{t: now.Add(-time.Minute), ms: 999})
	for i := 1; i <= 100; i++ {
		b.win = append(b.win, sample{t: now, ms: float64(i), bad: i > 95})
	}
	n, p99, er := b.windowStats(now, 10*time.Second)
	if n != 100 || p99 != 99 || er != 0.05 {
		t.Errorf("窗口统计 n=%d p99=%v err=%v，应为 100、99、0.05（过期样本丢弃）", n, p99, er)
	}
	path := filepath.Join(t.TempDir(), "ev.csv")
	ev, _ := openEventLog(path)
	ev.write("127.0.0.1:1", "down", "x")
	ev.Close()
	ev, _ = openEventLog(path)
	ev.write("127.0.0.1:1", "up", "y")
	ev.Close()
	data, _ := os.ReadFile(path)
	if lines := strings.Split(strings.TrimSpace(string(data)), "\n"); len(lines) != 3 || !strings.HasPrefix(lines[0], "unix_ms,") {
		t.Errorf("事件日志应为表头加 2 行（追加写时不重复表头）：\n%s", data)
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
