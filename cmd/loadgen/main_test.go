package main

import (
	"bytes"
	"encoding/csv"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestPercentileNearestRank(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, c := range []struct{ p, want float64 }{{50, 5}, {90, 9}, {99, 10}, {100, 10}, {1, 1}} {
		if got := percentile(v, c.p); got != c.want {
			t.Errorf("p%v = %v，期望 %v", c.p, got, c.want)
		}
	}
}

func TestParseRates(t *testing.T) {
	r, err := parseRates("0, 100,250.5")
	if err != nil || len(r) != 3 || r[2] != 250.5 {
		t.Fatalf("%v %v", r, err)
	}
	for _, bad := range []string{"", "a", "-1", "100,,200"} {
		if _, err := parseRates(bad); err == nil {
			t.Errorf("%q 应当报错", bad)
		}
	}
}

func TestRunCountsAndLatency(t *testing.T) {
	var n atomic.Int64
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		i := n.Add(1)
		time.Sleep(5 * time.Millisecond)
		if i%10 == 0 {
			http.Error(w, "busy", http.StatusServiceUnavailable)
			return
		}
		io.WriteString(w, "ok")
	}))
	defer ts.Close()

	o := options{url: ts.URL, method: "GET", plan: steps(time.Second, 0, 200), timeout: time.Second,
		slo: 50 * time.Millisecond, poisson: true, maxInflight: 1000, seed: 1}
	rows := run(o, &timelineStats{slo: o.slo}, io.Discard)
	if len(rows) != 3 || rows[2].name != "all" {
		t.Fatalf("应有 2 个阶段加 all 行，得到 %d 行", len(rows))
	}
	idle, busy := rows[0], rows[1]
	if idle.sent != 0 || !math.IsNaN(idle.p99) {
		t.Errorf("空载阶段不应发请求：%+v", idle)
	}
	// 泊松到达，1 秒 200 个的期望是 200，标准差约 14；放宽到 ±60。
	if busy.sent < 140 || busy.sent > 260 {
		t.Errorf("1 秒内发出 %d 个，偏离 200 太多", busy.sent)
	}
	if busy.ok+busy.httpErr != busy.sent || busy.timeout+busy.other != 0 {
		t.Errorf("计数对不上：%+v", busy)
	}
	if busy.httpErr == 0 || busy.codes[503] != busy.httpErr {
		t.Errorf("应当记到 503：%+v", busy.codes)
	}
	if busy.p50 < 5 || busy.p99 < busy.p50 || busy.max < busy.p99 {
		t.Errorf("延迟不合理：p50 %v p99 %v max %v", busy.p50, busy.p99, busy.max)
	}
	if math.Abs(busy.errRate-float64(busy.httpErr)/float64(busy.sent)) > 1e-9 {
		t.Errorf("错误率口径不对：%v", busy.errRate)
	}
	if rows[2].sent != busy.sent {
		t.Errorf("all 行应只合计有负载的阶段")
	}
}

func TestTimeoutIsCounted(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(500 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))
	defer ts.Close()
	o := options{url: ts.URL, method: "GET", plan: steps(500*time.Millisecond, 20),
		timeout: 100 * time.Millisecond, slo: time.Second, poisson: false, maxInflight: 1000, seed: 1}
	rows := run(o, &timelineStats{slo: o.slo}, io.Discard)
	r := rows[0]
	if r.sent == 0 || r.timeout != r.sent || r.ok != 0 || r.errRate != 1 {
		t.Fatalf("所有请求都应超时：%+v", r)
	}
}

func TestPostBodyTemplate(t *testing.T) {
	var got atomic.Value
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if r.Method == "POST" && r.Header.Get("Content-Type") == "application/json" {
			got.Store(string(b))
		}
	}))
	defer ts.Close()
	o := options{url: ts.URL, method: "POST", body: `{"student":"s{{seq}}"}`, plan: steps(300*time.Millisecond, 20),
		timeout: time.Second, slo: time.Second, maxInflight: 100, seed: 1}
	run(o, &timelineStats{slo: o.slo}, io.Discard)
	s, _ := got.Load().(string)
	if !strings.HasPrefix(s, `{"student":"s`) || strings.Contains(s, "{{seq}}") {
		t.Fatalf("请求体没有替换序号：%q", s)
	}
}

func TestCSVHasHeaderAndRows(t *testing.T) {
	rows := []row{{name: "1", target: 100, start: time.Unix(1000, 0), end: time.Unix(1030, 0), sent: 3, ok: 3,
		okRPS: 0.1, errRate: 0, mean: 1, p50: 1, p90: 1, p99: 1, max: 1, good: 1, lagP99: 0.1, cpu: 5}}
	var b strings.Builder
	if err := writeCSV(&b, rows); err != nil {
		t.Fatal(err)
	}
	recs, err := csv.NewReader(strings.NewReader(b.String())).ReadAll()
	if err != nil || len(recs) != 2 || len(recs[0]) != len(header) || len(recs[1]) != len(header) {
		t.Fatalf("CSV 格式不对：%v\n%s", err, b.String())
	}
	if recs[1][2] != "1000" || recs[1][3] != "1030" {
		t.Errorf("时间戳列不对：%v", recs[1])
	}
}

func TestStopBelow(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer ts.Close()
	o := options{url: ts.URL, method: "GET", plan: steps(300*time.Millisecond, 50, 100, 200),
		timeout: time.Second, slo: time.Second, maxInflight: 1000, seed: 1, stopBelow: 0.5}
	rows := run(o, &timelineStats{slo: o.slo}, io.Discard)
	if len(rows) != 2 || rows[0].name != "1" || rows[1].name != "all" {
		t.Fatalf("全部失败时应在第 1 阶段后停下，得到 %d 行", len(rows))
	}
}

func steps(d time.Duration, rates ...float64) []segment {
	var plan []segment
	for _, r := range rates {
		plan = append(plan, segment{from: r, to: r, dur: d})
	}
	return plan
}

func TestParsePlan(t *testing.T) {
	plan, err := parsePlan("50@2m, 50~600@30s,600@300s")
	if err != nil {
		t.Fatal(err)
	}
	want := []segment{{50, 50, 2 * time.Minute}, {50, 600, 30 * time.Second}, {600, 600, 300 * time.Second}}
	if len(plan) != 3 || plan[0] != want[0] || plan[1] != want[1] || plan[2] != want[2] {
		t.Fatalf("解析结果 %+v", plan)
	}
	if r := plan[1].rateAt(15 * time.Second); math.Abs(r-325) > 1e-9 {
		t.Errorf("爬升到一半应为 325，得到 %v", r)
	}
	for _, bad := range []string{"50", "50@0s", "x@1s", "50~@1s", "-1@1s"} {
		if _, err := parsePlan(bad); err == nil {
			t.Errorf("%q 应当报错", bad)
		}
	}
}

func TestTimelineAndRamp(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "ok") }))
	defer ts.Close()
	o := options{url: ts.URL, method: "GET", plan: []segment{{0, 0, time.Second}, {0, 400, 2 * time.Second}},
		timeout: time.Second, slo: time.Second, poisson: false, maxInflight: 1000, seed: 1}
	tl := &timelineStats{slo: o.slo}
	rows := run(o, tl, io.Discard)
	if len(rows) != 3 {
		t.Fatalf("应有 2 个阶段加 all 行，得到 %d 行", len(rows))
	}
	// 0 → 400 的线性爬升，2 秒，期望发出约 400 个（等间隔到达）
	if n := rows[1].sent; n < 340 || n > 460 {
		t.Errorf("爬升阶段发出 %d 个，期望约 400", n)
	}
	var b bytes.Buffer
	if err := tl.writeCSV(&b); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(b.String()), "\n")
	if len(lines) != 4 || lines[0] != strings.Join(timelineHeader, ",") {
		t.Fatalf("逐秒 CSV 应有表头加 3 行：\n%s", b.String())
	}
	f1, f2 := strings.Split(lines[2], ","), strings.Split(lines[3], ",")
	n1, _ := strconv.Atoi(f1[2])
	n2, _ := strconv.Atoi(f2[2])
	if strings.Split(lines[1], ",")[2] != "0" || n2 <= n1 {
		t.Errorf("第 0 秒应为 0，之后逐秒增多：%v %v %v", lines[1], n1, n2)
	}
}
