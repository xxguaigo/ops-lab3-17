package main

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestDesiredReplicas(t *testing.T) {
	nan := math.NaN()
	for _, c := range []struct {
		current     int
		signal      float64
		lo, hi, out int
	}{
		{2, 1.16, 2, 6, 4}, // L-11 第 13 页的例子：⌈2 × 116 ÷ 70⌉ = 4
		{4, 0.75, 2, 6, 4}, // 75 ÷ 70 = 1.07，在 10% 容差内，不动
		{4, 0.35, 2, 6, 2}, // ⌈4 × 0.5⌉ = 2
		{2, 5.0, 2, 20, 6}, // 期望 15，但每次最多扩到 max(2 × 2, 2 + 4) = 6
		{3, 2.0, 2, 4, 4},  // 夹到上限
		{3, nan, 4, 6, 4},  // 取不到信号：保持当前数，但不低于最少数
	} {
		if got := desiredReplicas(c.current, c.signal, 0.7, 0.1, c.lo, c.hi); got != c.out {
			t.Errorf("当前 %d、信号 %v：期望 %d，得到 %d", c.current, c.signal, c.out, got)
		}
	}
}

func TestStabilized(t *testing.T) {
	now := time.Now()
	hist := []histEntry{{now.Add(-400 * time.Second), 6}, {now.Add(-100 * time.Second), 5}, {now.Add(-15 * time.Second), 3}}
	if got := stabilized(6, 2, hist, now, 300*time.Second); got != 5 {
		t.Errorf("缩容应取窗口内的最大值 5（400 s 前的 6 已过期），得到 %d", got)
	}
	if got := stabilized(2, 4, hist, now, 300*time.Second); got != 4 {
		t.Errorf("扩容不等稳定窗口，得到 %d", got)
	}
	if got := stabilized(4, 2, nil, now, 300*time.Second); got != 2 {
		t.Errorf("没有历史时直接缩，得到 %d", got)
	}
}

func TestSchedule(t *testing.T) {
	sched, err := parseSchedule("+400s=2, +20s=4")
	if err != nil {
		t.Fatal(err)
	}
	if len(sched) != 2 || sched[0].at != 20*time.Second || sched[0].min != 4 {
		t.Fatalf("应按时刻排序：%+v", sched)
	}
	for _, c := range []struct {
		since   time.Duration
		running bool
		want    int
	}{{10 * time.Second, true, 2}, {30 * time.Second, true, 4}, {500 * time.Second, true, 2}, {30 * time.Second, false, 2}} {
		if got := minAt(2, sched, c.since, c.running); got != c.want {
			t.Errorf("负载开始后 %v（running=%v）：最少数应为 %d，得到 %d", c.since, c.running, c.want, got)
		}
	}
	for _, bad := range []string{"20s", "+20s=0", "+x=3", "=3"} {
		if _, err := parseSchedule(bad); err == nil {
			t.Errorf("%q 应当报错", bad)
		}
	}
}

func TestParseUnitShowAndProm(t *testing.T) {
	st := parseUnitShow("ActiveState=failed\nResult=oom-kill\nExecMainStatus=9\nNRestarts=3\nLoadState=loaded\n")
	if st.active != "failed" || st.result != "oom-kill" || st.status != "9" || st.restarts != 3 {
		t.Errorf("parseUnitShow = %+v", st)
	}
	if st := parseUnitShow("ActiveState=inactive\nLoadState=not-found\n"); st.active != "" {
		t.Errorf("不存在的服务应返回空状态：%+v", st)
	}
	v, err := parsePromValue("vector", json.RawMessage(`[{"metric":{},"value":[1700000000.1,"0.84"]}]`))
	if err != nil || v != 0.84 {
		t.Errorf("vector：%v %v", v, err)
	}
	if v, err := parsePromValue("scalar", json.RawMessage(`[1700000000.1,"2"]`)); err != nil || v != 2 {
		t.Errorf("scalar：%v %v", v, err)
	}
	if _, err := parsePromValue("vector", json.RawMessage(`[{"value":[1,"1"]},{"value":[1,"2"]}]`)); err == nil {
		t.Error("多条序列应当报错")
	}
	if _, err := parsePromValue("vector", json.RawMessage(`[]`)); err == nil {
		t.Error("没有序列应当报错")
	}
}
