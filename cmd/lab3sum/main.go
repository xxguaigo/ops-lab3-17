// lab3sum 汇总 scripts/run3.sh 产出的一次实验（runs/<名字>/），输出：
//
//  1. 缺口：逐秒看 timeline.csv，出现错误、或成功请求的 p99 超过 -slo 的秒，连成区间；
//  2. 实例·分钟：按 instances.csv，每个实例从“创建”（开始拉镜像）算到“停止”或“死掉”，只计负载期间；
//  3. 新实例的各段耗时：创建 → 启动（拉镜像）→ 就绪（启动、预热、健康检查），以及负载期间被 systemd 重启的次数；
//  4. 伸缩决策：decisions.csv 中有动作的那几次；
//  5. 注入：marks.csv 中每次注入之后，lbd 多久判定（down、ejected、removed）、多久恢复（up、restored），其间出错的请求数。
//
// 时间都换算成“负载开始后的秒数”（timeline.csv 的 sec）。原始文件都留在目录里，本工具只是帮你们整理，
// 结论以原始数据为准。
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

type second struct {
	unix                 int64
	sent, ok, errs, slow int
	p99                  float64
	hasP99               bool
}

type event struct {
	ms                 int64
	who, ev, detail, v string
}

func main() {
	dir := flag.String("dir", "", "一次实验的目录，例如 runs/b2-cpu")
	slo := flag.Float64("slo", 0.2, "判断缺口用的延迟阈值（秒）：某秒成功请求的 p99 超过它，或者出现错误，就算缺口")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "用法：lab3sum -dir runs/<名字> [-slo 0.2]")
		os.Exit(2)
	}
	if err := summarize(os.Stdout, *dir, *slo); err != nil {
		fmt.Fprintln(os.Stderr, "lab3sum：", err)
		os.Exit(1)
	}
}

func summarize(w io.Writer, dir string, slo float64) error {
	secs, err := readTimeline(filepath.Join(dir, "timeline.csv"))
	if err != nil {
		return err
	}
	if len(secs) == 0 {
		return fmt.Errorf("timeline.csv 没有数据")
	}
	t0 := secs[0].unix * 1000
	t1 := (secs[len(secs)-1].unix + 1) * 1000
	rel := func(ms int64) float64 { return float64(ms-t0) / 1000 }

	fmt.Fprintf(w, "# %s\n\n负载共 %d 秒；时间都是负载开始后的秒数。\n\n", filepath.Base(dir), len(secs))

	// 1 缺口
	fmt.Fprintf(w, "## 缺口（出错，或 p99 > %g ms 的秒）\n\n", slo*1000)
	type gap struct{ from, to, errs, slow, sent int }
	var gaps []gap
	totalErr, totalSlow, totalSent := 0, 0, 0
	for i, s := range secs {
		totalErr += s.errs
		totalSlow += s.slow
		totalSent += s.sent
		bad := s.errs > 0 || (s.hasP99 && s.p99 > slo*1000)
		if !bad {
			continue
		}
		if n := len(gaps); n > 0 && gaps[n-1].to == i {
			gaps[n-1].to = i + 1
			gaps[n-1].errs += s.errs
			gaps[n-1].slow += s.slow
			gaps[n-1].sent += s.sent
		} else {
			gaps = append(gaps, gap{i, i + 1, s.errs, s.slow, s.sent})
		}
	}
	if len(gaps) == 0 {
		fmt.Fprintln(w, "没有缺口。")
	} else {
		fmt.Fprintln(w, "| 起 | 止 | 秒数 | 发出 | 错误 | 慢（成功但超过阈值） |\n|---|---|---|---|---|---|")
		bad := 0
		for _, g := range gaps {
			fmt.Fprintf(w, "| %d | %d | %d | %d | %d | %d |\n", g.from, g.to, g.to-g.from, g.sent, g.errs, g.slow)
			bad += g.to - g.from
		}
		fmt.Fprintf(w, "\n缺口共 %d 秒；第一段从第 %d 秒到第 %d 秒。\n", bad, gaps[0].from, gaps[0].to)
	}
	fmt.Fprintf(w, "全程发出 %d 个请求，错误 %d 个（%.2f%%），成功但超过阈值 %d 个（%.2f%%）。\n\n", totalSent, totalErr,
		pct(totalErr, totalSent), totalSlow, pct(totalSlow, totalSent))

	// 2、3 实例
	inst, _ := readEvents(filepath.Join(dir, "instances.csv"), 3, 5, 6, 4) // unix_ms,time,t_run,instance,version,event,detail
	if len(inst) > 0 {
		writeInstances(w, inst, t0, t1, rel)
	}

	// 4 决策
	if dec, err := readCSV(filepath.Join(dir, "decisions.csv")); err == nil && len(dec) > 1 {
		fmt.Fprintln(w, "## 伸缩决策（有动作的）\n\n| 时刻 | 信号 | 当前 | 就绪 | 最少 | 计算 | 期望 | 动作 |\n|---|---|---|---|---|---|---|---|")
		for _, r := range dec[1:] {
			ms, _ := strconv.ParseInt(r[0], 10, 64)
			if ms < t0-60000 || ms > t1 || r[10] == "none" {
				continue
			}
			fmt.Fprintf(w, "| %.0f | %s | %s | %s | %s | %s | %s | %s |\n", rel(ms), dash(r[3]), r[4], r[5], r[6], r[8], r[9], r[10])
		}
		fmt.Fprintln(w)
	}

	// 5 注入
	marks, _ := readEvents(filepath.Join(dir, "marks.csv"), -1, 2, 3, -1)    // unix_ms,time,action,target
	lbEv, _ := readEvents(filepath.Join(dir, "lbd-events.csv"), 2, 3, 4, -1) // unix_ms,time,backend,event,detail
	writeMarks(w, marks, lbEv, secs, t0, t1, rel)
	return nil
}

func writeInstances(w io.Writer, inst []event, t0, t1 int64, rel func(int64) float64) {
	type life struct {
		who, v                       string
		created, started, ready, end int64
		restarts                     int
	}
	var lives []*life
	open := map[string]*life{}
	for _, e := range inst {
		switch e.ev {
		case "created":
			l := &life{who: e.who, v: e.v, created: e.ms}
			lives = append(lives, l)
			open[e.who] = l
		case "started":
			if l := open[e.who]; l != nil && l.started == 0 {
				l.started = e.ms
			}
		case "ready":
			if l := open[e.who]; l != nil && l.ready == 0 {
				l.ready = e.ms
			}
		case "restarted":
			if l := open[e.who]; l != nil && e.ms >= t0 && e.ms < t1 {
				l.restarts++
			}
		case "stopped", "died", "failed":
			if l := open[e.who]; l != nil {
				l.end = e.ms
				delete(open, e.who)
			}
		}
	}
	var minutes float64
	peak := 0
	for ms := t0; ms < t1; ms += 1000 {
		n := 0
		for _, l := range lives {
			if l.created <= ms && (l.end == 0 || l.end > ms) {
				n++
			}
		}
		peak = max(peak, n)
	}
	for _, l := range lives {
		a, b := max(l.created, t0), t1
		if l.end > 0 {
			b = min(l.end, t1)
		}
		if b > a {
			minutes += float64(b-a) / 60000
		}
	}
	fmt.Fprintf(w, "## 实例\n\n实例·分钟（从创建算起，只计负载期间）：%.1f；同时存在的实例最多 %d 个。\n\n", minutes, peak)
	fmt.Fprintln(w, "| 实例 | 版本 | 创建 | 启动 | 就绪 | 结束 | 拉镜像 s | 启动到就绪 s | 创建到就绪 s | 重启 |\n|---|---|---|---|---|---|---|---|---|---|")
	for _, l := range lives {
		if l.end != 0 && l.end < t0-60000 || l.created > t1 {
			continue
		}
		at := func(ms int64) string {
			if ms == 0 {
				return "-"
			}
			return strconv.FormatFloat(rel(ms), 'f', 1, 64)
		}
		span := func(a, b int64) string {
			if a == 0 || b == 0 {
				return "-"
			}
			return strconv.FormatFloat(float64(b-a)/1000, 'f', 1, 64)
		}
		fmt.Fprintf(w, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %d |\n", l.who, l.v, at(l.created), at(l.started), at(l.ready), at(l.end),
			span(l.created, l.started), span(l.started, l.ready), span(l.created, l.ready), l.restarts)
	}
	fmt.Fprintln(w)
}

// writeMarks 只看负载期间的注入（marks.csv 跨多次运行累积）。每次注入之后、下一次注入之前，找 lbd 的判定与恢复：
// 判定是 down（健康检查）、ejected（离群摘除）、removed（scaler 通过管理接口删除）；恢复是同一台的 up 或 restored。
// 对象为 * 时统计被判定的台数，恢复取最后一台恢复的时刻。resume、restore 是撤销注入，只列出时刻。
func writeMarks(w io.Writer, marks, lbEv []event, secs []second, t0, t1 int64, rel func(int64) float64) {
	var in []event
	for _, m := range marks {
		if m.ms >= t0-5000 && m.ms < t1 {
			in = append(in, m)
		}
	}
	if len(in) == 0 {
		return
	}
	fmt.Fprintln(w, "## 注入\n\n| 时刻 | 注入 | 对象 | lbd 判定 | 判定用时 s | 恢复 | 恢复用时 s | 注入到判定之间的错误 |\n|---|---|---|---|---|---|---|---|")
	for k, m := range in {
		if m.ev == "resume" || m.ev == "restore" || m.ev == "mark" {
			fmt.Fprintf(w, "| %.1f | %s | %s | - | - | - | - | - |\n", rel(m.ms), m.ev, m.detail)
			continue
		}
		until := t1 + 60000
		for _, n := range in[k+1:] {
			if n.ev != "resume" && n.ev != "restore" && n.ev != "mark" {
				until = n.ms
				break
			}
		}
		var det *event
		judged := map[string]bool{} // 被判定过的实例
		back := map[string]*event{} // 每台此后第一次恢复
		times := 0
		for i := range lbEv {
			e := &lbEv[i]
			if e.ms < m.ms || e.ms >= until || (m.detail != "*" && e.who != m.detail) {
				continue
			}
			switch e.ev {
			case "down", "ejected", "removed":
				times++
				if det == nil {
					det = e
				}
				judged[e.who] = true
			case "up", "restored":
				if judged[e.who] && back[e.who] == nil {
					back[e.who] = e
				}
			}
		}
		detS, detT, recS, recT, errs := "-", "-", "-", "-", 0
		end := m.ms + 30000
		if det != nil {
			detS, detT = det.ev+" "+det.who, strconv.FormatFloat(float64(det.ms-m.ms)/1000, 'f', 1, 64)
			if len(judged) > 1 {
				detS = fmt.Sprintf("%s 等 %d 台", det.ev, len(judged))
			}
			if times > len(judged) {
				detS += fmt.Sprintf("，共 %d 次", times)
			}
			end = det.ms
			var last *event
			for who := range judged {
				if back[who] == nil {
					last = nil
					break
				}
				if last == nil || back[who].ms > last.ms {
					last = back[who]
				}
			}
			if last != nil {
				recS, recT = last.ev+" "+last.who, strconv.FormatFloat(float64(last.ms-m.ms)/1000, 'f', 1, 64)
				if len(judged) > 1 {
					recS = fmt.Sprintf("%d 台都恢复", len(judged))
				}
			}
		}
		for _, s := range secs {
			if s.unix*1000 >= m.ms-999 && s.unix*1000 < end {
				errs += s.errs
			}
		}
		fmt.Fprintf(w, "| %.1f | %s | %s | %s | %s | %s | %s | %d |\n", rel(m.ms), m.ev, m.detail, detS, detT, recS, recT, errs)
	}
	fmt.Fprintln(w)
}

func pct(a, b int) float64 {
	if b == 0 {
		return math.NaN()
	}
	return 100 * float64(a) / float64(b)
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func readCSV(path string) ([][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	return r.ReadAll()
}

func readTimeline(path string) ([]second, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	var out []second
	for _, r := range rows[min(1, len(rows)):] {
		if len(r) < 9 {
			continue
		}
		s := second{}
		s.unix, _ = strconv.ParseInt(r[1], 10, 64)
		s.sent, _ = strconv.Atoi(r[2])
		s.ok, _ = strconv.Atoi(r[3])
		s.errs, _ = strconv.Atoi(r[4])
		s.slow, _ = strconv.Atoi(r[5])
		if v, err := strconv.ParseFloat(r[7], 64); err == nil {
			s.p99, s.hasP99 = v, true
		}
		out = append(out, s)
	}
	return out, nil
}

// readEvents 读 instances.csv、marks.csv、lbd-events.csv：第一列是 unix_ms；who、ev、detail、version 所在的列由参数给出（-1 表示没有）。
func readEvents(path string, who, ev, detail, version int) ([]event, error) {
	rows, err := readCSV(path)
	if err != nil {
		return nil, err
	}
	col := func(r []string, i int) string {
		if i < 0 || i >= len(r) {
			return ""
		}
		return r[i]
	}
	var out []event
	for _, r := range rows[min(1, len(rows)):] {
		ms, err := strconv.ParseInt(col(r, 0), 10, 64)
		if err != nil {
			continue
		}
		out = append(out, event{ms: ms, who: col(r, who), ev: col(r, ev), detail: col(r, detail), v: col(r, version)})
	}
	slices.SortStableFunc(out, func(a, b event) int { return int(a.ms - b.ms) })
	return out, nil
}
