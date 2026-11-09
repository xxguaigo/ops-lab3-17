package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSummarize(t *testing.T) {
	dir := t.TempDir()
	write := func(name, s string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// 负载从 Unix 1000 秒开始，共 6 秒；第 2、3 秒出错，第 4 秒 p99 300 ms
	write("timeline.csv", "sec,unix,sent,ok,errors,slow,p50_ms,p99_ms,max_ms\n"+
		"0,1000,10,10,0,0,5,10,12\n1,1001,10,10,0,0,5,10,12\n2,1002,10,7,3,0,5,20,30\n"+
		"3,1003,10,8,2,0,5,20,30\n4,1004,10,10,0,2,5,300,400\n5,1005,10,10,0,0,5,10,12\n")
	// 一个早已存在的实例，一个第 1 秒创建、第 1.5 秒启动、第 4 秒就绪的实例
	write("instances.csv", "unix_ms,time,t_run,instance,version,event,detail\n"+
		"900000,,,127.0.0.1:8101,v1,created,\n901000,,,127.0.0.1:8101,v1,ready,\n"+
		"1001000,,,127.0.0.1:8102,v1,created,扩容\n1001500,,,127.0.0.1:8102,v1,started,\n1004000,,,127.0.0.1:8102,v1,ready,\n")
	write("marks.csv", "unix_ms,time,action,target\n1002000,,kill,127.0.0.1:8101\n")
	write("lbd-events.csv", "unix_ms,time,backend,event,detail\n1003500,,127.0.0.1:8101,down,x\n1005000,,127.0.0.1:8101,up,y\n")
	var b strings.Builder
	if err := summarize(&b, dir, 0.2); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"| 2 | 5 | 3 | 30 | 5 | 2 |", // 第 2～4 秒连成一段缺口
		"缺口共 3 秒",
		"实例·分钟（从创建算起，只计负载期间）：0.2", // 6 秒 + 5 秒 = 11 秒 ≈ 0.18 分钟
		"| 127.0.0.1:8102 | v1 | 1.0 | 1.5 | 4.0 | - | 0.5 | 2.5 | 3.0 | 0 |",
		"| 2.0 | kill | 127.0.0.1:8101 | down 127.0.0.1:8101 | 1.5 | up 127.0.0.1:8101 | 3.0 | 5 |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("输出缺少 %q：\n%s", want, out)
		}
	}
}

// marks.csv 跨多次运行累积：负载开始以前的注入不列出；对象为 * 时按台数统计；resume 只列时刻。
func TestMarksWindowAndAll(t *testing.T) {
	dir := t.TempDir()
	write := func(name, s string) {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(s), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("timeline.csv", "sec,unix,sent,ok,errors,slow,p50_ms,p99_ms,max_ms\n"+
		"0,1000,10,10,0,0,5,10,12\n1,1001,10,6,4,0,5,10,12\n2,1002,10,0,10,0,,,\n3,1003,10,10,0,0,5,10,12\n")
	write("instances.csv", "unix_ms,time,t_run,instance,version,event,detail\n"+
		"900000,,,127.0.0.1:8101,v1,created,\n950000,,,127.0.0.1:8101,v1,restarted,\n1002500,,,127.0.0.1:8101,v1,restarted,\n")
	write("marks.csv", "unix_ms,time,action,target\n500000,,slow,127.0.0.1:8102\n"+
		"1001000,,pause,*\n1002000,,resume,*\n")
	write("lbd-events.csv", "unix_ms,time,backend,event,detail\n500100,,127.0.0.1:8102,down,old\n"+
		"1001500,,127.0.0.1:8101,down,x\n1001600,,127.0.0.1:8102,down,x\n1002800,,127.0.0.1:8101,up,y\n1003200,,127.0.0.1:8102,up,y\n")
	var b strings.Builder
	if err := summarize(&b, dir, 0.2); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{
		"| 127.0.0.1:8101 | v1 | -100.0 | - | - | - | - | - | - | 1 |", // 负载开始以前的重启不计
		"| 1.0 | pause | * | down 等 2 台 | 0.5 | 2 台都恢复 | 2.2 | 4 |",
		"| 2.0 | resume | * | - | - | - | - | - |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("输出缺少 %q：\n%s", want, out)
		}
	}
	if strings.Contains(out, "slow") {
		t.Errorf("负载开始以前的注入不应列出：\n%s", out)
	}
}
