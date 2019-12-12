package main

import "github.com/mapleFU/dm-chaos/checker"

func main()  {
	checker.CheckDM([]string{"maplewish.cn", "49.235.195.198"},
	"127.0.0.1", "127.0.0.1", 8261, "bin/go-ycsb", "bin/sync_diff_inspector")
}