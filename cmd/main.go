package main

import (
	"github.com/juju/errors"
	"github.com/mapleFU/dm-chaos/checker"
	"github.com/sirupsen/logrus"
)

func main()  {
	cfg := checker.NewPathConfig("workload", "task-template.yaml",
		"config-template.toml", "bin/go-ycsb", "bin/sync_diff_inspector")
	err := checker.CheckDM([]string{"maplewish.cn"},
	"127.0.0.1", "127.0.0.1", 8261, cfg)

	if err != nil {
		logrus.Info(errors.ErrorStack(err))
	}
}