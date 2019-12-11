// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package check

import (
	"context"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

// CheckConfig is check config for two source
type CheckConfig struct {
	Source   string
	Target   string
	Database string
	Tables   []string
}

// SimpleCheckTidbAndMysql checks tidb with mysql
func SimpleCheckTidbAndMysql(tidb string, mysql string, schema string, tables []string) error {
	source := DBConfig{
		Label: "databases",
	}
	source.Host = tidb
	source.Port = 4000
	source.User = "root"
	source.Password = ""

	var sources []DBConfig
	sources = append(sources, source)

	target := DBConfig{
		Label: "databases",
	}

	target.Host = mysql
	target.Port = 3306
	target.User = "root"
	target.Password = ""

	var ts []*CheckTables
	t := &CheckTables{
		Schema: schema,
		Tables: tables,
	}
	ts = append(ts, t)

	cfg := NewConfig(sources, target, ts)
	return errors.Trace(checkSyncState(context.Background(), cfg))
}

func checkSyncState(ctx context.Context, cfg *Config) error {
	beginTime := time.Now()
	defer func() {
		log.Infof("check data finished, all cost %v", time.Since(beginTime))
	}()

	d, err := NewDiff(ctx, cfg)
	if err != nil {
		return errors.Errorf("fail to initialize diff process %v", errors.ErrorStack(err))
	}

	err = d.Equal()
	if err != nil {
		return errors.Errorf("check data difference error %v", errors.ErrorStack(err))
	}

	log.Info(d.report)

	if d.report.Result != Pass {
		return errors.Errorf("check not pass %+v", d.report)
	}
	return nil
}
