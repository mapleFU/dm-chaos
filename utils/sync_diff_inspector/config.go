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
	"database/sql"
	"flag"
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/schrodinger-test/pkg/dbutil"
)

const (
	percent0   = 0
	percent100 = 100
)

// DBConfig is the config of database, and keep the connection.
type DBConfig struct {
	dbutil.DBConfig

	Label string `toml:"label" json:"label"`

	Snapshot string `toml:"snapshot" json:"snapshot"`

	Conn *sql.DB
}

// CheckTables saves the tables need to check.
type CheckTables struct {
	// schema name
	Schema string `toml:"schema" json:"schema"`

	// table list
	Tables []string `toml:"tables" json:"tables"`
}

// TableConfig is the config of table.
type TableConfig struct {
	// table's origin information
	TableInstance

	// field should be the primary key, unique key or field with index
	Field string `toml:"index-field"`
	// select range, for example: "age > 10 AND age < 20"
	Range string `toml:"range"`
	// set true if comparing sharding tables with target table, should have more than one source tables.
	IsSharding bool `toml:"is-sharding"`
	// saves the source tables's info.
	// may have more than one source for sharding tables.
	// or you want to compare table with different schema and table name.
	// SourceTables can be nil when source and target is one-to-one correspondence.
	SourceTables []TableInstance `toml:"source-table"`
	Info         *model.TableInfo
}

// TableInstance saves the base information of table.
type TableInstance struct {
	// database's label
	DBLabel string `toml:"label"`
	// schema name
	Schema string `toml:"schema"`
	// table name
	Table string `toml:"table"`
}

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	// log level
	LogLevel string `toml:"log-level" json:"log-level"`

	// source database's config
	SourceDBCfg []DBConfig `toml:"source-db" json:"source-db"`

	// target database's config
	TargetDBCfg DBConfig `toml:"target-db" json:"target-db"`

	// for example, the whole data is [1...100]
	// we can split these data to [1...10], [11...20], ..., [91...100]
	// the [1...10] is a chunk, and it's chunk size is 10
	// size of the split chunk
	ChunkSize int `toml:"chunk-size" json:"chunk-size"`

	// sampling check percent, for example 10 means only check 10% data
	Sample int `toml:"sample-percent" json:"sample-percent"`

	// how many goroutines are created to check data
	CheckThreadCount int `toml:"check-thread-count" json:"check-thread-count"`

	// set true if target-db and source-db all support tidb implicit column "_tidb_rowid"
	UseRowID bool `toml:"use-rowid" json:"use-rowid"`

	// set false if want to comapre the data directly
	UseChecksum bool `toml:"use-checksum" json:"use-checksum"`

	// is sharding tables or not
	//IsSharding bool `toml:"is-sharding" json:"is-sharding"`

	// the name of the file which saves sqls used to fix different data
	FixSQLFile string `toml:"fix-sql-file" json:"fix-sql-file"`

	// the tables to be checked
	Tables []*CheckTables `toml:"check-table" json:"check-table"`

	// the config of table
	TableCfgs []*TableConfig `toml:"table-config" json:"table-config"`
}

// NewConfig creates a new config.
func NewConfig(sourceDB []DBConfig, tagertDB DBConfig, tables []*CheckTables) *Config {
	cfg := defaultConfig()
	cfg.Tables = tables
	cfg.SourceDBCfg = sourceDB
	cfg.TargetDBCfg = tagertDB
	return cfg
}

// DefaultConfig creates default config
func defaultConfig() *Config {
	return &Config{
		LogLevel:         "debug",
		ChunkSize:        1000,
		Sample:           100,
		CheckThreadCount: 1,
		UseRowID:         false,
		UseChecksum:      true,
		FixSQLFile:       "fix.sql",
	}
}

func (c *Config) String() string {
	if c == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Config(%+v)", *c)
}

func (c *Config) checkConfig() bool {
	if c.Sample > percent100 || c.Sample < percent0 {
		log.Errorf("sample must be greater than 0 and less than or equal to 100!")
		return false
	}

	if c.CheckThreadCount <= 0 {
		log.Errorf("check-thcount must greater than 0!")
		return false
	}

	if len(c.SourceDBCfg) == 0 {
		log.Error("must have at least one source database")
		return false
	}

	for i := range c.SourceDBCfg {
		if c.SourceDBCfg[i].Label == "" {
			log.Error("must specify source database's label")
			return false
		}
	}

	if len(c.Tables) == 0 {
		log.Error("must specify check tables")
		return false
	}

	for _, tableCfg := range c.TableCfgs {
		if tableCfg.Schema == "" || tableCfg.Table == "" {
			log.Error("schema and table's name can't be empty")
			return false
		}

		if tableCfg.IsSharding {
			if len(tableCfg.SourceTables) <= 1 {
				log.Error("must have more than one source tables if comparing sharding tables")
				return false
			}

		} else {
			if len(tableCfg.SourceTables) > 1 {
				log.Error("have more than one source table in no sharding mode")
				return false
			}
		}

		for _, sourceTable := range tableCfg.SourceTables {
			if sourceTable.DBLabel == "" {
				log.Error("must specify the database label for source table")
				return false
			}

			if sourceTable.Schema == "" || sourceTable.Table == "" {
				log.Error("schema and table's name can't be empty")
				return false
			}
		}
	}

	return true
}
