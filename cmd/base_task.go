// This file is paste from `base_task.go` in sc-test/dm.

package main

import (
	"database/sql"
	"fmt"
	"github.com/mapleFU/dm-chaos/clients"
	"io/ioutil"
	"strings"

	"github.com/juju/errors"
)


// TODO: use template to make parameter more clear
// This cfg is using to drive checker.
var baseTaskCfg = `
name: {{.TaskName}}
task-mode: {{.TaskMode}}
meta-schema: dm_meta
disable-heartbeat: true
online-ddl-scheme: {{.OnlineDDLScheme}}
remove-meta: {{.RemoveMeta}}
target-database:
  host: {{.Host}}
  port: 4000
  user: root
  password: ""
mysql-instances:
{{.MySQLInstances}}
is-sharding: true
filters:
  user-filter-1:
    schema-pattern: "test_*"
    table-pattern: "tf_*"
    events: ["delete"]
    action: Ignore
routes:
  sharding-route-rules-table:
    schema-pattern: test_*
    table-pattern: t_*
    target-schema: test_target_{{.SchemaIndex}}
    target-table: t_target
  sharding-route-rules-schema:
    schema-pattern: test_*
    target-schema: test_target_{{.SchemaIndex}}
column-mappings:
{{.ColumnMappingRules}}
black-white-list:
  instance:
    do-dbs: ["test_{{.SchemaIndex}}", "do"]
    do-tables:
    - db-name: "test_{{.SchemaIndex}}"
      tbl-name: "~^t_[1-5]"
    - db-name: "test_{{.SchemaIndex}}"
      tbl-name: "~^tf_.*"
`

// each test case shares same column mapping now, but they use different schemas.
// instance-<instanceID>-<ruleID>
var columnMappingTpl = `
  instance-{{.InstanceID}}-{{.RuleID1}}:
    schema-pattern: "test_*"
    table-pattern: "t_*"
    expression: "partition id"
    source-column: "id"
    target-column: "id"
    arguments: ["{{.InstanceID}}", "test_", "t_"]
  instance-{{.InstanceID}}-{{.RuleID2}}:
    schema-pattern: "test_*"
    table-pattern: "tf_*"
    expression: "partition id"
    source-column: "id"
    target-column: "id"
    arguments: ["{{.InstanceID}}", "test_", "tf_"]`

var subTaskCfg = `
- source-id: {{.SourceHost}}:3306
  meta: null
  filter-rules: ["user-filter-1"]
  column-mapping-rules: [{{.ColumnMappingRules}}]
  route-rules: ["sharding-route-rules-table","sharding-route-rules-schema"]
  black-white-list: "instance"
  mydumper:
    mydumper-path: /dm-worker/bin/mydumper
    threads: 4
    chunk-filesize: 8
    skip-tz-utc: true
    extra-args: "-B test_{{.SchemaIndex}}"
  loader:
    pool-size: 32
    dir: ./dumped_data{{.SchemaIndex}}
  syncer:
    meta-file: ""
    worker-count: 32
    batch: 2000
    max-retry: 200
    disable-detect: false
    safe-mode: false
`

var baseTaskCfgNonSharding = `
name: {{.TaskName}}
task-mode: {{.TaskMode}}
meta-schema: dm_meta
disable-heartbeat: true
online-ddl-scheme: {{.OnlineDDLScheme}}
remove-meta: {{.RemoveMeta}}
target-database:
  host: {{.Host}}
  port: 4000
  user: root
  password: ""
mysql-instances:
{{.MySQLInstances}}
is-sharding: false
filters:
  user-filter-1:
    schema-pattern: "test_*"
    table-pattern: "tf_*"
    events: ["delete"]
    action: Ignore
routes:
  nonsharding-route-rules-schema:
    schema-pattern: test_*
    target-schema: test_target_{{.SchemaIndex}}
black-white-list:
  instance:
    do-dbs: ["test_{{.SchemaIndex}}"]
    do-tables:
    - db-name: "test_{{.SchemaIndex}}"
      tbl-name: "~^t_[1-5]"
  instance2:
    do-dbs: ["test_{{.SchemaIndex}}"]
    do-tables:
    - db-name: "test_{{.SchemaIndex}}"
      tbl-name: "~^tf_.*"
`

var subTaskCfgNonSharding = `
- source-id: {{.SourceHost}}:3306
  meta: null
  filter-rules: ["user-filter-1"]
  route-rules: ["nonsharding-route-rules-schema"]
  black-white-list: "{{.BlackWhiteList}}"
  mydumper:
    mydumper-path: /dm-worker/bin/mydumper
    threads: 4
    chunk-filesize: 8
    skip-tz-utc: true
    extra-args: "-B test_{{.SchemaIndex}}"
  loader:
    pool-size: 32
    dir: ./dumped_data{{.SchemaIndex}}
  syncer:
    meta-file: ""
    worker-count: 32
    batch: 2000
    max-retry: 200
    disable-detect: false
    safe-mode: false
`

var (
	t1       = "t_1"
	t2       = "t_2"
	t3       = "t_3"
	t4       = "t_4"
	t5       = "t_5"
	tIgn1    = "t_ignore_1"
	tIgn2    = "t_ignore_2"
	tIgn3    = "t_ignore_3"
	tFilter1 = "tf_1"
	dbName   = "test_1"

	// used in non-sharding auto skip sql pattern
	totalSkipTables1 = []string{t1, t2, t3, t4, t5}

	filterTables = []string{tFilter1}

	// non sharding cases
	instanceTables = map[int][]string{
		1: []string{t1, t2, t3, t4, tIgn1, tFilter1},
	}

	// sharding cases table 1
	instanceTablesSharding1 = map[int][]string{
		1: []string{t1, t2, t3, t4, tIgn1},
		2: []string{t1, t2, t3, t4, t5, tIgn2},
		3: []string{t1, t5, tIgn3},
	}

	// sharding cases table 2
	instanceTablesSharding2 = map[int][]string{
		1: []string{tIgn1, tFilter1},
		2: []string{tIgn2, tFilter1},
		3: []string{tIgn3, tFilter1},
	}

	// NOTE: currently multiple sharding DDL in same tasks may have problem
	// we separate sharding groups into different tasks
	instanceTablesFix = map[int]map[int][]string{
		0: instanceTables,
		1: instanceTablesSharding1,
		2: instanceTablesSharding2,
	}

	FilterTablePrefix = "tf"
)

func writeTaskConfigFile(cfg string, filename string) error {
	content := []byte(cfg)
	err := ioutil.WriteFile(filename, content, 0644)
	return errors.Trace(err)
}

func getSubTaskTables(caseID, instID int) []string {
	if isNonShardingCase(caseID) {
		if instID == 1 {
			return instanceTables[1]
		}
		return []string{fmt.Sprintf("tf_%d", instID)}
	}
	caseTurn := caseID%2 + 1
	idx := (instID-1)%len(instanceTablesFix[caseTurn]) + 1
	return instanceTablesFix[caseTurn][idx]
}

func tryEnableGTID(dbs ...*sql.DB) error {
	// NOTE: change enforce_gtid_consistency value online is supported by MySQL >= 5.7.6
	commands := []string{
		"SET @@GLOBAL.SQL_MODE = '';",
		"SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = WARN;",
		"SET @@GLOBAL.ENFORCE_GTID_CONSISTENCY = ON;",
		"SET @@GLOBAL.GTID_MODE = OFF_PERMISSIVE;",
		"SET @@GLOBAL.GTID_MODE = ON_PERMISSIVE;",
		"SET @@GLOBAL.GTID_MODE = ON;",
	}

	for _, cmd := range commands {
		for dbID, db := range dbs {
			if _, err := db.Exec(cmd); err != nil {
				return errors.Annotatef(err, "failed to execute %s on db #%d", cmd, dbID)
			}
		}
	}
	return nil
}

func initialTaskTables(caseID int, mysqls []*sql.DB, schema string) error {
	for idx, db := range mysqls {
		instID := idx + 1
		for _, t := range getSubTaskTables(caseID, instID) {
			err := clients.CreateNewTable(db, schema, t)
			if err != nil {
				return errors.Trace(err)
			}
		}
		sqls := DefaultInitSqls[:]
		sqls = append([]string{fmt.Sprintf("USE %s;", schema)}, sqls...)
		for _, stmt := range sqls {
			if _, err := db.Exec(stmt); err != nil {
				return errors.Trace(err)
			}
		}
	}

	return nil
}

func filterIgnoreTables(tbls []string) []string {
	result := make([]string, 0)
	for _, tbl := range tbls {
		if !strings.HasPrefix(tbl, "t_ignore") && !strings.HasPrefix(tbl, FilterTablePrefix) {
			result = append(result, tbl)
		}
	}
	return result
}

func filterFilterTables(tbls []string) []string {
	result := make([]string, 0)
	for _, tbl := range tbls {
		if strings.HasPrefix(tbl, FilterTablePrefix) {
			result = append(result, tbl)
		}
	}
	return result
}

func initialMySQLDBs(mysqls []string, schema string) ([]*sql.DB, error) {
	var err error
	dbs := make([]*sql.DB, len(mysqls))

	defer func() {
		if err != nil {
			for _, db := range dbs {
				if db != nil {
					db.Close()
				}
			}
		}
	}()

	for idx, mysql := range mysqls {

		dbs[idx], err = clients.OpenDB(fmt.Sprintf("root:@tcp(%s:3306)/", mysql))
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	if schema != "" {
		for _, db := range dbs {
			stmt := fmt.Sprintf("USE %s;", schema)
			_, err := db.Exec(stmt)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}

	return dbs, nil
}

func genTaskName(caseID int) string {
	return fmt.Sprintf("test-%d", caseID)
}

func isNonShardingCase(caseID int) bool {
	return caseID == 1
}

