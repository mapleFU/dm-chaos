// This file is paste from `base_task.go` in sc-test/dm.

package checker

import (
	"database/sql"
	"fmt"

	"github.com/juju/errors"
	"github.com/mapleFU/dm-chaos/clients"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)


func writeTaskConfigFile(cfg string, filename string) error {
	content := []byte(cfg)
	err := ioutil.WriteFile(filename, content, 0644)
	return errors.Trace(err)
}

func getSubTaskTables(caseID, instID int) []string {
	//if isNonShardingCase(caseID) {
	//if instID == 1 {
	//	return instanceTables[1]
	//}
	return []string{fmt.Sprintf("tf_%d", instID), fmt.Sprintf("test")}
	//}
	//caseTurn := caseID%2 + 1
	//idx := (instID-1)%len(instanceTablesFix[caseTurn]) + 1
	//return instanceTablesFix[caseTurn][idx]
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
			log.Infof("creating %v in db %v", t, idx)
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
