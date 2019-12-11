package main

import (
	"database/sql"
	"fmt"

	"github.com/juju/errors"
	"github.com/mapleFU/dm-chaos/clients"
)

const (
	SourceDB string = "source_db"
	TargetDB string = "target_db"
)

var dbCnt int

func init()  {
	dbCnt = 0
}

/**
 * This file is for some set up scripts, which will be used in
 */

func setMaxBinlogSize()  {
	DefaultInitSqls = append(DefaultInitSqls, fmt.Sprintf("SET @@GLOBAL.MAX_BINLOG_SIZE = %d;", maxBinlogSize))
}


// initialize databases
func initialDBs(tidb string, mysqls []string) (tidbConn *sql.DB, mysqlDB []*sql.DB, err error) {
	mysqlDB = make([]*sql.DB, len(mysqls))
	tidbConn = new(sql.DB)

	defer func() {
		if err != nil {
			for _, db := range mysqlDB {
				if db != nil {
					db.Close()
				}
			}
		}
	}()

	tidbConn, err = clients.OpenDB(fmt.Sprintf("root:@tcp(%s:4000)/", tidb))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	// open MySQL Database
	for idx, mysql := range mysqls {
		mysqlDB[idx], err = clients.OpenDB(fmt.Sprintf("root:@tcp(%s:3306)/", mysql))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}

	// TODO: calling to initTaskTables is naive, please change it to a better solving.
	err = initialTaskTables(dbCnt, mysqlDB, SourceDB)
	dbCnt += 1
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	err = clients.CreateDB(tidbConn, TargetDB)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	_, err = tidbConn.Exec("SET @@GLOBAL.SQL_MODE = '';")
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	return tidbConn, mysqlDB, nil
}
