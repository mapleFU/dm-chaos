package clients

import (
	"database/sql"
	"fmt"

	log "github.com/sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
)

// This client describe the control to database.
// Currently it's just a wrapper.
type DbClient struct {
	*sql.DB
}

var shardingInitialTable = "CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL AUTO_INCREMENT,update_time " +
	"TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, primary key(id)) ENGINE=InnoDB " +
	"DEFAULT CHARSET=utf8 COLLATE=utf8_bin;"

func OpenDB(dbConn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dbConn)

	if err != nil {
		panic(err)
	}

	return db, err
}

func CreateNewTable(db *sql.DB, schema string, table string) error {
	// create database firstly if not exist
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", schema)
	err := execSQL(db, sql)
	if err != nil {
		return errors.Trace(err)
	}

	tn := tableName(schema, table)
	sql = fmt.Sprintf("DROP TABLE IF EXISTS %s;", tn)
	err = execSQL(db, sql)
	if err != nil {
		return errors.Trace(err)
	}

	// create initial table
	createTableStat := fmt.Sprintf(shardingInitialTable, tn)
	return errors.Trace(execSQL(db, createTableStat))
}

func execSQL(db *sql.DB, sql string) error {
	if len(sql) == 0 {
		return nil
	}

	log.Debugf("execute sql %s", sql)
	_, err := db.Exec(sql)
	return errors.Trace(err)
}

func tableName(db, table string) string {
	return fmt.Sprintf("`%s`.`%s`", db, table)
}
