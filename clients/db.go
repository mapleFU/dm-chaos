package clients

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/mapleFU/dm-chaos/utils"
	log "github.com/sirupsen/logrus"
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

	log.Infof("execute sql %s\n", sql)
	_, err := db.Exec(sql)
	return errors.Trace(err)
}

func tableName(db, table string) string {
	return fmt.Sprintf("`%s`.`%s`", db, table)
}


func ShowCreateTable(db *sql.DB, schema, table string) (string, error) {
	var (
		name string
		desc string
	)

	tn := tableName(schema, table)
	sql := fmt.Sprintf("SHOW CREATE TABLE %s ;", tn)
	err := db.QueryRow(sql).Scan(&name, &desc)
	if err != nil {
		return "", errors.Trace(err)
	}

	return desc, nil
}

func CheckTableExists(db *sql.DB, schema, table string) (bool, error) {
	var version int64
	stmt := "SELECT version FROM `information_schema`.`tables` WHERE table_schema = ? and table_name = ? limit 1"
	err := db.QueryRow(stmt, schema, table).Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

// getSyncerCheckpoint returns binlog name, binlog position and error
func GetSyncerCheckpoint(db *sql.DB, id, taskName string) (string, uint32, error) {
	var (
		name string
		pos  uint32
	)
	stmt := fmt.Sprintf("SELECT binlog_name, binlog_pos from `dm_meta`.`%s_syncer_checkpoint` where id = ?", taskName)
	err := db.QueryRow(stmt, id).Scan(&name, &pos)
	if err != nil {
		// record not created
		if err == sql.ErrNoRows {
			return "", 0, nil
		}
		if utils.IsErrTableNotExists(err) {
			return "", 0, nil
		}
		return "", 0, errors.Trace(err)
	}
	return name, pos, nil
}

func CreateDB(db *sql.DB, dbName string) error {
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	return errors.Trace(execSQL(db, sql))
}