package utils

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	tmysql "github.com/pingcap/parser/mysql"
)

// The method is unimplemented.
func UnImplemented()  {
	panic("implement me")
}

// IsErrTableNotExists checks whether err is TableNotExists error
func IsErrTableNotExists(err error) bool {
	return isMySQLError(err, tmysql.ErrNoSuchTable)
}

func isMySQLError(err error, code uint16) bool {
	err = originError(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

func getColumnIndexName(db *sql.DB, schema, table, column string) (string, error) {
	var indexName string
	stmt := "SELECT INDEX_NAME FROM information_schema.STATISTICS where TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?"
	err := db.QueryRow(stmt, schema, table, column).Scan(&indexName)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", errors.Trace(err)
	}
	return indexName, nil
}

// originError return original error
func originError(err error) error {
	for {
		e := errors.Cause(err)
		if e == err {
			break
		}
		err = e
	}
	return err
}
