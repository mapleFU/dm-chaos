package main

import "fmt"

/**
 * This file is for some set up scripts, which will be used in
 */

func setMaxBinlogSize()  {
	DefaultInitSqls = append(DefaultInitSqls, fmt.Sprintf("SET @@GLOBAL.MAX_BINLOG_SIZE = %d;", maxBinlogSize))
}