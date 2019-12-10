package main

import (
	"flag"
	"fmt"

	//"github.com/joho/godotenv"
	log "github.com/sirupsen/logrus"
)

// Config.toml in `sync_diff_inspector` need to know the config of database,
// so we may need to config here.
type DBConfig struct {
	host string
	port string

	user string
	pass string

	// TODO: make clear What the fuck is instanceId
	instanceId string
}

var (
	mysqlFlavor       string
	mysqlVersion      string

	// The address of mysql server.
	// The count was initialized as one, but we may add more.
	mysqlAddress string

	dmWorkerCount     int
	dmCaseCount       int

	downloadAddr      string
	ddlCount          int
	dmWorkerMem       int

	// The address of mysql server.
	// There should be one master and at least one worker.
	dmWorkerAddress   []string
	dmMasterAddress   string

	withNemesis       bool
	withCheck         bool
	taskMode          string
	onlineDDL         bool
	onlineDDLTool     string
	filterDDL         bool
	cmptDDL           bool
	masterSlaveSwitch bool
	failResume        bool
	enableGTID        bool
	maxBinlogSize     int64
)

var (
	AllMode  = "all"
	FullMode = "full"
)

func init() {
	flag.StringVar(&mysqlFlavor, "mysql-flavor", "mysql", "mysql flavor (mysql or mariadb)")
	flag.StringVar(&mysqlVersion, "mysql-version", "5.7", "mysql version")

	// mysql address


	flag.IntVar(&dmWorkerCount, "worker-count", 2, "init worker count")
	flag.IntVar(&dmCaseCount, "case-count", 2, "init case count")

	flag.IntVar(&ddlCount, "ddl-count", 2000, "how many ddl")
	flag.IntVar(&dmWorkerMem, "dm-worker-memory", 6, "dm-worker memory quota in GB")

	flag.StringVar(&downloadAddr, "download-addr", "http://download.pingcap.org/dm-latest-linux-amd64.tar.gz", "download addr")
	flag.StringVar(&taskMode, "task-mode", AllMode, "mode of test task,  full, all (= full + incremental)")
	flag.BoolVar(&onlineDDL, "online-ddl", false, "whether enable online ddl, if enabled, must use docker image: 127.0.0.1:5001/pingcap/ptagent:latest")
	flag.StringVar(&onlineDDLTool, "online-ddl-tool", "pt", "online ddl tool, pt(stand for pt-osc) or gh-ost(stand for gh-ost)")
	flag.BoolVar(&filterDDL, "filter-ddl", false, "whether run ddl in filter list")
	flag.BoolVar(&cmptDDL, "cmpt-ddl", false, "whether enable incompatible ddl")
	flag.BoolVar(&masterSlaveSwitch, "master-slave-switch", false, "whether test upstream master/slave switch")
	flag.BoolVar(&failResume, "fail-resume", false, "use fail resume test case")
	flag.BoolVar(&enableGTID, "enable-gtid", false, "enable GTID")
	flag.Int64Var(&maxBinlogSize, "max-binlog-size", 1073741824, "max binlog size")
}

func main() {
	log.Infof("Second commit (￣◇￣;)")
	fmt.Println("First commit (((o(*ﾟ▽ﾟ*)o)))")
}