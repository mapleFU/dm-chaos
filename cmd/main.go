package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/mapleFU/dm-chaos/clients/dmctl"
	"github.com/mapleFU/dm-chaos/clients/dmctl/pb"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	//"github.com/joho/godotenv"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	//"github.com/juju/errors"
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

type replicationMySQL struct {
	hosts   []string
	current int
}

var (
	mysqlFlavor       string
	mysqlVersion      string

	// The address of mysql server.
	// The count was initialized as one, but we may add more.
	mysqlAddress arrayFlags

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

var DefaultInitSqls = []string{
	"SET @@GLOBAL.SQL_MODE = '';",
	"SET @@SESSION.SQL_MODE = '';",
}

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

	flag.Var(&mysqlAddress, "mysql-address", "Some description for this param.")
}


type arrayFlags []string

func (i *arrayFlags) String() string {
	return "my string representation"
}

func (i *arrayFlags) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func adjustTaskMode() {
	if taskMode != FullMode {
		taskMode = AllMode
	}
}

func SetupNotify() chan<- os.Signal {
	sc := make(chan os.Signal, 1)

	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("[binlog] Got signal [%s] to exist.", sig)
		//cancel()
		os.Exit(0)
	}()

	return sc
}


func main() {
	// parse the fucking flags
	flag.Parse()

	//signalChan := SetupNotify()
	_ = SetupNotify()

	// TODO: This fucking init-sqls is placed in `base_task.go`, please place it to better place.
	DefaultInitSqls = append(DefaultInitSqls, fmt.Sprintf("SET @@GLOBAL.MAX_BINLOG_SIZE = %d;", maxBinlogSize))

	// init mysql address as :3306
	// localhost and mysql
	mysqlAddress1 := "49.235.195.198"
	mysqlAddress2 := "maplewish.cn"
	tidbAddress := "127.0.0.1"

	testMysqlAddress := make([]string, 0)
	testMysqlAddress = append(testMysqlAddress, mysqlAddress1)
	testMysqlAddress = append(testMysqlAddress, mysqlAddress2)

	tidbConn, mysqlDBConn, err := initialDBs(tidbAddress, testMysqlAddress)

	if err != nil {
		panic(errors.ErrorStack(err))
	}

	log.Infof("tidbConn is %v, mysqlDBConn is %v", tidbConn, mysqlDBConn)

	//dmWorker1Host := "127.0.0.1"
	//dmWorker1Port := "8262"
	//
	//dmWorker2Host := "127.0.0.1"
	//dmWorker2Port := "8263"

	dmMasterHost := "127.0.0.1"
	dmMasterPort := 8261

	// drive ycsb to inserting datas
	// this part will insert large
	var wg sync.WaitGroup
	for _, mysqlAdd := range testMysqlAddress {
		wg.Add(1)
		go func(remoteAddress string) {
			defer wg.Done()

			cmd := exec.Command("bash", "-c", fmt.Sprintf("bin/go-ycsb run mysql -P workload -p mysql.host=%v  -p mysql.port=3306", remoteAddress))
			cmd.Stdout = os.Stdout
			if err := cmd.Start(); err != nil {
				log.Panic(err)
			}

			if err := cmd.Wait(); err != nil {
				log.Panic(err)
			}
		}(mysqlAdd)
	}

	wg.Wait()

	ctl, err := dmctl.CreateDMMasterCtl(fmt.Sprintf("%s:%d", dmMasterHost, dmMasterPort))
	if err != nil {
		panic(errors.ErrorStack(err))
	}
	data, err := ioutil.ReadFile("task-template")
	if err != nil {
		panic(err)
	}
	fileTemplate := string(data)

	err = ctl.StartTask(context.Background(), fileTemplate, nil)
	if err != nil {
		panic(errors.ErrorStack(err))
	}
	resp, err := ctl.QueryStatus(context.Background(), "test")
	if err != nil {
		panic(errors.ErrorStack(err))
	}


	finished := false
	for finished != true {
		tag := true
		for _, v := range resp {
			for _, task := range v.SubTaskStatus {
				if task.GetStage() != pb.Stage_Finished {
					tag = false
					break
				}
				if task.GetStage() == pb.Stage_InvalidStage {
					panic("In invalid stage now")
				}
			}
		}
		if tag {
			finished = true
		}
	}

	// calling check split checker

}