package checker

import (
	"context"
	"fmt"
	"github.com/mapleFU/dm-chaos/clients"
	"github.com/mapleFU/dm-chaos/clients/dmctl/pb"
	"os/exec"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/mapleFU/dm-chaos/clients/dmctl"
	"github.com/mapleFU/dm-chaos/utils"
	log "github.com/sirupsen/logrus"
)

var DefaultInitSqls = []string{
	"SET @@GLOBAL.SQL_MODE = '';",
	"SET @@SESSION.SQL_MODE = '';",
}

var (
	AllMode  = "all"
	FullMode = "full"
)

func ycsbInsertPhase(ycsbBianry string, sourceHosts []string)  {
	var wg sync.WaitGroup
	for mysqlCnt, mysqlAdd := range sourceHosts {
		wg.Add(1)
		go func(remoteAddress string) {
			defer wg.Done()

			cmd := exec.Command("bash", "-c",
				fmt.Sprintf("%v run mysql -P workload -p mysql.host=%v  -p mysql.port=3306 -tablename=usertable%d",
					ycsbBianry, remoteAddress, mysqlCnt))
			if err := cmd.Start(); err != nil {
				log.Panic(err)
			}

			if err := cmd.Wait(); err != nil {
				log.Panic(err)
			}
		}(mysqlAdd)
	}

	wg.Wait()
}

func taskStartPhase(sourceHosts []string, targetHost string, ctl *dmctl.DMMasterCtl)  {
	for id, _ := range sourceHosts {
		fileTemplate, err := utils.TaskTemplateRender(targetHost, id)
		if err != nil {
			panic(errors.ErrorStack(err))
		}
		err = ctl.StartTask(context.Background(), fileTemplate, nil)
		if err != nil {
			panic(errors.ErrorStack(err))
		}
	}
}

func waitForFinishingPhase(sourceHosts []string, ctl *dmctl.DMMasterCtl)  {
	var wg sync.WaitGroup
	for i := 0; i < len(sourceHosts); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
			time.Sleep(1 * time.Second)
		}()
	}
	// waiting all
	wg.Wait()
}

// CheckDM checks the dm using
// sourceAddress currently should be MySQL with port 3306.
func CheckDM(sourceHosts []string, targetHost, dmMasterHost string, dmMasterPort int, ycsbBinary, syncDiffBinary string) error {
	maxBinlogSize := 1073741824
	_, _, err := initialDBs(targetHost, sourceHosts)

	DefaultInitSqls = append(DefaultInitSqls, fmt.Sprintf("SET @@GLOBAL.MAX_BINLOG_SIZE = %d;", maxBinlogSize))

	if err != nil {
		panic(errors.ErrorStack(err))
	}

	ycsbInsertPhase(ycsbBinary, sourceHosts)

	// init dmctl
	ctl, err := dmctl.CreateDMMasterCtl(fmt.Sprintf("%s:%d", dmMasterHost, dmMasterPort))
	if err != nil {
		panic(errors.ErrorStack(err))
	}

	taskStartPhase(sourceHosts, targetHost, ctl)

	waitForFinishingPhase(sourceHosts, ctl)

	// calling check split checker
	checker := clients.NewChecker()
	for cnt, mysqlConnAddr := range sourceHosts {
		if checker.CheckDatabase(syncDiffBinary, mysqlConnAddr, targetHost, cnt) {
			log.Errorf("Sync from source %v to target %v failed", mysqlConnAddr, targetHost)
		}
	}

	return nil
}