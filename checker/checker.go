package checker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/mapleFU/dm-chaos/clients"
	"github.com/mapleFU/dm-chaos/clients/dmctl"
	"github.com/mapleFU/dm-chaos/clients/dmctl/pb"
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

func ycsbInsertPhase(ycsbBianry , workloadPath string, sourceHosts []string) error {
	var wg sync.WaitGroup
	errorGroup := make([]error, len(sourceHosts))

	for mysqlCnt, mysqlAdd := range sourceHosts {
		wg.Add(1)
		go func(remoteAddress string, cnt int) {
			defer wg.Done()

			cmd := exec.Command("bash", "-c",
				fmt.Sprintf("%v run mysql -P %v -p mysql.host=%v -p mysql.port=3306 -p tablename=usertable%d",
					ycsbBianry, workloadPath, remoteAddress, cnt))

			cmd.Stdout = os.Stdout

			if err := cmd.Start(); err != nil {
				errorGroup[cnt] = err
				return
			}

			if err := cmd.Wait(); err != nil {
				errorGroup[cnt] = err
			}
		}(mysqlAdd, mysqlCnt)
	}

	wg.Wait()

	// return for errorGroup
	for _, e := range errorGroup {
		if e != nil {
			return errors.Cause(e)
		}
	}
	return nil
}

func taskStartPhase(sourceHosts []string, targetHost string, ctl *dmctl.DMMasterCtl, templatePath string) error {
	for id, _ := range sourceHosts {
		fileTemplate, err := utils.TaskTemplateRender(targetHost, id, templatePath)
		if err != nil {
			return errors.Cause(err)
		}
		err = ctl.StartTaskWithContent(context.Background(), fileTemplate, nil)
		if err != nil {
			return errors.Cause(err)
		}
	}
	return nil
}

func waitForFinishingPhase(sourceHosts []string, ctl *dmctl.DMMasterCtl) error {
	var wg sync.WaitGroup
	errorsGroup := make([]error, len(sourceHosts))
	for i := 0; i < len(sourceHosts); i++ {
		wg.Add(1)
		go func(taskId int) {
			taskName := fmt.Sprintf("test%d", taskId)
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					ctl.StopTask(context.Background(), taskName, nil)
				}
			}()
			resp, err := ctl.QueryStatus(context.Background(), taskName)
			if err != nil {
				errorsGroup[taskId] = err
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
						if task.GetStage() == pb.Stage_InvalidStage || task.GetStage() == pb.Stage_Paused {
							ctl.StopTask(context.Background(), taskName, nil)
							errorsGroup[taskId] = errors.New(fmt.Sprintf("task is in invalid stage now"))
						}
					}
				}
				if tag {
					finished = true
				}
			}
			time.Sleep(1 * time.Second)
		}(i)
	}
	// waiting all
	wg.Wait()
	// return for errorGroup
	for _, e := range errorsGroup {
		if e != nil {
			return errors.Cause(e)
		}
	}
	return nil
}

type CheckDMPathCfg struct {
	YcsbWorkloadPath string
	TaskTemplatePath string
	SplitCheckerTemplatePath string

	YcsbBinaryPath string
	SyncDiffBinaryPath string
}

func NewPathConfig(YcsbWorkloadPath, TaskTemplatePath, SplitCheckerTemplatePath,
	YcsbBinaryPath, SyncDiffBinaryPath string) *CheckDMPathCfg {
	return &CheckDMPathCfg{
		YcsbWorkloadPath:   YcsbWorkloadPath,
		TaskTemplatePath:   TaskTemplatePath,
		SplitCheckerTemplatePath:   SplitCheckerTemplatePath,
		YcsbBinaryPath:     YcsbBinaryPath,
		SyncDiffBinaryPath: SyncDiffBinaryPath,
	}
}

// CheckDM checks the dm using
// sourceAddress currently should be MySQL with port 3306.
func CheckDM(sourceHosts []string, targetHost, dmMasterHost string, dmMasterPort int, pathCfg* CheckDMPathCfg) error {
	if pathCfg == nil {
		panic("CheckDMPathCfg should not be nil")
	}

	maxBinlogSize := 1073741824
	_, _, err := initialDBs(targetHost, sourceHosts)

	DefaultInitSqls = append(DefaultInitSqls, fmt.Sprintf("SET @@GLOBAL.MAX_BINLOG_SIZE = %d;", maxBinlogSize))

	if err != nil {
		panic(errors.ErrorStack(err))
	}

	err = ycsbInsertPhase(pathCfg.YcsbBinaryPath, pathCfg.YcsbWorkloadPath, sourceHosts)

	if err != nil {
		log.Warnf("Warning: stop at `ycsbInsertPhase`, return.")
		return errors.Cause(err)
	}

	// init dmctl
	ctl, err := dmctl.CreateDMMasterCtl(fmt.Sprintf("%s:%d", dmMasterHost, dmMasterPort))
	if err != nil {
		panic(errors.ErrorStack(err))
	}

	err = taskStartPhase(sourceHosts, targetHost, ctl, pathCfg.TaskTemplatePath)
	if err != nil {
		log.Warnf("Warning: stop at `taskStartPhase`")
		return errors.Cause(err)
	}

	log.Infof("Enter waiting for finishing phase")
	err = waitForFinishingPhase(sourceHosts, ctl)
	if err != nil {
		log.Warnf("Warning: stop at `waitForFinishingPhase`")
		return errors.Cause(err)
	}

	// calling check split checker
	checker := clients.NewChecker()
	for cnt, mysqlConnAddr := range sourceHosts {
		if checker.CheckDatabase(pathCfg.SyncDiffBinaryPath, pathCfg.SplitCheckerTemplatePath, mysqlConnAddr, targetHost, cnt) {
			errorMsg := fmt.Sprintf("Sync from source %v to target %v failed", mysqlConnAddr, targetHost)
			log.Errorf(errorMsg)
			return errors.New(errorMsg)
		}
	}

	return nil
}