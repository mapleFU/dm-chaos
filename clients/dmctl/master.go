package dmctl

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
	"github.com/mapleFU/dm-chaos/clients/dmctl/pb"
	"google.golang.org/grpc"
)

var (
	workerClient pb.WorkerClient
	masterClient pb.MasterClient
)

// DMMasterCtl controls dm master
type DMMasterCtl struct {
	client pb.MasterClient
}

// CreateDMMasterCtl creates dm-master client
func CreateDMMasterCtl(addr string) (*DMMasterCtl, error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBackoffMaxDelay(3*time.Second))
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &DMMasterCtl{client: pb.NewMasterClient(conn)}, nil
}

// QueryStatus shows status of tasks
func (ctl *DMMasterCtl) QueryStatus(ctx context.Context, taskName string) (map[string]*pb.QueryStatusResponse, error) {
	if ctl == nil || ctl.client == nil {
		return nil, errors.NotValidf("dm master control need be initialized")
	}

	resp, err := ctl.client.QueryStatus(ctx, &pb.QueryStatusListRequest{
		Name: taskName,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.GetResult() {
		return nil, errors.Errorf("fail to query status of task %s: %s", taskName, resp.GetMsg())
	}

	responses := make(map[string]*pb.QueryStatusResponse)
	for _, resp := range resp.GetWorkers() {
		responses[resp.GetWorker()] = resp
	}

	return responses, nil
}

// QueryError shows status of tasks
func (ctl *DMMasterCtl) QueryError(ctx context.Context, taskName string) (map[string]*pb.QueryErrorResponse, error) {
	if ctl == nil || ctl.client == nil {
		return nil, errors.NotValidf("dm master control need be initialized")
	}
	resp, err := ctl.client.QueryError(ctx, &pb.QueryErrorListRequest{
		Name: taskName,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !resp.GetResult() {
		return nil, errors.Errorf("fail to query warning of task %s: %s", taskName, resp.GetMsg())
	}
	responses := make(map[string]*pb.QueryErrorResponse)
	for _, resp := range resp.GetWorkers() {
		responses[resp.GetWorker()] = resp
	}
	return responses, nil
}

// SkipSQL skips specified binlog position or sql pattern
func (ctl *DMMasterCtl) SkipSQL(ctx context.Context, taskName, worker, pos, sqlPattern string, sharding bool) (*pb.HandleSQLsResponse, error) {
	resp, err := ctl.OperateSQL(ctx, pb.SQLOp_SKIP, taskName, worker, pos, sqlPattern, sharding, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (ctl *DMMasterCtl) ReplaceSQL(ctx context.Context, taskName, worker, pos, sqlPattern string, sharding bool, args []string) (*pb.HandleSQLsResponse, error) {
	resp, err := ctl.OperateSQL(ctx, pb.SQLOp_REPLACE, taskName, worker, pos, sqlPattern, sharding, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (ctl *DMMasterCtl) OperateSQL(ctx context.Context, op pb.SQLOp, taskName, worker, pos, sqlPattern string, sharding bool, args []string) (*pb.HandleSQLsResponse, error) {
	if ctl == nil || ctl.client == nil {
		return nil, errors.NotValidf("dm master control need be initialized")
	}
	resp, err := ctl.client.HandleSQLs(ctx, &pb.HandleSQLsRequest{
		Name:       taskName,
		Op:         op,
		BinlogPos:  pos,
		Worker:     worker,
		SqlPattern: sqlPattern,
		Sharding:   sharding,
		Args:       args,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// ResumeTask resumes a paused task
func (ctl *DMMasterCtl) ResumeTask(ctx context.Context, taskName string, workers []string) (*pb.OperateTaskResponse, error) {
	if ctl == nil || ctl.client == nil {
		return nil, errors.NotValidf("dm master control need be initialized")
	}
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	resp, err := ctl.client.OperateTask(ctx2, &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Resume,
		Name:    taskName,
		Workers: workers,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (ctl *DMMasterCtl) QueryUnresolvedGroups(ctx context.Context, taskName string) (map[string]map[string][]*pb.ShardingGroup, error) {
	responses, err := ctl.QueryStatus(ctx, taskName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	groups := make(map[string](map[string][]*pb.ShardingGroup))
	for _, wp := range responses {
		if !wp.GetResult() {
			return nil, errors.Errorf("fail to status from worker %s: %s", wp.GetWorker(), wp.GetMsg())
		}

		status := wp.GetSubTaskStatus()
		if status == nil {
			continue
		}

		for _, s := range status {
			if s.GetUnit() == pb.UnitType_Sync {
				name := s.GetName()
				ug := s.GetSync().GetUnresolvedGroups()
				if len(ug) > 0 {
					worker := wp.GetWorker()
					if _, ok := groups[worker]; !ok {
						groups[worker] = make(map[string][]*pb.ShardingGroup)
					}
					groups[worker][name] = ug
				}
			}
		}
	}

	return groups, nil
}

// relayCatchedUp return whether relay is catched up
func (ctl *DMMasterCtl) relayCatchedUp(ctx context.Context, taskName string) (map[string]bool, error) {
	responses, err := ctl.QueryStatus(ctx, taskName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	synced := make(map[string]bool)
	for _, wp := range responses {
		if !wp.GetResult() {
			return nil, errors.Errorf("fail to status from worker %s: %s", wp.GetWorker(), wp.GetMsg())
		}

		relayStatus := wp.GetRelayStatus()
		if relayStatus == nil {
			return nil, errors.NotFoundf("relay status of worker %s", wp.GetWorker())
		}
		synced[wp.GetWorker()] = relayStatus.GetRelayCatchUpMaster()
	}

	return synced, nil
}

// syncerCatchedUp return whether syncer is catched up
func (ctl *DMMasterCtl) syncerCatchedUp(ctx context.Context, taskName string) (map[string]bool, error) {
	responses, err := ctl.QueryStatus(ctx, taskName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	synced := make(map[string]bool)
	for _, wp := range responses {
		if !wp.GetResult() {
			return nil, errors.Errorf("fail to status from worker %s: %s", wp.GetWorker(), wp.GetMsg())
		}

		status := wp.GetSubTaskStatus()
		if status == nil {
			return nil, errors.NotFoundf("subtask status of worker %s", wp.GetWorker())
		}

		for _, s := range status {
			if s.GetUnit() == pb.UnitType_Sync {
				synced[wp.GetWorker()] = s.GetSync().GetSynced()
			}
		}

	}
	return synced, nil
}

// loadCatchedUp returns whether load is finished
func (ctl *DMMasterCtl) loadFinished(ctx context.Context, taskName string) (map[string]bool, error) {
	responses, err := ctl.QueryStatus(ctx, taskName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	synced := make(map[string]bool)
	for _, wp := range responses {
		if !wp.GetResult() {
			return nil, errors.Errorf("fail to status from worker %s: %s", wp.GetWorker(), wp.GetMsg())
		}

		status := wp.GetSubTaskStatus()
		if status == nil {
			return nil, errors.NotFoundf("subtask status of worker %s", wp.GetWorker())
		}

		for _, s := range status {
			if s.GetUnit() == pb.UnitType_Load {
				synced[wp.GetWorker()] = (s.GetStage() == pb.Stage_Finished)
			}
		}
	}

	return synced, nil
}

// StartTask starts task
func (ctl *DMMasterCtl) StartTask(ctx context.Context, task string, workers []string) error {
	/*
		return cli.OperateTask(ctx, &pb.OperateTaskRequest{
			Op:      op,
			Name:    name,
			Workers: workers,
		})
	*/

	content, err := GetFileContent(task)
	if err != nil {
		return errors.Trace(err)
	}

	resp, err := ctl.client.StartTask(ctx, &pb.StartTaskRequest{
		Task:    string(content),
		Workers: workers,
	})
	if err != nil {
		return errors.Trace(err)
	}
	for _, wp := range resp.GetWorkers() {
		if !wp.GetResult() {
			return errors.Errorf("fail to start task %v: %s", string(content), wp.GetMsg())
		}
	}

	log.Infof("start task %s, response %+v", task, resp)

	return nil
}

// StopTask stops task
func (ctl *DMMasterCtl) StopTask(ctx context.Context, task string, workers []string) error {
	resp, err := ctl.client.OperateTask(ctx, &pb.OperateTaskRequest{
		Op:      pb.TaskOp_Stop,
		Name:    task,
		Workers: workers,
	})
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.GetResult() {
		return errors.Errorf("fail to stop task %s: %s", task, resp.GetMsg())
	}

	return nil
}

// Update master config online
func (ctl *DMMasterCtl) UpdateMasterConfig(ctx context.Context, cfg string) error {
	resp, err := ctl.client.UpdateMasterConfig(ctx, &pb.UpdateMasterConfigRequest{
		Config: cfg,
	})
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.GetResult() {
		return errors.Errorf("fail to update master config: %s", resp.GetMsg())
	}

	return nil
}

// GetFileContent reads and returns file's content
func GetFileContent(fpath string) ([]byte, error) {
	content, err := ioutil.ReadFile(fpath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return content, nil
}
