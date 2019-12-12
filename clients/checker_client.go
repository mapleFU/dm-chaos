package clients

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"strings"

	"github.com/mapleFU/dm-chaos/utils"
	log "github.com/sirupsen/logrus"
)

type CheckerClient struct {

}

func NewChecker() *CheckerClient {
	return &CheckerClient{}
}

// check if the current database and target database is the same database.
func (cc *CheckerClient) CheckDatabase(syncDiffInspectorBinary, sourceId, targetId string, tableId int) bool {
	template, err := utils.SyncSplitTemplate(sourceId, targetId, tableId)
	if err != nil {
		panic(err)
	}
	f, err := ioutil.TempFile(".", "temp-check")

	if err != nil {
		panic(err)
	}
	defer os.Remove(f.Name())
	_, err = f.WriteString(template)

	if err != nil {
		panic(err)
	}

	cmd := exec.Command("bash", "-c",
		fmt.Sprintf("%v -config %v", syncDiffInspectorBinary, f.Name()))

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("cmd.Run() failed with %s\n", err)
	}
	outs := string(out)
	if strings.Contains(outs, "failed") {
		return false
	} else {
		return true
	}
}