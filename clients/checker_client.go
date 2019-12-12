package clients

import (
	"github.com/mapleFU/dm-chaos/utils"
	"io/ioutil"
)

type CheckerClient struct {

}

// check if the current database and target database is the same database.
func (cc *CheckerClient) CheckDatabase(sourceId, targetId string, tableId int) bool {
	template, err := utils.SyncSplitTemplate(sourceId, targetId, tableId)
	if err != nil {
		panic(err)
	}
	f, err := ioutil.TempFile(".", "temp-check")
	if err != nil {
		panic(err)
	}
	_, err = f.WriteString(template)

	if err != nil {
		panic(err)
	}


	return false
}