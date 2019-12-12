package utils

import (
	"fmt"
	"testing"
)

func TestSyncSplitTemplate(t *testing.T) {
	s, err := SyncSplitTemplate("127.0.0.1", "127.0.0.1", 5)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(s)
}

func TestTaskTemplateRender(t *testing.T) {
	s, err := TaskTemplateRender("127.0.0.1",5)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(s)
}