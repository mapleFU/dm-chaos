package utils

import (
	"bytes"
	"fmt"
)
import "text/template"

/**
cfg_render is used to config render files.
It:
1. read from os
2. render the files
3. return a content string
4. panic if render failed
 */

func TaskTemplate()  {

}

// source should be an mysql at 3306
// target should be an tidb at 4000.
func SyncSplitTemplate(sourceHost, targetHost string, tableId int, path string) (string, error) {

	t, err := template.ParseFiles(path)
	if err != nil {
		return "", err
	}
	var s string

	wbuf := bytes.NewBufferString(s)

	err = t.Execute(wbuf, map[string]string{
		"SourceHost": sourceHost,
		"TargetHost": targetHost,
		"TableID": fmt.Sprint(tableId),
	})
	if err != nil {
		return "", err
	}
	return wbuf.String(), nil
}

func TaskTemplateRender(targetHost string, taskId int, path string) (string, error) {
	t, err := template.ParseFiles(path)
	if err != nil {
		return "", err
	}
	var s string

	wbuf := bytes.NewBufferString(s)

	err = t.Execute(wbuf, map[string]string{
		"TargetHost": targetHost,
		"TestID": fmt.Sprint(taskId),
	})
	if err != nil {
		return "", err
	}
	return wbuf.String(), nil
}