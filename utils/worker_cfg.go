package utils

import (
	"bytes"
	"fmt"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
)


var (
	DMMasterPort = 8261
	DMWorkerPort = 8262
)

var dmWorkerCfgTemplate = `
worker-addr = ":%[4]d"

server-id = %[1]d
flavor = "mysql"
relay-dir = "./relay_log"
enable-gtid = %[2]v
source-id = "%[3]s:3306"

[from]
host = "%[3]s"
user = "root"
password = ""
port = 3306

[purge]
interval = 300
expires = 1
remain-space = 15
`


// MasterConfig is used for creating dm master
type MasterConfig struct {
	MasterAddr string    `toml:"master-addr"`
	Deploy     []*Deploy `toml:"deploy"`
}

// Deploy is a deploy config segment in dm-master.toml
type Deploy struct {
	SourceID string `toml:"source-id"`
	DmWorker string `toml:"dm-worker"`
}

// NewMasterConfig creates new master config
func NewMasterConfig(deploy []*Deploy) *MasterConfig {
	return &MasterConfig{
		MasterAddr: fmt.Sprintf(":%d", DMMasterPort),
		Deploy:     deploy,
	}
}

func (m *MasterConfig) toToml() (string, error) {
	buf := new(bytes.Buffer)
	err := toml.NewEncoder(buf).Encode(m)
	if err != nil {
		return "", errors.Trace(err)
	}

	return buf.String(), nil
}
