package main

// Deploy is a deploy config segment in dm-master.toml
type Deploy struct {
	SourceID string `toml:"source-id"`
	DmWorker string `toml:"dm-worker"`
}
