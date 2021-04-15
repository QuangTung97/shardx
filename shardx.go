package shardx

import "time"

// NodeID ...
type NodeID uint32

// PartitionID ...
type PartitionID uint32

// LeaseID ...
type LeaseID int64

// Revision ...
type Revision int64

//go:generate moq -out generated_moq_test.go . Timer

// Timer ...
type Timer interface {
	Reset()
	Stop()
	Chan() <-chan time.Time
}

// Runner ...
type Runner struct {
}

// New ...
func New() *Runner {
	return &Runner{}
}
