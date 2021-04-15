package shardx

import (
	"context"
	"fmt"
	"time"
)

type coreOptions struct {
	leaseChanSize int
	putNodeTimer  Timer
}

type simpleTimer struct {
	duration time.Duration
	timer    *time.Timer
}

func (t *simpleTimer) Reset() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
	t.timer.Reset(t.duration)
}

func (t *simpleTimer) Stop() {
	if !t.timer.Stop() {
		<-t.timer.C
	}
}

func (t *simpleTimer) Chan() <-chan time.Time {
	return t.timer.C
}

func defaultTimer(duration time.Duration) *simpleTimer {
	timer := time.NewTimer(1000 * time.Hour)
	if !timer.Stop() {
		<-timer.C
	}

	return &simpleTimer{
		duration: duration,
		timer:    timer,
	}
}

func defaultCoreOptions() coreOptions {
	return coreOptions{
		leaseChanSize: 100,
		putNodeTimer:  defaultTimer(30 * time.Second),
	}
}

type putNodeState struct {
	requesting bool
	leaseID    LeaseID
}

type core struct {
	prefix string

	selfNodeID   NodeID
	selfNodeInfo NodeInfo

	leaseChan         chan LeaseID
	finishPutNodeChan chan error

	putNodeTimerRunning bool
	putNodeTimer        Timer

	// state
	leaseID      LeaseID
	putNodeState putNodeState
}

func newCore(nodeID NodeID, prefix string, info NodeInfo, opts coreOptions) *core {
	return &core{
		prefix:       prefix,
		selfNodeID:   nodeID,
		selfNodeInfo: info,

		leaseChan:         make(chan LeaseID, opts.leaseChanSize),
		finishPutNodeChan: make(chan error, 100),
		putNodeTimer:      opts.putNodeTimer,

		// state
		leaseID: 0,
		putNodeState: putNodeState{
			requesting: false,
			leaseID:    0,
		},
	}
}

type putNodeCmd struct {
	key     string
	value   NodeInfo
	leaseID LeaseID
}

type runOutput struct {
	needPutNode bool
	putNodeCmd  putNodeCmd
}

func (c *core) doPutNode() runOutput {
	c.putNodeState = putNodeState{
		requesting: true,
		leaseID:    c.leaseID,
	}
	if c.putNodeTimerRunning {
		c.putNodeTimerRunning = false
		c.putNodeTimer.Stop()
	}

	return runOutput{
		needPutNode: true,
		putNodeCmd: putNodeCmd{
			key:     fmt.Sprintf("%s/node/%d", c.prefix, c.selfNodeID),
			value:   c.selfNodeInfo,
			leaseID: c.leaseID,
		},
	}
}

func (c *core) run(ctx context.Context) runOutput {
	select {
	case leaseID := <-c.leaseChan:
		c.leaseID = leaseID
		if c.putNodeState.requesting {
			return runOutput{}
		}
		return c.doPutNode()

	case <-c.putNodeTimer.Chan():
		c.putNodeTimerRunning = false
		return c.doPutNode()

	case err := <-c.finishPutNodeChan:
		c.putNodeState.requesting = false

		if c.putNodeState.leaseID != c.leaseID {
			return c.doPutNode()
		}
		if err != nil {
			c.putNodeTimerRunning = true
			c.putNodeTimer.Reset()
		}

		return runOutput{}

	case <-ctx.Done():
		return runOutput{}
	}
}

func (c *core) updateLeaseID(id LeaseID) {
	c.leaseChan <- id
}

func (c *core) finishPutNode(err error) {
	c.finishPutNodeChan <- err
}
