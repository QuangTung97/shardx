package shardx

import (
	"context"
	"fmt"
	"sort"
	"time"
)

type eventType int

const (
	eventTypePut    eventType = 1
	eventTypeDelete eventType = 2
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

type leaderInfo struct {
	key string
	rev Revision
}

type nodeEvent struct {
	nodeID    NodeID
	eventType eventType
}

type nodeEvents struct {
	events []nodeEvent
}

type core struct {
	prefix string

	selfNodeID     NodeID
	selfNodeInfo   string
	partitionCount PartitionID

	leaseChan         chan LeaseID
	finishPutNodeChan chan error
	nodeEventsChan    chan nodeEvents

	leaderChan chan leaderInfo

	putNodeTimerRunning bool
	putNodeTimer        Timer

	// state
	leaseID      LeaseID
	putNodeState putNodeState

	leader leaderInfo

	nodes map[NodeID]struct{}
}

func newCore(nodeID NodeID, prefix string, info string, partitionCount PartitionID, opts coreOptions) *core {
	return &core{
		prefix:         prefix,
		selfNodeID:     nodeID,
		selfNodeInfo:   info,
		partitionCount: partitionCount,

		leaseChan:         make(chan LeaseID, opts.leaseChanSize),
		finishPutNodeChan: make(chan error, 100),
		nodeEventsChan:    make(chan nodeEvents, 100),

		leaderChan: make(chan leaderInfo, opts.leaseChanSize),

		putNodeTimer: opts.putNodeTimer,

		// state
		leaseID: 0,
		putNodeState: putNodeState{
			requesting: false,
			leaseID:    0,
		},

		nodes: map[NodeID]struct{}{},
	}
}

type putNodeCmd struct {
	key     string
	value   string
	leaseID LeaseID
}

type updateExpected struct {
	key       string
	value     string
	leaderKey string
	leaderRev Revision
}

type sortUpdateExpected []updateExpected

var _ sort.Interface = sortUpdateExpected{}

func (s sortUpdateExpected) Len() int {
	return len(s)
}

func (s sortUpdateExpected) Less(i, j int) bool {
	return s[i].key < s[j].key
}

func (s sortUpdateExpected) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

type runOutput struct {
	needPutNode    bool
	putNodeCmd     putNodeCmd
	updateExpected []updateExpected
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

func (c *core) computeExpectedPartitionActions() runOutput {
	current := map[NodeID][]PartitionID{}
	for n := range c.nodes {
		current[n] = nil
	}

	expected := allocatePartitions(current, c.partitionCount)
	var result []updateExpected
	for n, partitions := range expected {
		currentPartitions := current[n]
		if len(partitions) <= len(currentPartitions) {
			continue
		}

		for _, p := range partitions[len(currentPartitions):] {
			result = append(result, updateExpected{
				key:       fmt.Sprintf("%s/expected/%d", c.prefix, p),
				value:     fmt.Sprintf("%d", n),
				leaderKey: c.leader.key,
				leaderRev: c.leader.rev,
			})
		}
	}

	sort.Sort(sortUpdateExpected(result))

	return runOutput{
		updateExpected: result,
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

	case ev := <-c.nodeEventsChan:
		for _, e := range ev.events {
			c.nodes[e.nodeID] = struct{}{}
		}
		if c.leader.rev == 0 {
			return runOutput{}
		}
		return c.computeExpectedPartitionActions()

	case leader := <-c.leaderChan:
		c.leader = leader

		if len(c.nodes) == 0 {
			return runOutput{}
		}
		return c.computeExpectedPartitionActions()

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

func (c *core) setLeader(leaderKey string, leaderRev Revision) {
	c.leaderChan <- leaderInfo{
		key: leaderKey,
		rev: leaderRev,
	}
}

func (c *core) recvNodeEvents(events []nodeEvent) {
	ev := make([]nodeEvent, len(events))
	copy(ev, events)
	c.nodeEventsChan <- nodeEvents{
		events: ev,
	}
}
