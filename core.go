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

// CHANNEL EVENTS

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

type expectedEvent struct {
	eventType   eventType
	partitionID PartitionID
	nodeID      NodeID
}

type expectedEvents struct {
	events []expectedEvent
}

// STRUCTS

type coreOptions struct {
	chanSize            int
	putNodeTimer        Timer
	updateExpectedTimer Timer
}

func defaultCoreOptions() coreOptions {
	return coreOptions{
		chanSize:     100,
		putNodeTimer: defaultTimer(30 * time.Second),
	}
}

type putNodeState struct {
	requesting bool
	leaseID    LeaseID
}

type updateExpectedState struct {
	requesting bool
	leader     leaderInfo
	nodes      map[NodeID]struct{}
}

type expectedState struct {
	// zero means node is invalid
	nodeID NodeID
}

func cloneNodes(nodes map[NodeID]struct{}) map[NodeID]struct{} {
	result := map[NodeID]struct{}{}
	for k, v := range nodes {
		result[k] = v
	}
	return result
}

func nodesEqual(a, b map[NodeID]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for n := range a {
		_, existed := b[n]
		if !existed {
			return false
		}
	}
	return true
}

type core struct {
	// configure
	prefix string

	selfNodeID     NodeID
	selfNodeInfo   string
	partitionCount PartitionID

	// Channel
	leaseChan         chan LeaseID
	finishPutNodeChan chan error
	nodeEventsChan    chan nodeEvents

	leaderChan chan leaderInfo

	finishExpectedChan chan error
	expectedEventsChan chan expectedEvents

	// Timer
	putNodeTimerRunning bool
	putNodeTimer        Timer

	updateExpectedTimerRunning bool
	updateExpectedTimer        Timer

	// state
	leaseID      LeaseID
	putNodeState putNodeState

	leader leaderInfo

	updateExpectedState updateExpectedState

	nodes    map[NodeID]struct{}
	expected []expectedState
}

func newCore(nodeID NodeID, prefix string, info string, partitionCount PartitionID, opts coreOptions) *core {
	return &core{
		// configure
		prefix:         prefix,
		selfNodeID:     nodeID,
		selfNodeInfo:   info,
		partitionCount: partitionCount,

		// channel
		leaseChan:         make(chan LeaseID, opts.chanSize),
		finishPutNodeChan: make(chan error, opts.chanSize),
		nodeEventsChan:    make(chan nodeEvents, opts.chanSize),

		leaderChan: make(chan leaderInfo, opts.chanSize),

		finishExpectedChan: make(chan error, opts.chanSize),
		expectedEventsChan: make(chan expectedEvents, opts.chanSize),

		// timer
		putNodeTimer:        opts.putNodeTimer,
		updateExpectedTimer: opts.updateExpectedTimer,

		// state
		leaseID: 0,
		putNodeState: putNodeState{
			requesting: false,
			leaseID:    0,
		},
		leader: leaderInfo{
			key: "",
			rev: 0,
		},
		updateExpectedState: updateExpectedState{
			requesting: false,
		},

		nodes:    map[NodeID]struct{}{},
		expected: make([]expectedState, partitionCount),
	}
}

type putNodeCmd struct {
	key     string
	value   string
	leaseID LeaseID
}

type updateExpected struct {
	key   string
	value string
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
	needPutNode bool
	putNodeCmd  putNodeCmd

	updateExpectedLeader leaderInfo
	updateExpected       []updateExpected
}

func computePutNodeCmd(
	prefix string, selfNodeID NodeID, selfNodeInfo string,
	leaseID LeaseID,
) putNodeCmd {
	return putNodeCmd{
		key:     fmt.Sprintf("%s/node/%d", prefix, selfNodeID),
		value:   selfNodeInfo,
		leaseID: leaseID,
	}
}

func (c *core) resetPutNodeState() {
	c.putNodeState = putNodeState{
		requesting: false,
		leaseID:    0,
	}
}

func (c *core) computePutNodeActions(output *runOutput) {
	if c.putNodeState.requesting {
		return
	}

	if c.leaseID == c.putNodeState.leaseID {
		return
	}

	c.putNodeState = putNodeState{
		requesting: true,
		leaseID:    c.leaseID,
	}

	if c.putNodeTimerRunning {
		c.putNodeTimerRunning = false
		c.putNodeTimer.Stop()
	}

	output.needPutNode = true
	output.putNodeCmd = computePutNodeCmd(c.prefix, c.selfNodeID, c.selfNodeInfo, c.leaseID)
}

func computeUpdateExpected(
	partitionCount PartitionID, prefix string,
	nodes map[NodeID]struct{}, expected []expectedState,
	leader leaderInfo,
) ([]updateExpected, leaderInfo) {
	current := map[NodeID][]PartitionID{}

	for partitionID, p := range expected {
		if p.nodeID == 0 {
			continue
		}
		current[p.nodeID] = append(current[p.nodeID], PartitionID(partitionID))
	}

	expectedFinalState := allocatePartitions(current, nodes, partitionCount)
	var result []updateExpected
	for n, partitions := range expectedFinalState {
		currentPartitions := current[n]
		if len(partitions) <= len(currentPartitions) {
			continue
		}

		for _, p := range partitions[len(currentPartitions):] {
			result = append(result, updateExpected{
				key:   fmt.Sprintf("%s/expected/%d", prefix, p),
				value: fmt.Sprintf("%d", n),
			})
		}
	}

	sort.Sort(sortUpdateExpected(result))
	return result, leader
}

func (c *core) computeExpectedPartitionActions(output *runOutput) {
	if c.updateExpectedState.requesting {
		return
	}

	if len(c.nodes) == 0 {
		return
	}
	if c.leader.rev == 0 {
		return
	}

	if nodesEqual(c.updateExpectedState.nodes, c.nodes) &&
		c.updateExpectedState.leader == c.leader {
		return
	}

	c.updateExpectedState = updateExpectedState{
		requesting: true,
		leader:     c.leader,
		nodes:      cloneNodes(c.nodes),
	}

	if c.updateExpectedTimerRunning {
		c.updateExpectedTimer.Stop()
	}

	output.updateExpected, output.updateExpectedLeader = computeUpdateExpected(
		c.partitionCount, c.prefix, c.nodes, c.expected, c.leader)
}

func (c *core) computeActions() runOutput {
	var output runOutput
	c.computePutNodeActions(&output)
	c.computeExpectedPartitionActions(&output)
	return output
}

func (c *core) handleFinishPutNode(err error) {
	c.putNodeState.requesting = false

	if err != nil {
		c.putNodeTimerRunning = true
		c.putNodeTimer.Reset()
	}
}

func (c *core) handleNodeEvents(ev nodeEvents) {
	for _, e := range ev.events {
		if e.eventType == eventTypePut {
			c.nodes[e.nodeID] = struct{}{}
		} else {
			delete(c.nodes, e.nodeID)
		}
	}
}

func (c *core) handleFinishUpdateExpected(err error) {
	c.updateExpectedState.requesting = false

	if err != nil {
		c.updateExpectedTimerRunning = true
		c.updateExpectedTimer.Reset()
	}
}

func (c *core) resetUpdateExpectedState() {
	c.updateExpectedState = updateExpectedState{
		requesting: false,
	}
}

func (c *core) handleExpectedEvents(ev expectedEvents) {
	for _, e := range ev.events {
		c.expected[e.partitionID] = expectedState{
			nodeID: e.nodeID,
		}
	}
}

func (c *core) run(ctx context.Context) runOutput {
	select {
	case leaseID := <-c.leaseChan:
		c.leaseID = leaseID

	case <-c.putNodeTimer.Chan():
		c.putNodeTimerRunning = false
		c.resetPutNodeState()

	case err := <-c.finishPutNodeChan:
		c.handleFinishPutNode(err)

	case ev := <-c.nodeEventsChan:
		c.handleNodeEvents(ev)

	case leader := <-c.leaderChan:
		c.leader = leader

	case err := <-c.finishExpectedChan:
		c.handleFinishUpdateExpected(err)

	case <-c.updateExpectedTimer.Chan():
		c.updateExpectedTimerRunning = false
		c.resetUpdateExpectedState()

	case ev := <-c.expectedEventsChan:
		c.handleExpectedEvents(ev)

	case <-ctx.Done():
	}
	return c.computeActions()
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

func (c *core) finishUpdateExpected(err error) {
	c.finishExpectedChan <- err
}

func (c *core) recvExpectedPartitionEvents(events []expectedEvent) {
	ev := make([]expectedEvent, len(events))
	copy(ev, events)
	c.expectedEventsChan <- expectedEvents{
		events: ev,
	}
}
