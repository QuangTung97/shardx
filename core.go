package shardx

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	eventType eventType
	key       string
	value     string
}

type currentEvent struct {
	eventType eventType
	key       string
	value     string
	leaseID   LeaseID
}

type expectedEvents struct {
	events []expectedEvent
}

type currentEvents struct {
	events []currentEvent
}

// STRUCTS

type coreOptions struct {
	chanSize            int
	putNodeTimer        Timer
	updateExpectedTimer Timer
	updateCurrentTimer  Timer
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
	completed  map[string]struct{}
}

type completedCurrentKey struct {
	key   string
	value string
}

type updateCurrentState struct {
	requesting bool
	leaseID    LeaseID
	expected   []expectedState
	completed  map[completedCurrentKey]struct{}
}

type expectedState struct {
	// zero means node is invalid
	nodeID NodeID
}

type currentState struct {
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

	finishCurrentChan chan error
	currentEventsChan chan currentEvents

	// Timer
	putNodeTimerRunning bool
	putNodeTimer        Timer

	updateExpectedTimerRunning bool
	updateExpectedTimer        Timer

	updateCurrentTimerRunning bool
	updateCurrentTimer        Timer

	// state
	leaseID      LeaseID
	putNodeState putNodeState

	leader leaderInfo

	updateExpectedState updateExpectedState

	updateCurrentState updateCurrentState

	nodes    map[NodeID]struct{}
	expected []expectedState
	current  []currentState
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

		finishCurrentChan: make(chan error, opts.chanSize),
		currentEventsChan: make(chan currentEvents, opts.chanSize),

		// timer
		putNodeTimer:        opts.putNodeTimer,
		updateExpectedTimer: opts.updateExpectedTimer,
		updateCurrentTimer:  opts.updateCurrentTimer,

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
		updateExpectedState: updateExpectedState{},
		updateCurrentState: updateCurrentState{
			requesting: false,
			expected:   make([]expectedState, partitionCount),
		},

		nodes:    map[NodeID]struct{}{},
		expected: make([]expectedState, partitionCount),
		current:  make([]currentState, partitionCount),
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

type updateCurrent struct {
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

	updateCurrentLeaseID LeaseID
	updateCurrent        []updateCurrent

	startPartitions []PartitionID
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
		// TODO Check if missing
		// c.putNodeTimerRunning = false
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

	if len(result) == 0 {
		return nil, leaderInfo{}
	}

	sort.Sort(sortUpdateExpected(result))
	return result, leader
}

func (c *core) computeExpectedPartitionActions(output *runOutput) {
	if c.updateExpectedState.requesting || len(c.updateExpectedState.completed) > 0 {
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
		// TODO
		c.updateExpectedTimer.Stop()
	}

	output.updateExpected, output.updateExpectedLeader = computeUpdateExpected(
		c.partitionCount, c.prefix, c.nodes, c.expected, c.leader)

	completed := map[string]struct{}{}
	for _, update := range output.updateExpected {
		completed[update.key] = struct{}{}
	}
	c.updateExpectedState.completed = completed
}

func cloneExpectedState(expected []expectedState) []expectedState {
	result := make([]expectedState, len(expected))
	copy(result, expected)
	return result
}

func expectedEquals(a, b []expectedState) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (c *core) partitionIsRunnable(partitionID PartitionID) bool {
	_, existed := c.nodes[c.selfNodeID]
	if existed && c.expected[partitionID].nodeID == c.selfNodeID && c.current[partitionID].nodeID == 0 {
		return true
	}
	return false
}

func (c *core) computeCurrentPartitionOutput(output *runOutput) {
	output.updateCurrentLeaseID = c.leaseID
	for partitionID := PartitionID(0); partitionID < c.partitionCount; partitionID++ {
		if c.partitionIsRunnable(partitionID) {
			output.updateCurrent = append(output.updateCurrent, updateCurrent{
				key:   fmt.Sprintf("%s/current/%d", c.prefix, partitionID),
				value: fmt.Sprintf("%d", c.selfNodeID),
			})
		}
	}
}

func (c *core) setCompletedCurrentSet(updateList []updateCurrent) {
	completed := map[completedCurrentKey]struct{}{}
	for _, update := range updateList {
		completed[completedCurrentKey{
			key:   update.key,
			value: update.value,
		}] = struct{}{}
	}
	c.updateCurrentState.completed = completed
}

func (c *core) computeCurrentPartitionActions(output *runOutput) {
	// CHECKING
	if c.updateCurrentState.requesting {
		return
	}

	if c.leaseID == 0 {
		return
	}

	if len(c.updateCurrentState.completed) > 0 {
		return
	}

	if c.leaseID == c.updateCurrentState.leaseID &&
		expectedEquals(c.expected, c.updateCurrentState.expected) {
		return
	}

	count := 0
	for partitionID := PartitionID(0); partitionID < c.partitionCount; partitionID++ {
		if c.partitionIsRunnable(partitionID) {
			count++
		}
	}

	if count == 0 {
		return
	}

	// DOING

	c.updateCurrentState = updateCurrentState{
		requesting: true,
		leaseID:    c.leaseID,
		expected:   cloneExpectedState(c.expected),
	}

	if c.updateCurrentTimerRunning {
		// TODO
		// c.updateCurrentTimerRunning = false
		c.updateCurrentTimer.Stop()
	}

	c.computeCurrentPartitionOutput(output)
	c.setCompletedCurrentSet(output.updateCurrent)
}

func (c *core) computeActions() runOutput {
	var output runOutput
	c.computePutNodeActions(&output)
	c.computeExpectedPartitionActions(&output)
	c.computeCurrentPartitionActions(&output)
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
		c.updateExpectedState.completed = nil
		c.updateExpectedTimerRunning = true
		c.updateExpectedTimer.Reset()
	}
}

func (c *core) resetUpdateExpectedState() {
	c.updateExpectedState = updateExpectedState{}
}

func partitionIDFromKey(prefix, key, partition string) PartitionID {
	v := strings.TrimPrefix(key, fmt.Sprintf("%s/%s/", prefix, partition))
	id, err := strconv.Atoi(v)
	if err != nil {
		panic(err)
	}
	return PartitionID(id)
}

func expectedPartitionIDFromKey(prefix, key string) PartitionID {
	return partitionIDFromKey(prefix, key, "expected")
}

func currentPartitionIDFromKey(prefix, key string) PartitionID {
	return partitionIDFromKey(prefix, key, "current")
}

func nodeIDFromValue(value string) NodeID {
	id, err := strconv.Atoi(value)
	if err != nil {
		panic(err)
	}
	return NodeID(id)
}

func (c *core) handleExpectedEvents(events []expectedEvent) {
	for _, e := range events {
		partitionID := expectedPartitionIDFromKey(c.prefix, e.key)
		nodeID := nodeIDFromValue(e.value)

		c.expected[partitionID] = expectedState{
			nodeID: nodeID,
		}

		delete(c.updateExpectedState.completed, e.key)
	}
}

func (c *core) handleFinishUpdateCurrent(err error) {
	c.updateCurrentState.requesting = false

	if err != nil {
		c.updateCurrentState.completed = nil
		c.updateCurrentTimerRunning = true
		c.updateCurrentTimer.Reset()
	}
}

func (c *core) resetUpdateCurrentState() {
	c.updateCurrentState = updateCurrentState{}
}

func (c *core) handleCurrentEvents(events []currentEvent) {
	for _, e := range events {
		partitionID := currentPartitionIDFromKey(c.prefix, e.key)
		if e.eventType == eventTypePut {
			if e.leaseID != c.updateCurrentState.leaseID {
				continue
			}

			nodeID := nodeIDFromValue(e.value)
			c.current[partitionID] = currentState{
				nodeID: nodeID,
			}

			delete(c.updateCurrentState.completed, completedCurrentKey{
				key:   e.key,
				value: e.value,
			})
		} else {
			c.current[partitionID] = currentState{
				nodeID: 0,
			}
		}
	}
}

//gocyclo:ignore
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
		c.handleExpectedEvents(ev.events)

	case err := <-c.finishCurrentChan:
		c.handleFinishUpdateCurrent(err)

	case <-c.updateCurrentTimer.Chan():
		c.updateCurrentTimerRunning = false
		c.resetUpdateCurrentState()

	case ev := <-c.currentEventsChan:
		c.handleCurrentEvents(ev.events)

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

func (c *core) finishUpdateCurrent(err error) {
	c.finishCurrentChan <- err
}

func (c *core) recvExpectedPartitionEvents(events []expectedEvent) {
	ev := make([]expectedEvent, len(events))
	copy(ev, events)
	c.expectedEventsChan <- expectedEvents{
		events: ev,
	}
}

func (c *core) recvCurrentPartitionEvents(events []currentEvent) {
	ev := make([]currentEvent, len(events))
	copy(ev, events)
	c.currentEventsChan <- currentEvents{
		events: ev,
	}
}
