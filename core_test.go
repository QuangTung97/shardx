package shardx

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func newContext() context.Context {
	return context.Background()
}

func newCoreOptionsTest() coreOptions {
	opts := defaultCoreOptions()
	opts.putNodeTimer = newTimerMock()
	opts.updateExpectedTimer = newTimerMock()
	opts.updateCurrentTimer = newTimerMock()
	return opts
}

func newCoreTest() *core {
	return newCore(12000, "/sample", "", 0, newCoreOptionsTest())
}

func newCoreWithNode(prefix string, id NodeID, info string) *core {
	return newCore(id, prefix, info, 0, newCoreOptionsTest())
}

func newCoreWithPutNodeTimer(id NodeID, prefix string, timer Timer) *core {
	opts := newCoreOptionsTest()
	opts.putNodeTimer = timer
	return newCore(id, prefix, "", 0, opts)
}

func newCoreWithPartitions(id NodeID, prefix string, partitionCount PartitionID) *core {
	return newCore(id, prefix, "", partitionCount, newCoreOptionsTest())
}

func newCoreWithPartitionsAndExpectedTimer(id NodeID, prefix string, partitionCount PartitionID, timer Timer) *core {
	opts := newCoreOptionsTest()
	opts.updateExpectedTimer = timer
	return newCore(id, prefix, "", partitionCount, opts)
}

func newCoreWithPartitionsAndCurrentTimer(id NodeID, prefix string, partitionCount PartitionID, timer Timer) *core {
	opts := newCoreOptionsTest()
	opts.updateCurrentTimer = timer
	return newCore(id, prefix, "", partitionCount, opts)
}

func newTimerMock() *TimerMock {
	t := &TimerMock{}
	t.ChanFunc = func() <-chan time.Time {
		return nil
	}
	return t
}

func timerExpire(timer *TimerMock) {
	timer.ChanFunc = func() <-chan time.Time {
		ch := make(chan time.Time, 1)
		ch <- time.Now()
		return ch
	}
}

func TestCore_Run__Context_Cancelled(t *testing.T) {
	t.Parallel()

	c := newCoreTest()
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()

	c.run(ctx)
}

func TestCore_Run__Update_LeaseID__Need_Put_Node(t *testing.T) {
	t.Parallel()

	c := newCoreWithNode("/sample", 8, "some-addr")
	ctx := newContext()

	c.updateLeaseID(1000)
	output := c.run(ctx)

	assert.Equal(t, true, output.needPutNode)
	assert.Equal(t, putNodeCmd{
		key:     "/sample/node/8",
		value:   "some-addr",
		leaseID: 1000,
	}, output.putNodeCmd)
}

func TestCore_Run__Update_LeaseID__While_Requesting__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithNode("/sample", 8, "some-addr")
	ctx := newContext()

	c.updateLeaseID(1000)
	_ = c.run(ctx)
	c.updateLeaseID(2000)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Update_LeaseID__Then_Finish_Put_Node_OK__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithNode("/sample", 8, "some-addr")
	ctx := newContext()

	c.updateLeaseID(1000)
	_ = c.run(ctx)
	c.finishPutNode(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Update_LeaseID__Then_Finish_Put_Node_Error__Set_Timer(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()
	c := newCoreWithPutNodeTimer(80, "/sample", timer)
	ctx := newContext()

	c.updateLeaseID(1000)
	_ = c.run(ctx)
	c.finishPutNode(errors.New("put-node-error"))

	timer.ResetFunc = func() {
	}

	output := c.run(ctx)

	calls := timer.ResetCalls()
	assert.Equal(t, 1, len(calls))

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Put_Node_Error__Then__Timer_Expired(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()
	c := newCoreWithPutNodeTimer(80, "/example", timer)
	ctx := newContext()

	c.updateLeaseID(1500)
	_ = c.run(ctx)

	c.finishPutNode(errors.New("put-node-error"))
	timer.ResetFunc = func() {
	}
	_ = c.run(ctx)

	timerExpire(timer)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		needPutNode: true,
		putNodeCmd: putNodeCmd{
			key:     "/example/node/80",
			leaseID: 1500,
		},
	}, output)
}

func TestCore_Run__Finish_Put_Node_Error__Update_Lease__Stop_Timer(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()
	c := newCoreWithPutNodeTimer(80, "/example", timer)
	ctx := newContext()

	c.updateLeaseID(1500)
	_ = c.run(ctx)

	c.finishPutNode(errors.New("put-node-error"))
	timer.ResetFunc = func() {}
	_ = c.run(ctx)

	timer.StopFunc = func() {}

	c.updateLeaseID(2500)
	output := c.run(ctx)

	assert.Equal(t, 1, len(timer.StopCalls()))
	assert.Equal(t, runOutput{
		needPutNode: true,
		putNodeCmd: putNodeCmd{
			key:     "/example/node/80",
			leaseID: 2500,
		},
	}, output)
}

func TestCore_Run__Finish_Put_Node_OK__After_Update_Lease__Put_Node_Again(t *testing.T) {
	t.Parallel()

	c := newCoreWithNode("/sample", 8, "some-addr")
	ctx := newContext()

	c.updateLeaseID(1000)
	_ = c.run(ctx)
	c.updateLeaseID(2000)
	_ = c.run(ctx)
	c.finishPutNode(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		needPutNode: true,
		putNodeCmd: putNodeCmd{
			key:     "/sample/node/8",
			value:   "some-addr",
			leaseID: 2000,
		},
	}, output)
}

func TestCore_Run__Recv_Node_Events__Not_Leader__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithNode("/sample", 8, "some-addr")
	ctx := newContext()

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__SetLeader__With_No_Nodes__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 4)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__SetLeader__With_2_Nodes__Update_Expected_Partitions(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.setLeader("/sample/leader/1234", 550)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/0",
				value: "8",
			},
			{
				key:   "/sample/expected/1",
				value: "8",
			},
			{
				key:   "/sample/expected/2",
				value: "10",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Node_Events__With_Leader__Update_Expected_Partitions(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/0",
				value: "8",
			},
			{
				key:   "/sample/expected/1",
				value: "8",
			},
			{
				key:   "/sample/expected/2",
				value: "10",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Node_Event_Deleted__With_Leader__Not_Yet_Recv_Expected_Events__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.finishUpdateExpected(nil)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 8, eventType: eventTypeDelete},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Node_Event_Deleted__With_Leader__Not_Yet_Finish_Update_Expected__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "10",
		},
	})
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 8, eventType: eventTypeDelete},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Node_Event_Deleted__With_Leader__Reallocate_Expected_Partitions(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.finishUpdateExpected(nil)
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "10",
		},
	})
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 8, eventType: eventTypeDelete},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/0",
				value: "10",
			},
			{
				key:   "/sample/expected/1",
				value: "10",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Node_Events__Second_Times__First_Times_Not_Yet_Completed__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 9, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Expected__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.finishUpdateExpected(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Expected_Error__Set_Timer(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()

	c := newCoreWithPartitionsAndExpectedTimer(12, "/sample", 3, timer)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	timer.ResetFunc = func() {}

	c.finishUpdateExpected(errors.New("finish-update-expected-error"))
	output := c.run(ctx)

	assert.Equal(t, 1, len(timer.ResetCalls()))
	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Expected_Error__Set_Timer__Then_Timer_Expired__Retry_Update_Expected(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()

	c := newCoreWithPartitionsAndExpectedTimer(12, "/sample", 3, timer)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	timer.ResetFunc = func() {}

	c.finishUpdateExpected(errors.New("finish-update-expected-error"))
	_ = c.run(ctx)

	timerExpire(timer)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/0",
				value: "8",
			},
			{
				key:   "/sample/expected/1",
				value: "8",
			},
			{
				key:   "/sample/expected/2",
				value: "10",
			},
		},
	}, output)
}

func TestCore_Run__Finish_Update_Expected_Error__Recv_Node_Event__Stop_Timer_And_Update_Expected(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()

	c := newCoreWithPartitionsAndExpectedTimer(12, "/sample", 3, timer)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	timer.ResetFunc = func() {}

	c.finishUpdateExpected(errors.New("finish-update-expected-error"))
	_ = c.run(ctx)

	timer.StopFunc = func() {}

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 9, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, 1, len(timer.StopCalls()))
	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/0",
				value: "8",
			},
			{
				key:   "/sample/expected/1",
				value: "9",
			},
			{
				key:   "/sample/expected/2",
				value: "10",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Expected_Partition_Events__Not_Leader__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "11",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "13",
		},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Node_Events_Second_Times__After_Finish_Update_Expected__Reallocate_Partitions(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "8",
		},
		{
			key:   "/sample/expected/2",
			value: "10",
		},
	})
	_ = c.run(ctx)

	c.finishUpdateExpected(nil)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 13, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/1",
				value: "13",
			},
		},
	}, output)
}

func TestCore_Run__Leader_Changed__Reallocate_Partitions(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "10",
		},
	})
	_ = c.run(ctx)

	c.finishUpdateExpected(nil)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 13, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateExpectedLeader: leaderInfo{
			key: "/sample/leader/1234",
			rev: 550,
		},
		updateExpected: []updateExpected{
			{
				key:   "/sample/expected/1",
				value: "13",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Events__When_Not_Yet_Finish_Update_Expected__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "10",
		},
	})
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 13, eventType: eventTypePut},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Leader_Changed__After_Already_Finish_Update_And_Recv_Expected_Events__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.setLeader("/sample/leader/1234", 550)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{nodeID: 10, eventType: eventTypePut},
		{nodeID: 8, eventType: eventTypePut},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "8",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "10",
		},
	})
	_ = c.run(ctx)

	c.finishUpdateExpected(nil)
	_ = c.run(ctx)

	c.setLeader("/sample/leader/4567", 660)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Self_Expected_Event__Update_Current_Partition(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(3344)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateCurrentLeaseID: 3344,
		updateCurrent: []updateCurrent{
			{
				key:   "/sample/current/0",
				value: "12",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Self_Expected_Event__Without_LeaseID__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Self_Expected_Event__Update_Multi_Current_Partitions(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateCurrentLeaseID: 1234,
		updateCurrent: []updateCurrent{
			{
				key:   "/sample/current/0",
				value: "12",
			},
			{
				key:   "/sample/current/2",
				value: "12",
			},
		},
	}, output)
}

func TestCore_Run__Recv_Self_Expected_Events__While_Updating_Current_Partitions__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "12",
		},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Current__Not_Change_Anything__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.finishUpdateCurrent(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Current_Events__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/current/0",
			value:     "12",
			leaseID:   1234,
		},
		{
			eventType: eventTypePut,
			key:       "/sample/current/2",
			value:     "12",
			leaseID:   1234,
		},
	})
	output := c.run(ctx)
	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Current_Events__Then_Recv_Another_Expected_Events__Not_Finish_Update_Current__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/current/0",
			value:     "12",
			leaseID:   1234,
		},
		{
			eventType: eventTypePut,
			key:       "/sample/current/2",
			value:     "12",
			leaseID:   1234,
		},
	})
	output := c.run(ctx)
	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Recv_Another_Expected_Events__Then_Finish_Update_Current__Not_Recv_Current_Events__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.finishUpdateCurrent(nil)
	output := c.run(ctx)
	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Current_And_Recv_Current_Events__After_Expected_Changed__Update_Current_Again(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/current/0",
			value:     "12",
			leaseID:   1234,
		},
		{
			eventType: eventTypePut,
			key:       "/sample/current/2",
			value:     "12",
			leaseID:   1234,
		},
	})
	_ = c.run(ctx)

	c.finishUpdateCurrent(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateCurrentLeaseID: 1234,
		updateCurrent: []updateCurrent{
			{
				key:   "/sample/current/1",
				value: "12",
			},
		},
	}, output)
}

func TestCore_Run__Finish_Update_Current_And_Recv_Current_Events_With_Wrong_LeaseID__After_Expected_Changed__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/current/0",
			value:     "12",
			leaseID:   3333,
		},
		{
			eventType: eventTypePut,
			key:       "/sample/current/2",
			value:     "12",
			leaseID:   1234,
		},
	})
	_ = c.run(ctx)

	c.finishUpdateCurrent(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Current_And_Recv_Current_Events__Update_LeaseID__Without_Deleted_Current__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.updateLeaseID(4444)
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/current/0",
			value:     "12",
			leaseID:   1234,
		},
		{
			eventType: eventTypePut,
			key:       "/sample/current/2",
			value:     "12",
			leaseID:   1234,
		},
	})
	_ = c.run(ctx)

	c.finishUpdateCurrent(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Current_And_Recv_Current_Events__Update_LeaseID__With_Deleted_Current__Update_Current(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	c.updateLeaseID(4444)
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/current/0",
			value:     "12",
			leaseID:   1234,
		},
		{
			eventType: eventTypePut,
			key:       "/sample/current/2",
			value:     "12",
			leaseID:   1234,
		},
	})
	_ = c.run(ctx)

	c.recvCurrentPartitionEvents([]currentEvent{
		{
			eventType: eventTypeDelete,
			key:       "/sample/current/0",
		},
		{
			eventType: eventTypeDelete,
			key:       "/sample/current/2",
		},
	})
	_ = c.run(ctx)

	c.finishUpdateCurrent(nil)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateCurrentLeaseID: 4444,
		updateCurrent: []updateCurrent{
			{
				key:   "/sample/current/0",
				value: "12",
			},
			{
				key:   "/sample/current/2",
				value: "12",
			},
		},
	}, output)
}

func TestCore_Run__Finish_Update_Current_Error__Set_Timer(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()
	c := newCoreWithPartitionsAndCurrentTimer(12, "/sample", 3, timer)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	timer.ResetFunc = func() {}

	c.finishUpdateCurrent(errors.New("finish-update-error"))
	output := c.run(ctx)

	assert.Equal(t, 1, len(timer.ResetCalls()))
	assert.Equal(t, runOutput{}, output)
}

func TestCore_Run__Finish_Update_Current_Error__Timer_Expired__Retry_Update_Current(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()
	c := newCoreWithPartitionsAndCurrentTimer(12, "/sample", 3, timer)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	timer.ResetFunc = func() {}

	c.finishUpdateCurrent(errors.New("finish-update-error"))
	_ = c.run(ctx)

	timerExpire(timer)
	output := c.run(ctx)

	assert.Equal(t, runOutput{
		updateCurrentLeaseID: 1234,
		updateCurrent: []updateCurrent{
			{
				key:   "/sample/current/0",
				value: "12",
			},
			{
				key:   "/sample/current/2",
				value: "12",
			},
		},
	}, output)
}

func TestCore_Run__Finish_Update_Current_Error__Lease_Changed__Stop_Timer_And_Update_Current_Again(t *testing.T) {
	t.Parallel()

	timer := newTimerMock()
	c := newCoreWithPartitionsAndCurrentTimer(12, "/sample", 3, timer)
	ctx := newContext()

	c.updateLeaseID(1234)
	_ = c.run(ctx)

	c.recvNodeEvents([]nodeEvent{
		{
			eventType: eventTypePut,
			nodeID:    12,
		},
	})
	_ = c.run(ctx)

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/1",
			value:     "9",
		},
		{
			eventType: eventTypePut,
			key:       "/sample/expected/2",
			value:     "12",
		},
	})
	_ = c.run(ctx)

	timer.ResetFunc = func() {}

	c.finishUpdateCurrent(errors.New("finish-update-error"))
	_ = c.run(ctx)

	timer.StopFunc = func() {}

	c.updateLeaseID(2222)
	output := c.run(ctx)

	assert.Equal(t, 1, len(timer.StopCalls()))
	assert.Equal(t, runOutput{
		updateCurrentLeaseID: 2222,
		updateCurrent: []updateCurrent{
			{
				key:   "/sample/current/0",
				value: "12",
			},
			{
				key:   "/sample/current/2",
				value: "12",
			},
		},
	}, output)
}

func TestCore_Run__Not_Recv_Self_Node_Event__Do_Nothing(t *testing.T) {
	t.Parallel()

	c := newCoreWithPartitions(12, "/sample", 3)
	ctx := newContext()

	c.recvExpectedPartitionEvents([]expectedEvent{
		{
			eventType: eventTypePut,
			key:       "/sample/expected/0",
			value:     "12",
		},
	})
	output := c.run(ctx)

	assert.Equal(t, runOutput{}, output)
}

// HELPERS

func TestNodesEqual(t *testing.T) {
	table := []struct {
		name   string
		a      map[NodeID]struct{}
		b      map[NodeID]struct{}
		result bool
	}{
		{
			name:   "both-empty",
			result: true,
		},
		{
			name: "a-empty",
			b: map[NodeID]struct{}{
				10: {},
			},
			result: false,
		},
		{
			name: "b-empty",
			a: map[NodeID]struct{}{
				12: {},
			},
			result: false,
		},
		{
			name: "a-b-two-elements-eq",
			a: map[NodeID]struct{}{
				12: {}, 20: {},
			},
			b: map[NodeID]struct{}{
				12: {}, 20: {},
			},
			result: true,
		},
		{
			name: "a-b-two-elements-not-eq",
			a: map[NodeID]struct{}{
				12: {}, 21: {},
			},
			b: map[NodeID]struct{}{
				12: {}, 20: {},
			},
			result: false,
		},
	}

	for _, entry := range table {
		e := entry
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := nodesEqual(e.a, e.b)
			assert.Equal(t, e.result, result)
		})
	}
}

func TestExpectedPartitionIDFromKey_OK(t *testing.T) {
	t.Parallel()
	v := expectedPartitionIDFromKey("/sample", "/sample/expected/11")
	assert.Equal(t, PartitionID(11), v)
}

func TestExpectedPartitionIDFromKey_Panic(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() {
		expectedPartitionIDFromKey("/sample", "/app/expected/11")
	})
}

func TestNodeIDFromValue_OK(t *testing.T) {
	t.Parallel()
	v := nodeIDFromValue("8")
	assert.Equal(t, NodeID(8), v)
}

func TestNodeIDFromValue_Panic(t *testing.T) {
	t.Parallel()
	assert.Panics(t, func() {
		nodeIDFromValue("sample")
	})
}

func TestExpectedEquals(t *testing.T) {
	table := []struct {
		name   string
		a      []expectedState
		b      []expectedState
		result bool
	}{
		{
			name:   "both-empty",
			result: true,
		},
		{
			name: "a-not-empty",
			a: []expectedState{
				{nodeID: 10},
			},
			result: false,
		},
		{
			name: "b-not-empty",
			b: []expectedState{
				{nodeID: 10},
			},
			result: false,
		},
		{
			name: "a-b-not-empty-not-eq",
			a: []expectedState{
				{nodeID: 12},
			},
			b: []expectedState{
				{nodeID: 10},
			},
			result: false,
		},
		{
			name: "a-b-not-empty-eq",
			a: []expectedState{
				{nodeID: 10},
			},
			b: []expectedState{
				{nodeID: 10},
			},
			result: true,
		},
	}
	for _, entry := range table {
		e := entry
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := expectedEquals(e.a, e.b)
			assert.Equal(t, e.result, result)
		})
	}
}
