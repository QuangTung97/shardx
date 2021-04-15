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
	return opts
}

func newCoreTest() *core {
	return newCore(12000, "/sample", "", 0, newCoreOptionsTest())
}

func newCoreWithNode(prefix string, id NodeID, info string) *core {
	return newCore(id, prefix, info, 0, newCoreOptionsTest())
}

func newCoreWithPutNodeTimer(id NodeID, prefix string, timer Timer) *core {
	opts := defaultCoreOptions()
	opts.putNodeTimer = timer
	return newCore(id, prefix, "", 0, opts)
}

func newCoreWithPartitions(id NodeID, prefix string, partitionCount PartitionID) *core {
	return newCore(id, prefix, "", partitionCount, newCoreOptionsTest())
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

	assert.Equal(t, runOutput{
	}, output)
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
		updateExpected: []updateExpected{
			{
				key:       "/sample/expected/0",
				value:     "8",
				leaderKey: "/sample/leader/1234",
				leaderRev: 550,
			},
			{
				key:       "/sample/expected/1",
				value:     "8",
				leaderKey: "/sample/leader/1234",
				leaderRev: 550,
			},
			{
				key:       "/sample/expected/2",
				value:     "10",
				leaderKey: "/sample/leader/1234",
				leaderRev: 550,
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
		updateExpected: []updateExpected{
			{
				key:       "/sample/expected/0",
				value:     "8",
				leaderKey: "/sample/leader/1234",
				leaderRev: 550,
			},
			{
				key:       "/sample/expected/1",
				value:     "8",
				leaderKey: "/sample/leader/1234",
				leaderRev: 550,
			},
			{
				key:       "/sample/expected/2",
				value:     "10",
				leaderKey: "/sample/leader/1234",
				leaderRev: 550,
			},
		},
	}, output)
}
