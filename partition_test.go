package shardx

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAllocatePartitions(t *testing.T) {
	table := []struct {
		name      string
		partCount PartitionID
		current   map[NodeID][]PartitionID
		result    map[NodeID][]PartitionID
	}{
		{
			name:   "empty",
			result: map[NodeID][]PartitionID{},
		},
		{
			name:      "single-node-single-partition",
			partCount: 1,
			current: map[NodeID][]PartitionID{
				10: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {0},
			},
		},
		{
			name:      "single-node-multi-partitions",
			partCount: 3,
			current: map[NodeID][]PartitionID{
				10: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {0, 1, 2},
			},
		},
		{
			name:      "multi-nodes-multi-partitions",
			partCount: 3,
			current: map[NodeID][]PartitionID{
				10: nil,
				20: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {0, 1},
				20: {2},
			},
		},
		{
			name:      "multi-nodes-multi-partitions",
			partCount: 7,
			current: map[NodeID][]PartitionID{
				10: nil,
				20: nil,
				30: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {0, 1, 2},
				20: {3, 4},
				30: {5, 6},
			},
		},
		{
			name:      "with-current-allocations",
			partCount: 7,
			current: map[NodeID][]PartitionID{
				10: {5},
				20: nil,
				30: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {5, 0, 1},
				20: {2, 3},
				30: {4, 6},
			},
		},
		{
			name:      "with-current-allocations-bigger",
			partCount: 7,
			current: map[NodeID][]PartitionID{
				10: {5, 1, 3, 4},
				20: nil,
				30: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {5, 1, 3},
				20: {0, 2},
				30: {4, 6},
			},
		},
		{
			name:      "with-current-allocations-bigger",
			partCount: 7,
			current: map[NodeID][]PartitionID{
				10: {4},
				20: {3, 0, 1, 6},
				30: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {4, 1, 2},
				20: {3, 0},
				30: {5, 6},
			},
		},
		{
			name:      "with-current-allocations-bigger",
			partCount: 10,
			current: map[NodeID][]PartitionID{
				10: {4, 2, 1, 6, 7},
				20: {0, 3, 5},
				30: nil,
				40: nil,
			},
			result: map[NodeID][]PartitionID{
				10: {4, 2, 1},
				20: {0, 3, 5},
				30: {6, 7},
				40: {8, 9},
			},
		},
	}

	for _, entry := range table {
		e := entry // For Parallel Test
		t.Run(e.name, func(t *testing.T) {
			t.Parallel()

			result := allocatePartitions(e.current, e.partCount)
			assert.Equal(t, e.result, result)
		})
	}
}
