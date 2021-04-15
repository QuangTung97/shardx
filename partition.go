package shardx

import (
	"sort"
)

type sortNodeID []NodeID

var _ sort.Interface = sortNodeID{}

func (s sortNodeID) Len() int {
	return len(s)
}

func (s sortNodeID) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s sortNodeID) Swap(i, j int) {
	s[j], s[i] = s[i], s[j]
}

func computeFreePartitions(
	nodes []NodeID, current map[NodeID][]PartitionID, partitionCount PartitionID,
	higherCount int, higherValue PartitionID, lowerValue PartitionID,
) []PartitionID {
	freeList := make([]bool, partitionCount)
	for i := range freeList {
		freeList[i] = true
	}

	for i, n := range nodes {
		partitions := current[n]
		length := PartitionID(len(partitions))

		if i < higherCount {
			if length > higherValue {
				length = higherValue
			}

			for _, p := range partitions[:length] {
				freeList[p] = false
			}
		} else {
			if length > lowerValue {
				length = lowerValue
			}
			for _, p := range partitions[:length] {
				freeList[p] = false
			}
		}
	}

	freePartitions := make([]PartitionID, 0, partitionCount)
	for i := range freeList {
		if freeList[i] {
			p := PartitionID(i)
			freePartitions = append(freePartitions, p)
		}
	}

	return freePartitions
}

func allocatePartitions(
	current map[NodeID][]PartitionID, partitionCount PartitionID,
) map[NodeID][]PartitionID {
	if len(current) == 0 {
		return map[NodeID][]PartitionID{}
	}

	nodes := make([]NodeID, 0, len(current))
	for n := range current {
		nodes = append(nodes, n)
	}
	sort.Sort(sortNodeID(nodes))

	nodeCount := PartitionID(len(nodes))
	lowerValue := partitionCount / nodeCount
	higherCount := int(partitionCount - lowerValue*nodeCount)
	higherValue := lowerValue + 1

	freePartitions := computeFreePartitions(nodes, current, partitionCount, higherCount, higherValue, lowerValue)

	result := map[NodeID][]PartitionID{}
	for key, value := range current {
		result[key] = value
	}

	for i, n := range nodes {
		length := PartitionID(len(current[n]))

		if i < higherCount {
			if length > higherValue {
				result[n] = result[n][:higherValue]
				continue
			}

			num := higherValue - length
			for k := PartitionID(0); k < num; k++ {
				result[n] = append(result[n], freePartitions[0])
				freePartitions = freePartitions[1:]
			}
		} else {
			if length > lowerValue {
				result[n] = result[n][:lowerValue]
				continue
			}

			num := lowerValue - length
			for k := PartitionID(0); k < num; k++ {
				result[n] = append(result[n], freePartitions[0])
				freePartitions = freePartitions[1:]
			}
		}
	}
	return result
}
