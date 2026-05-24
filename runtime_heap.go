package main

type visibilityHeap []*deliveryRecord

func (h visibilityHeap) Len() int { return len(h) }

func (h visibilityHeap) Less(i, j int) bool {
	if h[i].Deadline.Equal(h[j].Deadline) {
		return h[i].seq < h[j].seq
	}
	return h[i].Deadline.Before(h[j].Deadline)
}

func (h visibilityHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}

func (h *visibilityHeap) Push(x any) {
	dr := x.(*deliveryRecord)
	dr.heapIndex = len(*h)
	*h = append(*h, dr)
}

func (h *visibilityHeap) Pop() any {
	old := *h
	n := len(old)
	dr := old[n-1]
	dr.heapIndex = -1
	*h = old[0 : n-1]
	return dr
}
