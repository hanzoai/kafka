package protocol

import "testing"

// TestFetchDecodesEveryVersion: two partitions in one request, at every Fetch
// version, come back as the two partitions asked for. v5 added each
// partition's log_start_offset; a decoder that skips it misreads every
// partition after the first.
func TestFetchDecodesEveryVersion(t *testing.T) {
	s := newStack(t)
	s.topic(t, "versions", 2)
	produce(t, s.addr, "versions", 0, 3)
	w := dial(t, s.addr)

	for v := int16(0); v <= 12; v++ {
		resp, err := w.fetch(v, fetchRequest(0, 1, at{"versions", 0, 0}, at{"versions", 1, 0}))
		if err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 2 {
			t.Fatalf("v%d: response %+v, want one topic with two partitions", v, resp.Topics)
		}
		p0, p1 := resp.Topics[0].Partitions[0], resp.Topics[0].Partitions[1]
		if p0.Partition != 0 || p0.ErrorCode != 0 || p0.HighWatermark != 3 || len(p0.RecordBatches) == 0 {
			t.Fatalf("v%d: partition 0 = {index %d, error %d, high watermark %d, %d record bytes}, want {0, 0, 3, records}",
				v, p0.Partition, p0.ErrorCode, p0.HighWatermark, len(p0.RecordBatches))
		}
		if p1.Partition != 1 || p1.ErrorCode != 0 || p1.HighWatermark != 0 || len(p1.RecordBatches) != 0 {
			t.Fatalf("v%d: partition 1 = {index %d, error %d, high watermark %d, %d record bytes}, want {1, 0, 0, none}",
				v, p1.Partition, p1.ErrorCode, p1.HighWatermark, len(p1.RecordBatches))
		}
	}
}
