package protocol

import (
	"math"
	"testing"
	"time"

	"github.com/hanzoai/kafka/pubsub"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func sameOps(got, want map[string]int) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

// TestBoundsFollowStreamState holds the bounds cache to the stream: unchanged
// state answers from the cache for exactly one StreamInfo, and every way the
// state moves (publish, a foreign message at the tail, delete, purge,
// publish after purge) is seen on the next read.
func TestBoundsFollowStreamState(t *testing.T) {
	s := newStack(t)
	const topic = "bounds"
	s.topic(t, topic, 1)
	stream := pubsub.StreamName(topic, 0)
	api := s.watchAPI(t)

	want := func(step string, logStart, next int64) {
		t.Helper()
		bd, err := s.b.partitionBounds(topic, 0)
		if err != nil {
			t.Fatalf("%s: bounds: %v", step, err)
		}
		if bd.logStart != logStart || bd.next != next {
			t.Fatalf("%s: bounds [%d, %d), want [%d, %d)", step, bd.logStart, bd.next, logStart, next)
		}
	}
	// cached asserts that reading unchanged bounds costs one StreamInfo and
	// reads no message.
	cached := func(step string, logStart, next int64) {
		t.Helper()
		api.take(t)
		want(step, logStart, next)
		if got := api.take(t); !sameOps(got, map[string]int{"STREAM.INFO": 1}) {
			t.Fatalf("%s: unchanged bounds cost %v, want one STREAM.INFO", step, got)
		}
	}

	want("empty", 0, 0)
	cached("empty again", 0, 0)

	produce(t, s.addr, topic, 0, 5)
	want("after publish", 0, 5)
	cached("after publish, again", 0, 5)

	produce(t, s.addr, topic, 0, 3)
	want("after second publish", 0, 8)

	if _, err := s.js.Publish(pubsub.SubjectName(topic, 0), []byte("not a kafka batch")); err != nil {
		t.Fatalf("foreign publish: %v", err)
	}
	want("foreign tail", 0, 8)
	cached("foreign tail, again", 0, 8)

	info, err := s.js.StreamInfo(stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	if err := s.js.DeleteMsg(stream, info.State.FirstSeq); err != nil {
		t.Fatalf("delete head: %v", err)
	}
	want("head deleted", 1, 8)

	if err := s.js.PurgeStream(stream); err != nil {
		t.Fatalf("purge: %v", err)
	}
	want("purged", 0, 0)
	cached("purged, again", 0, 0)

	produce(t, s.addr, topic, 0, 2)
	want("publish after purge", 0, 2)
}

// TestIdleFetchReadsNoMessage: a caught-up fetch costs one StreamInfo per
// partition, whether it answers at once or parks for its wait, and never
// reads a message or creates a consumer.
func TestIdleFetchReadsNoMessage(t *testing.T) {
	s := newStack(t)
	s.topic(t, "idle", 1)
	produce(t, s.addr, "idle", 0, 3)
	w := dial(t, s.addr)
	api := s.watchAPI(t)

	if _, err := w.fetch(12, fetchRequest(0, 1, at{"idle", 0, 3})); err != nil {
		t.Fatalf("warm fetch: %v", err)
	}
	api.take(t)
	for range 10 {
		if _, err := w.fetch(12, fetchRequest(0, 1, at{"idle", 0, 3})); err != nil {
			t.Fatalf("fetch: %v", err)
		}
	}
	if got := api.take(t); !sameOps(got, map[string]int{"STREAM.INFO": 10}) {
		t.Fatalf("10 caught-up fetches cost %v, want 10 STREAM.INFO", got)
	}
	if _, err := w.fetch(12, fetchRequest(200, 1, at{"idle", 0, 3})); err != nil {
		t.Fatalf("parked fetch: %v", err)
	}
	if got := api.take(t); !sameOps(got, map[string]int{"STREAM.INFO": 1}) {
		t.Fatalf("a parked caught-up fetch cost %v, want one STREAM.INFO", got)
	}
}

// TestCaughtUpFetchHoldsForMaxWait: at every Fetch version this broker
// serves, a caught-up fetch is held for its MaxWaitMs and then answered empty
// with the partition's watermarks.
func TestCaughtUpFetchHoldsForMaxWait(t *testing.T) {
	s := newStack(t)
	s.topic(t, "hold", 1)
	produce(t, s.addr, "hold", 0, 3)
	w := dial(t, s.addr)

	const wait = 150 * time.Millisecond
	for v := int16(0); v <= 12; v++ {
		start := time.Now()
		resp, err := w.fetch(v, fetchRequest(int32(wait/time.Millisecond), 1, at{"hold", 0, 3}))
		took := time.Since(start)
		if err != nil {
			t.Fatalf("v%d: %v", v, err)
		}
		if took < wait || took > wait+time.Second {
			t.Fatalf("v%d: answered after %v, want about %v", v, took, wait)
		}
		p := resp.Topics[0].Partitions[0]
		if p.ErrorCode != 0 || p.HighWatermark != 3 || len(p.RecordBatches) != 0 {
			t.Fatalf("v%d: error %d, high watermark %d, %d record bytes; want 0, 3, 0",
				v, p.ErrorCode, p.HighWatermark, len(p.RecordBatches))
		}
	}
}

// TestFetchAnswersAtOnce: no wait, a negative wait, an out-of-range offset,
// MinBytes already met, and records stored past what one response carries all
// answer without holding the fetch.
func TestFetchAnswersAtOnce(t *testing.T) {
	s := newStack(t)
	s.topic(t, "now", 1)
	produce(t, s.addr, "now", 0, 3)
	w := dial(t, s.addr)

	for _, c := range []struct {
		name      string
		req       *kmsg.FetchRequest
		errorCode int16
		records   bool
	}{
		{"no wait", fetchRequest(0, 1, at{"now", 0, 3}), 0, false},
		{"negative wait", fetchRequest(-1, 1, at{"now", 0, 3}), 0, false},
		{"zero MinBytes", fetchRequest(10_000, 0, at{"now", 0, 3}), 0, false},
		{"out of range", fetchRequest(10_000, 1, at{"now", 0, 13}), ErrOffsetOutOfRange.Code, false},
		{"data waiting", fetchRequest(10_000, 1, at{"now", 0, 0}), 0, true},
		// One record set per partition is all a fetch carries; with more
		// stored behind it, waiting cannot reach MinBytes.
		{"more than one record set waiting", fetchRequest(10_000, 1<<20, at{"now", 0, 0}), 0, true},
	} {
		start := time.Now()
		resp, err := w.fetch(12, c.req)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if took := time.Since(start); took > 500*time.Millisecond {
			t.Fatalf("%s: answered after %v, want at once", c.name, took)
		}
		p := resp.Topics[0].Partitions[0]
		if p.ErrorCode != c.errorCode || (len(p.RecordBatches) > 0) != c.records {
			t.Fatalf("%s: error %d with %d record bytes, want error %d, records %v",
				c.name, p.ErrorCode, len(p.RecordBatches), c.errorCode, c.records)
		}
	}
}

// TestFetchHoldsForMinBytes: the last record set, short of MinBytes, is held
// until the wait runs out, then served.
func TestFetchHoldsForMinBytes(t *testing.T) {
	s := newStack(t)
	s.topic(t, "min", 1)
	produce(t, s.addr, "min", 0, 3)
	w := dial(t, s.addr)

	const wait = 200 * time.Millisecond
	start := time.Now()
	resp, err := w.fetch(12, fetchRequest(int32(wait/time.Millisecond), 1<<20, at{"min", 0, 2}))
	took := time.Since(start)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if took < wait || took > wait+time.Second {
		t.Fatalf("answered after %v, want about %v", took, wait)
	}
	if p := resp.Topics[0].Partitions[0]; p.ErrorCode != 0 || len(p.RecordBatches) == 0 {
		t.Fatalf("error %d with %d record bytes, want the records held", p.ErrorCode, len(p.RecordBatches))
	}
}

// parked reports whether a fetch is parked on stream.
func (b *Broker) parked(stream string) bool {
	b.waitMu.Lock()
	defer b.waitMu.Unlock()
	return len(b.waiters[stream]) > 0
}

// fetchAfterProduce parks a caught-up fetch on broker `on`, produces one
// record through `via` once it is parked, and returns how long the fetch
// took and what it got.
func fetchAfterProduce(t *testing.T, s *stack, on *Broker, onAddr, via, topic string) (time.Duration, kmsg.FetchResponseTopicPartition) {
	t.Helper()
	produce(t, s.addr, topic, 0, 2)
	w := dial(t, onAddr)
	type answer struct {
		resp *kmsg.FetchResponse
		err  error
	}
	done := make(chan answer, 1)
	start := time.Now()
	go func() {
		resp, err := w.fetch(12, fetchRequest(10_000, 1, at{topic, 0, 2}))
		done <- answer{resp, err}
	}()
	stream := pubsub.StreamName(topic, 0)
	for !on.parked(stream) {
		if time.Since(start) > 5*time.Second {
			t.Fatal("fetch never parked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	produce(t, via, topic, 0, 1)
	a := <-done
	if a.err != nil {
		t.Fatalf("fetch: %v", a.err)
	}
	return time.Since(start), a.resp.Topics[0].Partitions[0]
}

// TestParkedFetchWakesOnProduce: a parked fetch answers as soon as its
// partition gets a record, not when its 10s wait runs out.
func TestParkedFetchWakesOnProduce(t *testing.T) {
	s := newStack(t)
	s.topic(t, "wake", 1)
	took, p := fetchAfterProduce(t, s, s.b, s.addr, s.addr, "wake")
	if took > 5*time.Second {
		t.Fatalf("answered after %v; the produce did not wake it", took)
	}
	if p.ErrorCode != 0 || p.HighWatermark != 3 || len(p.RecordBatches) == 0 {
		t.Fatalf("error %d, high watermark %d, %d record bytes; want the new record",
			p.ErrorCode, p.HighWatermark, len(p.RecordBatches))
	}
}

// TestParkedFetchWakesOnOtherBroker: brokers share one store, so a fetch
// parked on one wakes for a produce served by another.
func TestParkedFetchWakesOnOtherBroker(t *testing.T) {
	s := newStack(t)
	s.topic(t, "across", 1)
	_, other := s.broker(t)
	took, p := fetchAfterProduce(t, s, s.b, s.addr, other, "across")
	if took > 5*time.Second {
		t.Fatalf("answered after %v; the other broker's produce did not wake it", took)
	}
	if p.ErrorCode != 0 || p.HighWatermark != 3 || len(p.RecordBatches) == 0 {
		t.Fatalf("error %d, high watermark %d, %d record bytes; want the new record",
			p.ErrorCode, p.HighWatermark, len(p.RecordBatches))
	}
}

// TestShutdownReleasesParkedFetch: a broker shutting down answers its parked
// fetches rather than holding them for their wait.
func TestShutdownReleasesParkedFetch(t *testing.T) {
	s := newStack(t)
	s.topic(t, "down", 1)
	w := dial(t, s.addr)
	done := make(chan error, 1)
	go func() {
		_, err := w.fetch(12, fetchRequest(20_000, 1, at{"down", 0, 0}))
		done <- err
	}()
	stream := pubsub.StreamName("down", 0)
	for start := time.Now(); !s.b.parked(stream); {
		if time.Since(start) > 5*time.Second {
			t.Fatal("fetch never parked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.b.Shutdown()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("parked fetch after shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown left the fetch parked")
	}
}

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

func TestFetchWait(t *testing.T) {
	for _, c := range []struct {
		maxWaitMs uint32
		want      time.Duration
	}{
		{0, 0},
		{500, 500 * time.Millisecond},
		{uint32(0xFFFFFFFF), 0}, // -1 on the wire
		{math.MaxInt32, maxFetchWait},
		{uint32(maxFetchWait/time.Millisecond) + 1, maxFetchWait},
	} {
		if got := fetchWait(c.maxWaitMs); got != c.want {
			t.Errorf("fetchWait(%d) = %v, want %v", int32(c.maxWaitMs), got, c.want)
		}
	}
}
