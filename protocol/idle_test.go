package protocol

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// BenchmarkIdleFetch measures what caught-up consumers cost when there is
// nothing to read: 60 connections over 20 topics, each looping the Fetch a
// librdkafka consumer sends when caught up (v12, the highest this broker
// serves, with MaxWaitMs 500 and MinBytes 1) against an embedded pubsub. One
// op is one fetch round trip. It reports process CPU per wall second in cores
// (broker, pubsub and clients together, as they share the process),
// JetStream API requests per second, and JetStream consumers created per
// second.
func BenchmarkIdleFetch(b *testing.B) {
	const consumers, topics, records = 60, 20, 3
	s := newStack(b)
	for i := range topics {
		name := fmt.Sprintf("idle-%02d", i)
		s.topic(b, name, 1)
		produce(b, s.addr, name, 0, records)
	}
	wires := make([]*wire, consumers)
	reqs := make([]*kmsg.FetchRequest, consumers)
	for i := range consumers {
		wires[i] = dial(b, s.addr)
		reqs[i] = fetchRequest(500, 1, at{fmt.Sprintf("idle-%02d", i%topics), 0, records})
	}
	failed := make(chan error, consumers)
	// One fetch per connection first, so the measurement starts from open
	// connections and partitions already read once.
	var warm sync.WaitGroup
	for i := range consumers {
		warm.Go(func() {
			if _, err := wires[i].fetch(12, fetchRequest(0, 1, at{fmt.Sprintf("idle-%02d", i%topics), 0, records})); err != nil {
				failed <- err
			}
		})
	}
	warm.Wait()
	select {
	case err := <-failed:
		b.Fatal(err)
	default:
	}
	api := s.watchAPI(b)

	var done atomic.Int64
	var wg sync.WaitGroup
	cpu0, t0 := cpuTime(b), time.Now()
	b.ResetTimer()
	for i := range consumers {
		wg.Go(func() {
			for done.Add(1) <= int64(b.N) {
				resp, err := wires[i].fetch(12, reqs[i])
				if err != nil {
					failed <- err
					return
				}
				p := resp.Topics[0].Partitions[0]
				if p.ErrorCode != 0 || p.HighWatermark != records || len(p.RecordBatches) != 0 {
					failed <- fmt.Errorf("caught-up fetch: error %d, high watermark %d, %d record bytes",
						p.ErrorCode, p.HighWatermark, len(p.RecordBatches))
					return
				}
			}
		})
	}
	wg.Wait()
	b.StopTimer()
	wall, cpu := time.Since(t0), cpuTime(b)-cpu0
	select {
	case err := <-failed:
		b.Fatal(err)
	default:
	}
	ops, calls := api.take(b), 0
	for _, n := range ops {
		calls += n
	}

	sec := wall.Seconds()
	b.ReportMetric(float64(b.N)/sec, "fetch/s")
	b.ReportMetric(cpu.Seconds()/sec, "cores")
	b.ReportMetric(float64(calls)/sec, "jsapi/s")
	b.ReportMetric(float64(ops["CONSUMER.CREATE"])/sec, "consumers/s")
	b.Logf("JetStream API requests over %v: %v", wall.Round(time.Millisecond), ops)
}
