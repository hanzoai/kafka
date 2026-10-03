package protocol

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/hanzoai/kafka/pubsub"
	"github.com/hanzoai/kafka/types"
	psembed "github.com/hanzoai/pubsub/embed"
	"github.com/nats-io/nats.go"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// stack is one embedded pubsub, a broker over it, and a client connection of
// its own for reading and writing the store behind the broker's back.
type stack struct {
	ps   *psembed.Server
	b    *Broker
	addr string
	nc   *nats.Conn
	js   nats.JetStreamContext
}

func newStack(tb testing.TB) *stack {
	tb.Helper()
	ps, err := psembed.Open(psembed.Options{
		Host: "127.0.0.1", Port: -1, ServerName: "kafka-protocol", StoreDir: tb.TempDir(),
	})
	if err != nil {
		tb.Fatalf("pubsub open: %v", err)
	}
	tb.Cleanup(ps.Shutdown)

	s := &stack{ps: ps}
	s.b, s.addr = s.broker(tb)
	s.nc, err = nats.Connect(ps.ClientURL())
	if err != nil {
		tb.Fatalf("store connect: %v", err)
	}
	tb.Cleanup(s.nc.Close)
	if s.js, err = s.nc.JetStream(); err != nil {
		tb.Fatalf("store jetstream: %v", err)
	}
	return s
}

// broker starts a broker over the stack's pubsub and returns it with the
// address it listens on.
func (s *stack) broker(tb testing.TB) (*Broker, string) {
	tb.Helper()
	b := NewBroker(&types.Configuration{
		PubSubUrl:      s.ps.ClientURL(),
		BrokerHost:     "127.0.0.1",
		BrokerPort:     0,
		NodeID:         1,
		StreamReplicas: 1,
		StorageType:    "file",
	})
	errc := make(chan error, 1)
	go func() { errc <- b.Serve() }()
	deadline := time.Now().Add(5 * time.Second)
	for b.Addr() == nil {
		select {
		case err := <-errc:
			tb.Fatalf("broker serve: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			tb.Fatal("broker never bound")
		}
		time.Sleep(5 * time.Millisecond)
	}
	tb.Cleanup(b.Shutdown)
	return b, b.Addr().String()
}

func (s *stack) topic(tb testing.TB, name string, partitions uint32) {
	tb.Helper()
	r := pubsub.Retention{MaxAge: types.DefaultRetentionMaxAge, MaxBytes: types.DefaultRetentionMaxBytes}
	if err := s.b.PubSub.CreateTopicStreams(name, partitions, 1, nats.FileStorage, r); err != nil {
		tb.Fatalf("create topic %s: %v", name, err)
	}
}

// produce writes n one-record batches to a partition through the broker at
// addr, each acknowledged before the next is sent.
func produce(tb testing.TB, addr, topic string, partition int32, n int) {
	tb.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		tb.Fatalf("producer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := range n {
		r := &kgo.Record{Topic: topic, Partition: partition, Value: fmt.Appendf(nil, "record-%04d", i)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			tb.Fatalf("produce %s/%d #%d: %v", topic, partition, i, err)
		}
	}
}

// wire is one raw Kafka connection that sends a request and reads its
// response before sending the next, as a consumer's fetch connection does.
type wire struct {
	c   net.Conn
	f   *kmsg.RequestFormatter
	cid int32
}

func dial(tb testing.TB, addr string) *wire {
	tb.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		tb.Fatalf("dial %s: %v", addr, err)
	}
	tb.Cleanup(func() { c.Close() })
	return &wire{c: c, f: kmsg.NewRequestFormatter(kmsg.FormatterClientID("protocol-test"))}
}

// do sends req at version v and decodes the response at the same version.
func (w *wire) do(v int16, req kmsg.Request) (kmsg.Response, error) {
	req.SetVersion(v)
	w.cid++
	if _, err := w.c.Write(w.f.AppendRequest(nil, req, w.cid)); err != nil {
		return nil, err
	}
	var size [4]byte
	if _, err := io.ReadFull(w.c, size[:]); err != nil {
		return nil, err
	}
	body := make([]byte, binary.BigEndian.Uint32(size[:]))
	if _, err := io.ReadFull(w.c, body); err != nil {
		return nil, err
	}
	if len(body) < 4 {
		return nil, fmt.Errorf("response of %d bytes", len(body))
	}
	if cid := int32(binary.BigEndian.Uint32(body)); cid != w.cid {
		return nil, fmt.Errorf("correlation id %d, want %d", cid, w.cid)
	}
	body = body[4:]
	if req.IsFlexible() {
		// A flexible response header ends in a tagged-field section.
		n, k := binary.Uvarint(body)
		if k <= 0 || n != 0 {
			return nil, fmt.Errorf("response header tags: count %d (%d bytes)", n, k)
		}
		body = body[k:]
	}
	resp := req.ResponseKind()
	resp.SetVersion(v)
	return resp, resp.ReadFrom(body)
}

func (w *wire) fetch(v int16, req *kmsg.FetchRequest) (*kmsg.FetchResponse, error) {
	resp, err := w.do(v, req)
	if err != nil {
		return nil, err
	}
	return resp.(*kmsg.FetchResponse), nil
}

// at is one partition position in a fetch request.
type at struct {
	topic     string
	partition int32
	offset    int64
}

// fetchRequest builds a Fetch for the given positions the way consumers send
// one: no session, a 1 MiB partition limit, log start offset unknown (-1).
func fetchRequest(maxWaitMs, minBytes int32, positions ...at) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = maxWaitMs
	req.MinBytes = minBytes
	req.MaxBytes = 50 << 20
	for _, pos := range positions {
		if n := len(req.Topics); n == 0 || req.Topics[n-1].Topic != pos.topic {
			t := kmsg.NewFetchRequestTopic()
			t.Topic = pos.topic
			req.Topics = append(req.Topics, t)
		}
		p := kmsg.NewFetchRequestTopicPartition()
		p.Partition = pos.partition
		p.FetchOffset = pos.offset
		p.PartitionMaxBytes = 1 << 20
		t := &req.Topics[len(req.Topics)-1]
		t.Partitions = append(t.Partitions, p)
	}
	return req
}

// cpuTime is the CPU this process has used, user plus system.
func cpuTime(tb testing.TB) time.Duration {
	tb.Helper()
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		tb.Fatalf("getrusage: %v", err)
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}
