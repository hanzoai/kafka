package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/hanzoai/kafka/pubsub"
	"github.com/hanzoai/kafka/types"
	psembed "github.com/hanzoai/pubsub/embed"
	natsio "github.com/nats-io/nats.go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var defaultRetention = pubsub.Retention{
	MaxAge:   types.DefaultRetentionMaxAge,
	MaxBytes: types.DefaultRetentionMaxBytes,
}

// limits reads back what the store holds for a stream.
func limits(t *testing.T, js natsio.JetStreamContext, stream string) natsio.StreamConfig {
	t.Helper()
	info, err := js.StreamInfo(stream)
	if err != nil {
		t.Fatalf("stream info %s: %v", stream, err)
	}
	return info.Config
}

func wantLimits(t *testing.T, js natsio.JetStreamContext, stream string, age time.Duration, bytes int64) {
	t.Helper()
	c := limits(t, js, stream)
	if c.MaxAge != age || c.MaxBytes != bytes || c.Discard != natsio.DiscardOld {
		t.Errorf("%s: max age %s, max bytes %d, discard %s; want %s, %d, old",
			stream, c.MaxAge, c.MaxBytes, c.Discard, age, bytes)
	}
}

// TestTopicsAreBounded holds every partition stream to a retention limit: the
// ones a client creates, the ones Metadata auto-creates, and the ones left
// unbounded by a broker that predates retention, which the next boot bounds.
// A stream someone limited on purpose keeps its own limits.
func TestTopicsAreBounded(t *testing.T) {
	ps, err := psembed.Open(psembed.Options{
		Host: "127.0.0.1", Port: -1, ServerName: "kafka-retention", StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("pubsub open: %v", err)
	}
	t.Cleanup(ps.Shutdown)
	s := &stack{ps: ps}
	if s.nc, err = natsio.Connect(ps.ClientURL()); err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(s.nc.Close)
	if s.js, err = s.nc.JetStream(); err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}

	// What an older broker left behind: one partition with no limit at all,
	// and one an operator limited by hand.
	for _, c := range []*natsio.StreamConfig{
		{Name: pubsub.StreamName("legacy", 0), Subjects: []string{pubsub.SubjectName("legacy", 0)}},
		{Name: pubsub.StreamName("tuned", 0), Subjects: []string{pubsub.SubjectName("tuned", 0)}, MaxAge: time.Hour},
	} {
		if _, err := s.js.AddStream(c); err != nil {
			t.Fatalf("add %s: %v", c.Name, err)
		}
	}
	// Streams that are not partitions are not the broker's to touch.
	if _, err := s.js.AddStream(&natsio.StreamConfig{Name: "EVENT", Subjects: []string{"event.>"}}); err != nil {
		t.Fatalf("add EVENT: %v", err)
	}

	s.startBroker(t)

	wantLimits(t, s.js, pubsub.StreamName("legacy", 0), types.DefaultRetentionMaxAge, types.DefaultRetentionMaxBytes)
	if c := limits(t, s.js, pubsub.StreamName("tuned", 0)); c.MaxAge != time.Hour || c.MaxBytes != -1 {
		t.Errorf("tuned: max age %s, max bytes %d; want 1h0m0s, -1", c.MaxAge, c.MaxBytes)
	}
	if c := limits(t, s.js, "EVENT"); c.MaxAge != 0 || c.MaxBytes != -1 {
		t.Errorf("EVENT: max age %s, max bytes %d; want untouched", c.MaxAge, c.MaxBytes)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cl := s.client(t, kgo.AllowAutoTopicCreation())
	if _, err := kadm.NewClient(cl).CreateTopics(ctx, 2, 1, nil, "made"); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	for p := uint32(0); p < 2; p++ {
		wantLimits(t, s.js, pubsub.StreamName("made", p), types.DefaultRetentionMaxAge, types.DefaultRetentionMaxBytes)
	}
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "auto", Value: []byte("x")}).FirstErr(); err != nil {
		t.Fatalf("produce to auto-created topic: %v", err)
	}
	wantLimits(t, s.js, pubsub.StreamName("auto", 0), types.DefaultRetentionMaxAge, types.DefaultRetentionMaxBytes)
}

// TestRetentionIsConfigurable takes the broker's configured limits, and a
// negative value lifts the limit on that axis.
func TestRetentionIsConfigurable(t *testing.T) {
	s := &stack{}
	var err error
	if s.ps, err = psembed.Open(psembed.Options{
		Host: "127.0.0.1", Port: -1, ServerName: "kafka-retention-cfg", StoreDir: t.TempDir(),
	}); err != nil {
		t.Fatalf("pubsub open: %v", err)
	}
	t.Cleanup(s.ps.Shutdown)
	if s.nc, err = natsio.Connect(s.ps.ClientURL()); err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(s.nc.Close)
	if s.js, err = s.nc.JetStream(); err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	s.startBroker(t, func(c *types.Configuration) {
		c.RetentionMaxAge = time.Hour
		c.RetentionMaxBytes = -1
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := kadm.NewClient(s.client(t)).CreateTopics(ctx, 1, 1, nil, "hourly"); err != nil {
		t.Fatalf("create topics: %v", err)
	}
	wantLimits(t, s.js, pubsub.StreamName("hourly", 0), time.Hour, -1)
}
