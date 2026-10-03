package protocol

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/hanzoai/kafka/types"
	psembed "github.com/hanzoai/pubsub/embed"
	"github.com/twmb/franz-go/pkg/kgo"
)

// door is a TCP port that relays every connection to a unix socket, which is
// what a host that owns the Kafka port does in front of the broker.
func door(tb testing.TB, sock string) (addr string) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("door listen: %v", err)
	}
	tb.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				u, err := net.Dial("unix", sock)
				if err != nil {
					return
				}
				defer u.Close()
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(u, c); done <- struct{}{} }()
				go func() { _, _ = io.Copy(c, u); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return ln.Addr().String()
}

// A broker serving a unix socket behind a TCP door answers clients that know
// only the door: Metadata advertises Config's port, not the socket.
func TestAcceptServesAUnixSocketBehindATCPDoor(t *testing.T) {
	ps, err := psembed.Open(psembed.Options{
		Host: "127.0.0.1", Port: -1, ServerName: "kafka-accept", StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("pubsub open: %v", err)
	}
	t.Cleanup(ps.Shutdown)

	sock := filepath.Join(t.TempDir(), "k.wire")
	addr := door(t, sock)
	_, port, _ := net.SplitHostPort(addr)
	var p int
	fmt.Sscan(port, &p)

	b := NewBroker(&types.Configuration{
		PubSubUrl:      ps.ClientURL(),
		BrokerHost:     "127.0.0.1",
		BrokerPort:     p,
		NodeID:         1,
		StreamReplicas: 1,
		StorageType:    "file",
	})
	if err := b.Connect(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- b.Accept(ln) }()

	const topic = "behind-the-door"
	s := &stack{b: b}
	s.topic(t, topic, 1)
	produce(t, addr, topic, 0, 3)

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got []string
	for len(got) < 3 {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("consumed %d of 3 records before the deadline: %v", len(got), got)
		}
		fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	for i, v := range got {
		if want := fmt.Sprintf("record-%04d", i); v != want {
			t.Fatalf("record %d = %q, want %q", i, v, want)
		}
	}

	b.Shutdown()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Accept after Shutdown = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not return after Shutdown")
	}
}

// A Shutdown that lands before Accept has published its listener must still
// stop it, or Accept blocks forever on a socket nothing will close.
func TestAcceptAfterShutdownReturns(t *testing.T) {
	b := NewBroker(&types.Configuration{BrokerHost: "127.0.0.1"})
	b.Shutdown()
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "k.wire"))
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- b.Accept(ln) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Accept after Shutdown = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept blocked after Shutdown")
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("listener still open after Accept returned: %v", err)
	}
}

// A listener closed by someone other than Shutdown can never accept again, so
// Accept returns the error instead of spinning on it.
func TestAcceptReturnsWhenItsListenerCloses(t *testing.T) {
	b := NewBroker(&types.Configuration{BrokerHost: "127.0.0.1"})
	t.Cleanup(b.Shutdown)
	ln, err := net.Listen("unix", filepath.Join(t.TempDir(), "k.wire"))
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- b.Accept(ln) }()
	time.Sleep(50 * time.Millisecond)
	ln.Close()
	select {
	case err := <-done:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("Accept on a closed listener = %v, want net.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept kept running on a closed listener")
	}
}
