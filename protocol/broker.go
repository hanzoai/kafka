package protocol

import (
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"time"

	log "github.com/hanzoai/kafka/logging"
	"github.com/hanzoai/kafka/pubsub"
	"github.com/hanzoai/kafka/serde"
	"github.com/hanzoai/kafka/types"
	"github.com/nats-io/nats.go"
)

// maxRequestSize bounds a single Kafka request frame (128 MiB).
const maxRequestSize = 128 << 20

// Broker represents a Hanzo Kafka broker instance
type Broker struct {
	Config         *types.Configuration
	PubSub         *pubsub.Client
	ShutDownSignal chan bool

	// listener is published by Serve on its own goroutine and read by callers
	// on theirs — Addr is how a caller learns the port the kernel picked, and
	// Shutdown closes it from wherever the shutdown came from. Nothing ordered
	// those three, so the bind raced every reader.
	listenerMu   sync.RWMutex
	listener     net.Listener
	partitionMu  sync.Map // map[string]*sync.Mutex keyed by "topic-partition"
	readHints    sync.Map // map[string]readHint keyed by "topic-partition"
	bounds       sync.Map // map[string]bounds keyed by partition stream name
	shutdownOnce sync.Once

	// waiters holds, per partition stream, the wake channel of every fetch
	// parked on it until data arrives or its wait runs out.
	waitMu  sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

// partitionLock returns a mutex for a topic+partition, ensuring safe concurrent offset assignment.
func (b *Broker) partitionLock(topic string, partition uint32) *sync.Mutex {
	key := fmt.Sprintf("%s-%d", topic, partition)
	v, _ := b.partitionMu.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// Log positions on a partition are read from the stored RecordBatch headers,
// never derived from PubSub sequences: Hanzo PubSub sequences are only
// monotonic, not dense (the production store allocates from a sparse e18
// space, and deletes leave holes), so sequence arithmetic addresses nothing.
// Messages that do not walk as valid batch chains are skipped everywhere —
// the stream subject is reachable by any PubSub client, and one raw publish
// served verbatim poisons every consumer that fetches it (the 2026-07
// insights outage: unfetchable records until the stream was purged).

// boundsScanLimit caps how many stored messages a poison-recovery scan will
// read before treating the partition as empty. Valid batches are found in one
// read; only a partition whose edges are wall-to-wall foreign messages pays
// more, and unbounded reads on the fetch path would let one bad stream stall
// the broker.
const boundsScanLimit = 10000

// bounds are a partition's Kafka log bounds together with the stream state
// they were read from. Stored messages never change and sequences never
// repeat within a stream, so the bounds hold for as long as the stream reports
// the same state: the same stream (Created) with the same first and last
// sequence and message count. Publish, purge, delete and retention each move
// at least one of those.
type bounds struct {
	created  time.Time
	firstSeq uint64
	lastSeq  uint64
	msgs     uint64
	logStart int64 // first offset of the first valid record set
	next     int64 // one past the last offset of the last valid record set
}

// of reports whether bd was read from the state info reports.
func (bd bounds) of(info *nats.StreamInfo) bool {
	st := info.State
	return bd.created.Equal(info.Created) && bd.firstSeq == st.FirstSeq &&
		bd.lastSeq == st.LastSeq && bd.msgs == st.Msgs
}

// partitionBounds derives the Kafka log bounds: logStart is the first offset
// of the first valid record set, next is one past the last offset of the last
// valid one (the high watermark, and the offset produce stamps next). It costs
// one StreamInfo; the stored messages are read only when the stream's state
// differs from the one the cached bounds were read from.
func (b *Broker) partitionBounds(topic string, partition uint32) (bounds, error) {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil {
		return bounds{}, err
	}
	key := pubsub.StreamName(topic, partition)
	if v, ok := b.bounds.Load(key); ok {
		if bd := v.(bounds); bd.of(info) {
			return bd, nil
		}
	}
	st := info.State
	bd := bounds{created: info.Created, firstSeq: st.FirstSeq, lastSeq: st.LastSeq, msgs: st.Msgs}
	if bd.logStart, bd.next, err = b.readBounds(topic, partition, st); err != nil {
		return bounds{}, err
	}
	b.bounds.Store(key, bd)
	return bd, nil
}

// readBounds reads the log bounds off a stream in state st. A read that fails
// is an error, never a guess: bounds the store did not answer for would send
// consumers to reset and have produce stamp offsets that already exist. A
// stream that changed after st was taken answers for its newer content, which
// is what any caller holding st would read a moment later.
func (b *Broker) readBounds(topic string, partition uint32, st nats.StreamState) (logStart, next int64, err error) {
	if st.Msgs == 0 {
		return 0, 0, nil
	}

	// Head: first valid record set at or after FirstSeq.
	var first int64
	ok := false
	for seq, i := st.FirstSeq, 0; !ok && i < boundsScanLimit && seq <= st.LastSeq; i++ {
		msg, err := b.PubSub.NextMessage(topic, partition, seq)
		if err != nil {
			return 0, 0, err
		}
		if msg == nil {
			break
		}
		first, _, ok = batchSpan(msg.Data)
		seq = msg.Sequence + 1
	}
	if !ok {
		return 0, 0, nil // nothing but foreign messages: an empty log
	}

	// Tail: the last stored message, asked for by subject, since LastSeq can
	// name a message deleted after it was stored. If it is foreign, fall back
	// to a bounded forward scan for the last valid record set.
	tail, err := b.PubSub.LastMessage(topic, partition)
	if err != nil {
		return 0, 0, err
	}
	if tail == nil {
		return 0, 0, nil
	}
	if _, last, ok := batchSpan(tail.Data); ok {
		return first, last + 1, nil
	}
	log.Warn("partition %s/%d tail is not a record batch; scanning", topic, partition)
	next = first
	for seq, i := st.FirstSeq, 0; i < boundsScanLimit && seq <= tail.Sequence; i++ {
		msg, err := b.PubSub.NextMessage(topic, partition, seq)
		if err != nil {
			return 0, 0, err
		}
		if msg == nil {
			break
		}
		if _, last, ok := batchSpan(msg.Data); ok {
			next = last + 1
		}
		seq = msg.Sequence + 1
	}
	return first, next, nil
}

// readHint remembers, per partition, where the last served fetch left off, so
// a sequential consumer costs one addressed read per fetch instead of a
// search.
type readHint struct {
	offset int64  // next Kafka offset the consumer will ask for
	seq    uint64 // first sequence that can hold it
}

// findRecordSet returns the stored record set whose span contains offset, or
// the first valid one past it (sequence holes and skipped foreign messages
// leave gaps in the offset space), searching the sequence range bd was read
// from. Returns nil when nothing at or past offset exists. Callers have
// already bounds-checked offset against bd, so nil means the data the bounds
// promised could not be read — serve empty and let the client retry, never
// serve bytes that did not walk.
func (b *Broker) findRecordSet(topic string, partition uint32, offset int64, bd bounds) *pubsub.StoredMsg {
	if bd.msgs == 0 {
		return nil
	}

	key := fmt.Sprintf("%s-%d", topic, partition)
	if v, ok := b.readHints.Load(key); ok {
		h := v.(readHint)
		if h.offset == offset {
			if msg := b.probeFrom(topic, partition, h.seq, offset); msg != nil {
				b.rememberHint(key, offset, msg)
				return msg
			}
		}
	}

	// Binary search over the sequence space. A probe at mid returns the first
	// stored valid record set at or after mid together with its real sequence,
	// so sparse sequences and holes still halve the interval each round.
	lo, hi := bd.firstSeq, bd.lastSeq
	for lo <= hi {
		mid := lo + (hi-lo)/2
		msg := b.probeFrom(topic, partition, mid, 0)
		if msg == nil {
			// Nothing valid at or after mid: the target, if any, is below.
			if mid == 0 {
				break
			}
			hi = mid - 1
			continue
		}
		f, l, _ := batchSpan(msg.Data)
		switch {
		case offset >= f && offset <= l:
			b.rememberHint(key, offset, msg)
			return msg
		case offset < f:
			if msg.Sequence <= lo {
				// Everything from lo on starts past offset: this is the first
				// record set after the gap that swallowed it.
				b.rememberHint(key, offset, msg)
				return msg
			}
			hi = min(mid-1, msg.Sequence-1)
		default: // offset > l
			lo = msg.Sequence + 1
		}
	}
	return nil
}

// probeFrom returns the first valid record set at or after seq, skipping up to
// boundsScanLimit foreign messages. wantOffset is a fast-path hint: when the
// exact sequence holds the batch (the dense steady state), one direct read
// answers without a subscription.
func (b *Broker) probeFrom(topic string, partition uint32, seq uint64, wantOffset int64) *pubsub.StoredMsg {
	if raw, err := b.PubSub.GetMessage(topic, partition, seq); err == nil {
		if f, l, ok := batchSpan(raw.Data); ok && (wantOffset == 0 || (wantOffset >= f && wantOffset <= l)) {
			return &pubsub.StoredMsg{Sequence: seq, Data: raw.Data}
		}
	}
	for i := 0; i < boundsScanLimit; i++ {
		msg, err := b.PubSub.NextMessage(topic, partition, seq)
		if err != nil || msg == nil {
			return nil
		}
		if _, _, ok := batchSpan(msg.Data); ok {
			return msg
		}
		seq = msg.Sequence + 1
	}
	return nil
}

func (b *Broker) rememberHint(key string, offset int64, msg *pubsub.StoredMsg) {
	_, last, ok := batchSpan(msg.Data)
	if !ok {
		return
	}
	b.readHints.Store(key, readHint{offset: last + 1, seq: msg.Sequence + 1})
}

// NewBroker creates a new Broker instance with the provided configuration
func NewBroker(config *types.Configuration) *Broker {
	return &Broker{
		Config:         config,
		ShutDownSignal: make(chan bool),
		waiters:        make(map[string]map[chan struct{}]struct{}),
	}
}

// park registers wake to be signalled when any of streams gets a new tail,
// and returns the call that removes it again.
func (b *Broker) park(streams []string, wake chan struct{}) (unpark func()) {
	b.waitMu.Lock()
	for _, s := range streams {
		set := b.waiters[s]
		if set == nil {
			set = make(map[chan struct{}]struct{})
			b.waiters[s] = set
		}
		set[wake] = struct{}{}
	}
	b.waitMu.Unlock()
	return func() {
		b.waitMu.Lock()
		for _, s := range streams {
			delete(b.waiters[s], wake)
			if len(b.waiters[s]) == 0 {
				delete(b.waiters, s)
			}
		}
		b.waitMu.Unlock()
	}
}

// wake signals every fetch parked on stream. A wake channel holds one signal,
// so one that fires while its fetch is still reading is not lost.
func (b *Broker) wake(stream string) {
	b.waitMu.Lock()
	for ch := range b.waiters[stream] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	b.waitMu.Unlock()
}

// retention resolves the configured partition limits: zero takes the default,
// a negative value means no limit on that axis.
func (b *Broker) retention() pubsub.Retention {
	r := pubsub.Retention{MaxAge: b.Config.RetentionMaxAge, MaxBytes: b.Config.RetentionMaxBytes}
	switch {
	case r.MaxAge == 0:
		r.MaxAge = types.DefaultRetentionMaxAge
	case r.MaxAge < 0:
		r.MaxAge = 0
	}
	switch {
	case r.MaxBytes == 0:
		r.MaxBytes = types.DefaultRetentionMaxBytes
	case r.MaxBytes < 0:
		r.MaxBytes = -1
	}
	return r
}

// Startup initializes the broker and blocks serving Kafka clients. It is the
// standalone entrypoint (main.go): any startup failure is fatal.
func (b *Broker) Startup() {
	if err := b.Serve(); err != nil {
		log.Panic("Hanzo Kafka failed: %v", err)
	}
}

// Serve is the embed-safe form of Startup: it connects to PubSub, starts the
// admin server, and runs the Kafka accept loop, RETURNING errors instead of
// exiting the process so a host binary (hanzoai/cloud) can run the adaptor
// in-process. It stores the listener so Shutdown can stop it; a clean Shutdown
// closes ShutDownSignal + the listener, so Accept fails and Serve returns nil.
// Run it in a goroutine when embedding.
func (b *Broker) Serve() error {
	var err error

	b.PubSub, err = pubsub.NewClient(b.Config.PubSubUrl)
	if err != nil {
		return fmt.Errorf("connect pubsub: %w", err)
	}

	if err = b.PubSub.EnsureOffsetBucket(); err != nil {
		return fmt.Errorf("ensure offset bucket: %w", err)
	}

	// A fetch with nothing to read parks until a partition it asked for gets a
	// new tail. Every broker on the bus announces its appends, so a fetch
	// served here wakes for a produce served by any of them.
	if err = b.PubSub.WatchTails(b.wake); err != nil {
		return fmt.Errorf("watch partition tails: %w", err)
	}

	// Partitions created before the broker bounded them grow for as long as
	// the store lives. One that cannot be bounded still serves, so this logs
	// rather than refusing to start.
	r := b.retention()
	n, err := b.PubSub.BoundTopicStreams(r)
	if n > 0 {
		log.Info("Bounded %d topic streams to max age %s, max bytes %d", n, r.MaxAge, r.MaxBytes)
	}
	if err != nil {
		log.Error("Bound topic streams: %v", err)
	}

	b.StartAdmin()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Config.BrokerPort))
	if err != nil {
		return fmt.Errorf("listen :%d: %w", b.Config.BrokerPort, err)
	}
	// Port 0 asked the kernel to pick: settle the real port BEFORE publishing
	// the listener, so a caller that can see the listener can also see the port
	// Metadata and FindCoordinator will advertise.
	if b.Config.BrokerPort == 0 {
		b.Config.BrokerPort = ln.Addr().(*net.TCPAddr).Port
	}
	b.listenerMu.Lock()
	b.listener = ln
	b.listenerMu.Unlock()

	log.Info("Hanzo Kafka listening on port %d (PubSub: %s)", b.Config.BrokerPort, b.Config.PubSubUrl)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.ShutDownSignal:
				return nil
			default:
				log.Error("Error accepting connection: %v", err)
				continue
			}
		}
		go b.HandleConnection(conn)
	}
}

// HandleConnection processes incoming requests from a client connection
func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	connectionAddr := conn.RemoteAddr().String()
	log.Info("Connection established with %s", connectionAddr)

	for {
		startTime := time.Now()
		lengthBuffer := make([]byte, 4)
		_, err := io.ReadFull(conn, lengthBuffer)
		if err != nil {
			log.Info("failed to read request's length. Error: %v ", err)
			return
		}
		length := serde.Encoding.Uint32(lengthBuffer)
		if length > maxRequestSize {
			// A framed length this large is not Kafka — it is a stray protocol
			// (an HTTP probe reads as a ~1GB frame) or garbage. Allocating it
			// would let any port scan OOM the broker.
			log.Error("request frame of %d bytes from %s exceeds %d; closing", length, connectionAddr, maxRequestSize)
			return
		}
		buffer := make([]byte, length+4)
		copy(buffer, lengthBuffer)
		_, err = io.ReadFull(conn, buffer[4:])
		if err != nil {
			if err.Error() != "EOF" {
				log.Error("Error reading from connection: %v", err)
			}
			break
		}
		req := serde.ParseHeader(buffer, connectionAddr)
		apiKeyHandler := b.APIDispatcher(req.RequestAPIKey)
		// A frame that arrived is a trace, not news. This used to name three APIs
		// as the chatty ones and log the rest at Info, which is a guess about
		// which calls a client repeats — and the guess was wrong about the one
		// that matters. Measured in production: Metadata was 824 of 1200 lines,
		// 69% of everything the process said, drowning the boot errors that
		// explain an outage inside eight minutes of a pod's history.
		//
		// Info is for what an operator did not already know: the broker came up,
		// a client went away, a handler panicked. Those lines are Error and
		// lifecycle and they stay. Every received frame is Debug, so there is no
		// list to keep correct as APIs are added.
		log.Debug("Received %v v%d corr=%d from %s len=%d body=%d", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, connectionAddr, length, len(req.Body))

		response, handlerErr := b.safeHandle(apiKeyHandler, req)
		if handlerErr != nil {
			log.Error("Panic in handler %v (apiKey=%d, version=%d): %v", apiKeyHandler.Name, req.RequestAPIKey, req.RequestAPIVersion, handlerErr)
			break
		}

		log.Debug("Response %v v%d corr=%d len=%d", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, len(response))
		_, err = conn.Write(response)
		if err != nil {
			log.Error("Error writing to connection: %v", err)
			break
		}
		d := time.Since(startTime)
		log.Trace("handleConnection Iteration took %v", d)
	}
	log.Info("Connection with %s closed.", connectionAddr)
}

// safeHandle calls the API handler with panic recovery so a single bad request doesn't crash the process.
func (b *Broker) safeHandle(h APIKeyHandler, req types.Request) (response []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v\n%s", r, debug.Stack())
		}
	}()
	return h.Handler(req), nil
}

// Addr returns the listener address once Serve has bound it, else nil.
func (b *Broker) Addr() net.Addr {
	b.listenerMu.RLock()
	defer b.listenerMu.RUnlock()
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// Shutdown gracefully shuts down the broker. Idempotent: hosts and tests may
// tear down on overlapping paths, and a second call must be a no-op, not a
// closed-channel panic.
func (b *Broker) Shutdown() {
	b.shutdownOnce.Do(func() {
		close(b.ShutDownSignal)
		b.listenerMu.RLock()
		ln := b.listener
		b.listenerMu.RUnlock()
		if ln != nil {
			ln.Close()
		}
		if b.PubSub != nil {
			b.PubSub.Close()
		}
		log.Info("Hanzo Kafka shut down")
	})
}
