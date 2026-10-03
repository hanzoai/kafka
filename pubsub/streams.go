package pubsub

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/hanzoai/kafka/logging"
	"github.com/nats-io/nats.go"
)

// StreamName returns the Hanzo Kafka name for a topic+partition
func StreamName(topic string, partition uint32) string {
	return fmt.Sprintf("kafka-%s-%d", topic, partition)
}

// SubjectName returns the PubSub subject for a topic+partition.
// Must match the subject pattern configured on the JetStream stream:
// StreamName(topic, partition) + ".data"
func SubjectName(topic string, partition uint32) string {
	return fmt.Sprintf("%s.data", StreamName(topic, partition))
}

// ParseStreamName extracts topic and partition from a stream name
func ParseStreamName(name string) (topic string, partition uint32, ok bool) {
	if !strings.HasPrefix(name, "kafka-") {
		return "", 0, false
	}
	rest := name[6:]
	lastDash := strings.LastIndex(rest, "-")
	if lastDash == -1 {
		return "", 0, false
	}
	topic = rest[:lastDash]
	_, err := fmt.Sscanf(rest[lastDash+1:], "%d", &partition)
	return topic, partition, err == nil
}

// Retention bounds a partition stream: JetStream drops the oldest messages
// once they are older than MaxAge or the stream holds more than MaxBytes. A
// zero MaxAge and a MaxBytes of -1 are JetStream's spelling of no limit.
type Retention struct {
	MaxAge   time.Duration
	MaxBytes int64
}

// bounded reports whether r limits anything.
func (r Retention) bounded() bool {
	return r.MaxAge > 0 || r.MaxBytes > 0
}

// CreateTopicStreams creates N Hanzo Kafka streams for a topic (one per
// partition), each bounded by r.
func (c *Client) CreateTopicStreams(topic string, numPartitions uint32, replicas int, storage nats.StorageType, r Retention) error {
	if replicas < 1 {
		replicas = 1
	}
	for i := uint32(0); i < numPartitions; i++ {
		cfg := &nats.StreamConfig{
			Name:     StreamName(topic, i),
			Subjects: []string{SubjectName(topic, i)},
			Replicas: replicas,
			Storage:  storage,
			MaxAge:   r.MaxAge,
			MaxBytes: r.MaxBytes,
			Discard:  nats.DiscardOld,
		}
		_, err := c.JS.AddStream(cfg)
		if err != nil {
			return fmt.Errorf("failed to create stream for %s partition %d: %w", topic, i, err)
		}
		log.Info("Created stream %s", cfg.Name)
	}
	return nil
}

// BoundTopicStreams applies r to every partition stream that has no limit at
// all — streams created before the broker bounded them, which otherwise grow
// for as long as the store lives. A stream carrying any limit was set that way
// on purpose and is left alone. It returns how many streams it bounded, and
// keeps going past a stream it cannot update: that stream still serves, as
// unbounded as before.
func (c *Client) BoundTopicStreams(r Retention) (int, error) {
	if !r.bounded() {
		return 0, nil
	}
	var n int
	var errs []error
	for name := range c.JS.StreamNames() {
		if _, _, ok := ParseStreamName(name); !ok {
			continue
		}
		info, err := c.JS.StreamInfo(name)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		cfg := info.Config
		if cfg.MaxAge > 0 || cfg.MaxBytes > 0 || cfg.MaxMsgs > 0 {
			continue
		}
		cfg.MaxAge = r.MaxAge
		cfg.MaxBytes = r.MaxBytes
		cfg.Discard = nats.DiscardOld
		if _, err := c.JS.UpdateStream(&cfg); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		n++
	}
	return n, errors.Join(errs...)
}

// TopicExists checks if at least partition 0 stream exists for this topic
func (c *Client) TopicExists(topic string) bool {
	_, err := c.JS.StreamInfo(StreamName(topic, 0))
	return err == nil
}

// GetTopicPartitionCount counts partition streams for a topic
func (c *Client) GetTopicPartitionCount(topic string) (uint32, error) {
	var count uint32
	for {
		_, err := c.JS.StreamInfo(StreamName(topic, count))
		if err != nil {
			break
		}
		count++
	}
	return count, nil
}

// GetStreamInfo returns Hanzo Kafka info for a topic+partition
func (c *Client) GetStreamInfo(topic string, partition uint32) (*nats.StreamInfo, error) {
	return c.JS.StreamInfo(StreamName(topic, partition))
}

// Publish publishes record batch bytes to a partition, returns sequence (= Kafka offset + 1)
func (c *Client) Publish(topic string, partition uint32, data []byte) (uint64, error) {
	ack, err := c.JS.Publish(SubjectName(topic, partition), data)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

// GetMessage retrieves a message by stream sequence number
func (c *Client) GetMessage(topic string, partition uint32, sequence uint64) (*nats.RawStreamMsg, error) {
	return c.JS.GetMsg(StreamName(topic, partition), sequence)
}

// StoredMsg is a stored partition message with its stream sequence.
type StoredMsg struct {
	Sequence uint64
	Data     []byte
}

// LastMessage returns the last message stored on a partition, or nil if it
// holds none. It asks by subject: a stream's LastSeq keeps naming a message
// that was deleted after it was stored.
func (c *Client) LastMessage(topic string, partition uint32) (*StoredMsg, error) {
	msg, err := c.JS.GetLastMsg(StreamName(topic, partition), SubjectName(topic, partition))
	if errors.Is(err, nats.ErrMsgNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &StoredMsg{Sequence: msg.Sequence, Data: msg.Data}, nil
}

// NextMessage returns the first stored message with sequence >= seq, or nil if
// the partition holds none at or past it. This — not sequence arithmetic — is
// the primitive for reading a partition: Hanzo PubSub assigns sequences that
// are only guaranteed monotonic, not dense (deletes leave holes and the
// production store allocates from a sparse space), so "seq+1" addresses
// nothing. One message-get request with next_by_subj asks the store itself for
// the next real message; the partition stream carries exactly one subject, so
// filtering by it skips nothing. The JetStream context only sends
// next_by_subj on the direct-get path, which these streams do not enable, so
// the request is made here.
func (c *Client) NextMessage(topic string, partition uint32, seq uint64) (*StoredMsg, error) {
	if seq < 1 {
		seq = 1
	}
	req, err := json.Marshal(msgGetRequest{Seq: seq, NextFor: SubjectName(topic, partition)})
	if err != nil {
		return nil, err
	}
	reply, err := c.NC.Request(msgGetSubject+StreamName(topic, partition), req, apiTimeout)
	if err != nil {
		return nil, err
	}
	var resp msgGetResponse
	if err := json.Unmarshal(reply.Data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if errors.Is(resp.Error, nats.ErrMsgNotFound) {
			return nil, nil // nothing at or past seq
		}
		return nil, resp.Error
	}
	if resp.Message == nil {
		return nil, errors.New("message get: empty response")
	}
	return &StoredMsg{Sequence: resp.Message.Sequence, Data: resp.Message.Data}, nil
}

// msgGetSubject is the JetStream message-get API, suffixed by stream name.
const msgGetSubject = "$JS.API.STREAM.MSG.GET."

// apiTimeout matches the JetStream context's default API wait.
const apiTimeout = 5 * time.Second

// msgGetRequest asks for the first stored message at or after Seq whose
// subject matches NextFor.
type msgGetRequest struct {
	Seq     uint64 `json:"seq"`
	NextFor string `json:"next_by_subj"`
}

type msgGetResponse struct {
	Message *struct {
		Sequence uint64 `json:"seq"`
		Data     []byte `json:"data"`
	} `json:"message"`
	Error *nats.APIError `json:"error"`
}

// tailPrefix prefixes the core subject a broker announces a partition's new
// tail on once the store has acknowledged an append. It lies outside every
// partition stream's subject, so no stream stores the announcement.
const tailPrefix = "_kafka.tail."

// AnnounceTail tells every broker on the bus that a partition's stream has a
// new tail. The announcement carries no payload: a listener rereads the stream.
func (c *Client) AnnounceTail(topic string, partition uint32) error {
	return c.NC.Publish(tailPrefix+StreamName(topic, partition), nil)
}

// WatchTails calls fn with the stream name of every tail any broker on the bus
// announces. fn runs on the subscription's goroutine and must not block.
func (c *Client) WatchTails(fn func(stream string)) error {
	_, err := c.NC.Subscribe(tailPrefix+">", func(m *nats.Msg) {
		fn(strings.TrimPrefix(m.Subject, tailPrefix))
	})
	return err
}

// ListTopics returns all unique topic names from kafka-* streams
func (c *Client) ListTopics() ([]string, error) {
	topicSet := make(map[string]bool)
	for name := range c.JS.StreamNames() {
		topic, _, ok := ParseStreamName(name)
		if ok {
			topicSet[topic] = true
		}
	}
	var topics []string
	for t := range topicSet {
		topics = append(topics, t)
	}
	return topics, nil
}
