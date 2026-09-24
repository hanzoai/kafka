package types

import "time"

// Partition retention defaults. A topic partition is one JetStream stream, and
// every stream draws on one store shared with the rest of the bus.
const (
	// DefaultRetentionMaxAge is Kafka's own log.retention.hours (168).
	DefaultRetentionMaxAge = 7 * 24 * time.Hour

	// DefaultRetentionMaxBytes caps one partition. JetStream reserves a
	// stream's MaxBytes against the server's store limit (75% of the disk
	// free at boot, unless set) the moment the stream is created, and refuses
	// the stream once the reservations would exceed it. The cap is sized for
	// dozens of topics sharing a small store, not for one large topic.
	DefaultRetentionMaxBytes = 256 << 20
)

// Configuration represents the Hanzo Kafka broker configuration
type Configuration struct {
	// Hanzo PubSub connection
	PubSubUrl      string // PubSub server URL (e.g. "nats://localhost:4222")
	PubSubCredFile string // Optional PubSub credentials file

	// Kafka listener
	BrokerHost string `kafka:"CompactString"`
	BrokerPort int

	// Hanzo Kafka defaults
	StreamReplicas int    // Number of replicas for Hanzo Kafka (default 1)
	StorageType    string // "file" or "memory" (default "file")

	// Partition retention: a partition drops its oldest records once they
	// are older than RetentionMaxAge or it holds more than RetentionMaxBytes.
	// Zero takes the default above; a negative value means no limit.
	RetentionMaxAge   time.Duration
	RetentionMaxBytes int64

	// Admin HTTP server
	AdminPort int // HTTP admin/monitoring port (default 9093, 0 to disable)

	// Broker identity
	NodeID int
}
