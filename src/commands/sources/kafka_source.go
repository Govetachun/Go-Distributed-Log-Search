package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"toshokan/src/database"

	"github.com/sirupsen/logrus"
)

// KafkaPrefix is the URL prefix for Kafka sources
// Equivalent to KAFKA_PREFIX constant in Rust
const KafkaPrefix = "kafka://"

const (
	consumerThreadMessagesChannelSize = 10
	pollDuration                      = time.Second
)

// MessageFromConsumerThread represents messages from the consumer thread
// Equivalent to MessageFromConsumerThread enum in Rust
type MessageFromConsumerThread struct {
	Type          MessageType
	Payload       *PayloadMessage
	PostRebalance *PostRebalanceMessage
}

type MessageType string

const (
	MessageTypePayload       MessageType = "payload"
	MessageTypeEOF           MessageType = "eof"
	MessageTypePreRebalance  MessageType = "pre_rebalance"
	MessageTypePostRebalance MessageType = "post_rebalance"
)

// PayloadMessage represents a payload message
type PayloadMessage struct {
	Bytes     []byte
	Partition int32
	Offset    int64
}

// PostRebalanceMessage represents a post-rebalance message
type PostRebalanceMessage struct {
	Partitions   []int32
	ResponseChan chan []PartitionOffsetWithOptional
}

// KafkaSource represents a Kafka data source
// Equivalent to KafkaSource struct in Rust
type KafkaSource struct {
	messagesChan      chan *MessageFromConsumerThread
	checkpoint        *KafkaCheckpoint
	partitionToOffset map[int32]int64
	mutex             sync.RWMutex

	// Test tracking fields (equivalent to cfg(feature = "in-tests") fields)
	savedPartitionsAndOffsets  []PartitionOffset
	loadedPartitionsAndOffsets []PartitionOffsetWithOptional
}

// ParseKafkaURL parses a Kafka URL into servers and topic
// Equivalent to parse_url function in Rust
func ParseKafkaURL(url string) (string, string, error) {
	if !strings.HasPrefix(url, KafkaPrefix) {
		return "", "", fmt.Errorf("'%s' does not start with %s", url, KafkaPrefix)
	}

	trimmedInput := url[len(KafkaPrefix):]
	parts := strings.SplitN(trimmedInput, "/", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("'%s' needs to include a '/' to include the topic name", url)
	}

	return parts[0], parts[1], nil
}

// NewKafkaSourceFromURL creates a new KafkaSource from a URL
// Equivalent to KafkaSource::from_url in Rust
func NewKafkaSourceFromURL(ctx context.Context, url string, stream bool, db database.DBAdapter) (*KafkaSource, error) {
	servers, topic, err := ParseKafkaURL(url)
	if err != nil {
		return nil, err
	}

	messagesChan := make(chan *MessageFromConsumerThread, consumerThreadMessagesChannelSize)

	// Create mock Kafka consumer for now
	// In a real implementation, this would use a Kafka library like Sarama or Segmentio
	consumer := &MockKafkaConsumer{
		servers:      servers,
		topic:        topic,
		stream:       stream,
		messagesChan: messagesChan,
	}

	logrus.Debugf("Reading from kafka '%s' (topic '%s')", servers, topic)

	// Start consumer in background
	go consumer.Run(ctx)

	var checkpoint *KafkaCheckpoint
	if stream {
		// URL is not a good identifier as a source id, but we'll live with it for now
		checkpoint = NewKafkaCheckpoint(url, db)
	}

	source := &KafkaSource{
		messagesChan:      messagesChan,
		checkpoint:        checkpoint,
		partitionToOffset: make(map[int32]int64),
	}

	// Wait for initial assignment
	err = source.waitForAssignment(ctx)
	if err != nil {
		return nil, fmt.Errorf("first message got is not an assignment message: %w", err)
	}

	return source, nil
}

// waitForAssignment waits for the initial partition assignment
// Equivalent to wait_for_assignment method in Rust
func (ks *KafkaSource) waitForAssignment(ctx context.Context) error {
	select {
	case msg := <-ks.messagesChan:
		if msg == nil {
			return fmt.Errorf("kafka consumer thread closed")
		}
		if msg.Type != MessageTypePostRebalance {
			return fmt.Errorf("got a non assignment message")
		}
		return ks.handlePostRebalanceMsg(ctx, msg.PostRebalance)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// handlePostRebalanceMsg handles post-rebalance messages
// Equivalent to handle_post_rebalance_msg method in Rust
func (ks *KafkaSource) handlePostRebalanceMsg(ctx context.Context, msg *PostRebalanceMessage) error {
	if ks.checkpoint == nil {
		// Send empty response and return
		select {
		case msg.ResponseChan <- []PartitionOffsetWithOptional{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}

	partitionsAndOffsets, err := ks.checkpoint.Load(ctx, msg.Partitions)
	if err != nil {
		return fmt.Errorf("failed to load checkpoint: %w", err)
	}

	ks.trackLoadedCheckpoint(partitionsAndOffsets)

	// Send response
	select {
	case msg.ResponseChan <- partitionsAndOffsets:
	case <-ctx.Done():
		return ctx.Err()
	}

	ks.mutex.Lock()
	ks.partitionToOffset = make(map[int32]int64)
	ks.mutex.Unlock()

	return nil
}

// trackSavedCheckpoint tracks saved checkpoints for testing
// Equivalent to track_saved_checkpoint method in Rust
func (ks *KafkaSource) trackSavedCheckpoint(partitionsAndOffsets []PartitionOffset) {
	// Implementation would be conditional based on build tags in Go
	ks.savedPartitionsAndOffsets = append([]PartitionOffset{}, partitionsAndOffsets...)
}

// trackLoadedCheckpoint tracks loaded checkpoints for testing
// Equivalent to track_loaded_checkpoint method in Rust
func (ks *KafkaSource) trackLoadedCheckpoint(partitionsAndOffsets []PartitionOffsetWithOptional) {
	// Implementation would be conditional based on build tags in Go
	ks.loadedPartitionsAndOffsets = append([]PartitionOffsetWithOptional{}, partitionsAndOffsets...)
}

// GetOne implements Source interface
// Equivalent to Source::get_one implementation for KafkaSource in Rust
func (ks *KafkaSource) GetOne(ctx context.Context) (*SourceItem, error) {
	for {
		select {
		case msg := <-ks.messagesChan:
			if msg == nil {
				return nil, fmt.Errorf("kafka consumer thread closed")
			}

			switch msg.Type {
			case MessageTypePayload:
				ks.mutex.Lock()
				ks.partitionToOffset[msg.Payload.Partition] = msg.Payload.Offset
				ks.mutex.Unlock()

				var jsonMap JsonMap
				if err := json.Unmarshal(msg.Payload.Bytes, &jsonMap); err != nil {
					return nil, fmt.Errorf("failed to parse JSON from Kafka message: %w", err)
				}

				return &SourceItem{
					Type:     SourceItemTypeDocument,
					Document: jsonMap,
				}, nil

			case MessageTypeEOF:
				return &SourceItem{Type: SourceItemTypeClose}, nil

			case MessageTypePreRebalance:
				return &SourceItem{Type: SourceItemTypeRestart}, nil

			case MessageTypePostRebalance:
				err := ks.handlePostRebalanceMsg(ctx, msg.PostRebalance)
				if err != nil {
					return nil, err
				}
				// Continue to next iteration after handling rebalance
			}

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// GetCheckpointCommitter implements Source interface
// Equivalent to Source::get_checkpoint_commiter implementation for KafkaSource in Rust
func (ks *KafkaSource) GetCheckpointCommitter(ctx context.Context) (CheckpointCommitter, error) {
	if ks.checkpoint == nil {
		return nil, nil
	}

	ks.mutex.RLock()
	defer ks.mutex.RUnlock()

	// Convert map to sorted slice and add 1 to offset
	// (we don't want to seek to the last record already read, but the next)
	var partitions []int32
	for partition := range ks.partitionToOffset {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})

	flat := make([]PartitionOffset, len(partitions))
	for i, partition := range partitions {
		flat[i] = PartitionOffset{
			Partition: partition,
			Offset:    ks.partitionToOffset[partition] + 1,
		}
	}

	ks.trackSavedCheckpoint(flat)

	// Clear the map since we're creating a checkpoint
	ks.partitionToOffset = make(map[int32]int64)

	return ks.checkpoint.Committer(flat), nil
}

// Close implements Source interface
func (ks *KafkaSource) Close() error {
	// Close the messages channel to signal shutdown
	if ks.messagesChan != nil {
		close(ks.messagesChan)
	}
	return nil
}

// MockKafkaConsumer is a mock implementation for demonstration
// In a real implementation, this would use a proper Kafka library
type MockKafkaConsumer struct {
	servers      string
	topic        string
	stream       bool
	messagesChan chan *MessageFromConsumerThread
}

// Run simulates running the Kafka consumer
func (mkc *MockKafkaConsumer) Run(ctx context.Context) {
	defer close(mkc.messagesChan)

	// Send initial assignment
	responseChan := make(chan []PartitionOffsetWithOptional, 1)
	mkc.messagesChan <- &MessageFromConsumerThread{
		Type: MessageTypePostRebalance,
		PostRebalance: &PostRebalanceMessage{
			Partitions:   []int32{0}, // Mock single partition
			ResponseChan: responseChan,
		},
	}

	// Wait for response
	select {
	case <-responseChan:
	case <-ctx.Done():
		return
	}

	// Simulate sending some messages
	for i := 0; i < 5; i++ {
		mockDoc := map[string]interface{}{
			"id":      strconv.Itoa(i),
			"message": fmt.Sprintf("Mock Kafka message %d", i),
			"topic":   mkc.topic,
		}

		docBytes, _ := json.Marshal(mockDoc)

		select {
		case mkc.messagesChan <- &MessageFromConsumerThread{
			Type: MessageTypePayload,
			Payload: &PayloadMessage{
				Bytes:     docBytes,
				Partition: 0,
				Offset:    int64(i),
			},
		}:
		case <-ctx.Done():
			return
		}
	}

	// Send EOF
	mkc.messagesChan <- &MessageFromConsumerThread{
		Type: MessageTypeEOF,
	}
}
