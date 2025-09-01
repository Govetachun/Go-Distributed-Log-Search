package sources

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"toshokan/src/database"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
)

// KafkaPrefix is the URL prefix for Kafka sources
const KafkaPrefix = "kafka://"

const (
	consumerThreadMessagesChannelSize = 10
	pollDuration                      = time.Second
	defaultCommitInterval             = 30 * time.Second
	defaultSessionTimeout             = 6 * time.Second
	defaultHeartbeatInterval          = 2 * time.Second
)

// MessageFromConsumerThread represents messages from the consumer thread
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
func NewKafkaSourceFromURL(ctx context.Context, url string, stream bool, db database.DBAdapter) (*KafkaSource, error) {
	servers, topic, err := ParseKafkaURL(url)
	if err != nil {
		return nil, err
	}

	messagesChan := make(chan *MessageFromConsumerThread, consumerThreadMessagesChannelSize)

	// Create real Kafka consumer using Sarama
	consumer, err := NewKafkaConsumer(servers, topic, stream, messagesChan)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
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
func (ks *KafkaSource) trackSavedCheckpoint(partitionsAndOffsets []PartitionOffset) {
	// Implementation would be conditional based on build tags in Go
	ks.savedPartitionsAndOffsets = append([]PartitionOffset{}, partitionsAndOffsets...)
}

// trackLoadedCheckpoint tracks loaded checkpoints for testing
func (ks *KafkaSource) trackLoadedCheckpoint(partitionsAndOffsets []PartitionOffsetWithOptional) {
	// Implementation would be conditional based on build tags in Go
	ks.loadedPartitionsAndOffsets = append([]PartitionOffsetWithOptional{}, partitionsAndOffsets...)
}

// GetOne implements Source interface
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

// KafkaConsumer implements a real Kafka consumer using Sarama
type KafkaConsumer struct {
	servers        string
	topic          string
	stream         bool
	messagesChan   chan *MessageFromConsumerThread
	consumer       sarama.Consumer
	partConsumer   sarama.PartitionConsumer
	groupID        string
	commitInterval time.Duration
	config         *sarama.Config
}

// NewKafkaConsumer creates a new Kafka consumer with advanced configuration
func NewKafkaConsumer(servers, topic string, stream bool, messagesChan chan *MessageFromConsumerThread) (*KafkaConsumer, error) {
	config := sarama.NewConfig()

	// Kafka version compatibility
	config.Version = sarama.V2_8_0_0

	// Consumer settings
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Session.Timeout = defaultSessionTimeout
	config.Consumer.Group.Heartbeat.Interval = defaultHeartbeatInterval

	// Set consumer group ID
	groupID := fmt.Sprintf("toshokan_%s_%s",
		map[bool]string{true: "stream", false: "batch"}[stream],
		topic)

	if stream {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest // Start from latest for streaming
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from beginning for batch
	}

	// Disable auto-commit for manual checkpoint management
	config.Consumer.Offsets.AutoCommit.Enable = false

	// Network and retry settings
	config.Net.DialTimeout = 10 * time.Second
	config.Net.ReadTimeout = 10 * time.Second
	config.Net.WriteTimeout = 10 * time.Second
	config.Consumer.Retry.Backoff = 2 * time.Second

	// Fetch settings for better performance
	config.Consumer.Fetch.Min = 1024        // 1KB minimum
	config.Consumer.Fetch.Default = 1024000 // 1MB default
	config.Consumer.Fetch.Max = 10240000    // 10MB maximum

	// Channel buffer sizes
	config.ChannelBufferSize = 256

	// Enable debug logging if needed
	logrus.Debugf("Creating Kafka consumer with config: groupID=%s, servers=%s, topic=%s, stream=%v",
		groupID, servers, topic, stream)

	consumer, err := sarama.NewConsumer(strings.Split(servers, ","), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	return &KafkaConsumer{
		servers:        servers,
		topic:          topic,
		stream:         stream,
		messagesChan:   messagesChan,
		consumer:       consumer,
		groupID:        groupID,
		commitInterval: defaultCommitInterval,
		config:         config,
	}, nil
}

// Run starts the Kafka consumer
func (kc *KafkaConsumer) Run(ctx context.Context) {
	defer close(kc.messagesChan)
	defer func() {
		if kc.partConsumer != nil {
			kc.partConsumer.Close()
		}
		if kc.consumer != nil {
			kc.consumer.Close()
		}
	}()

	// Get partition information
	partitions, err := kc.consumer.Partitions(kc.topic)
	if err != nil {
		logrus.Errorf("Failed to get partitions for topic %s: %v", kc.topic, err)
		return
	}

	if len(partitions) == 0 {
		logrus.Errorf("No partitions found for topic %s", kc.topic)
		return
	}

	logrus.Debugf("Found %d partitions for topic %s", len(partitions), kc.topic)

	// Send initial assignment for all partitions
	responseChan := make(chan []PartitionOffsetWithOptional, 1)
	kc.messagesChan <- &MessageFromConsumerThread{
		Type: MessageTypePostRebalance,
		PostRebalance: &PostRebalanceMessage{
			Partitions:   partitions,
			ResponseChan: responseChan,
		},
	}

	// Wait for checkpoint response
	var checkpointResponse []PartitionOffsetWithOptional
	select {
	case checkpointResponse = <-responseChan:
	case <-ctx.Done():
		return
	}

	// Start consuming from all partitions concurrently
	logrus.Infof("Starting consumers for %d partitions of topic %s", len(partitions), kc.topic)

	// Channel to collect messages from all partitions
	allMessages := make(chan *sarama.ConsumerMessage, len(partitions)*10)
	allErrors := make(chan *sarama.ConsumerError, len(partitions)*10)

	// Start partition consumers
	var partConsumers []sarama.PartitionConsumer
	for _, partition := range partitions {
		// Find the offset for this partition from checkpoint
		var startOffset int64 = sarama.OffsetOldest
		if kc.stream {
			startOffset = sarama.OffsetNewest
		}

		for _, checkpoint := range checkpointResponse {
			if checkpoint.Partition == partition && checkpoint.Offset != nil {
				startOffset = *checkpoint.Offset
				logrus.Debugf("Starting from checkpoint offset %d for partition %d", startOffset, partition)
				break
			}
		}

		partConsumer, err := kc.consumer.ConsumePartition(kc.topic, partition, startOffset)
		if err != nil {
			logrus.Errorf("Failed to create partition consumer for partition %d: %v", partition, err)
			continue
		}

		partConsumers = append(partConsumers, partConsumer)

		// Start goroutine for this partition
		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case message := <-pc.Messages():
					if message != nil {
						allMessages <- message
					}
				case err := <-pc.Errors():
					if err != nil {
						allErrors <- err
					}
				case <-ctx.Done():
					return
				}
			}
		}(partConsumer)

		logrus.Infof("Started consumer for partition %d at offset %d", partition, startOffset)
	}

	// Store partition consumers for cleanup
	for _, pc := range partConsumers {
		kc.partConsumer = pc // Store the last one for cleanup (could be improved)
	}

	messageCount := 0
	commitTicker := time.NewTicker(kc.commitInterval)
	defer commitTicker.Stop()

	// Main consumption loop
	for {
		select {
		case message := <-allMessages:
			if message == nil {
				continue
			}

			messageCount++
			logrus.Debugf("Received message %d: partition=%d, offset=%d, key=%s",
				messageCount, message.Partition, message.Offset, string(message.Key))

			select {
			case kc.messagesChan <- &MessageFromConsumerThread{
				Type: MessageTypePayload,
				Payload: &PayloadMessage{
					Bytes:     message.Value,
					Partition: message.Partition,
					Offset:    message.Offset,
				},
			}:
			case <-ctx.Done():
				return
			}

		case err := <-allErrors:
			if err != nil {
				logrus.Errorf("Kafka consumer error (topic %s, partition %d): %v",
					err.Topic, err.Partition, err.Err)
				// Continue consuming despite errors
			}

		case <-commitTicker.C:
			if kc.stream && messageCount > 0 {
				logrus.Debugf("Commit interval reached, processed %d messages", messageCount)
				// In a real implementation, you would commit offsets here
				// For now, we rely on the checkpoint mechanism
			}

		case <-ctx.Done():
			logrus.Infof("Kafka consumer context cancelled, shutting down (processed %d messages)", messageCount)

			// Close all partition consumers
			for _, pc := range partConsumers {
				if pc != nil {
					pc.Close()
				}
			}
			return
		}
	}
}
