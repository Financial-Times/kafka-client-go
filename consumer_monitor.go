package kafka

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
)

const (
	maxOffsetFetchInterval     = 10 * time.Minute
	defaultOffsetFetchInterval = 3 * time.Minute

	maxOffsetFetchFailureCount     = 10
	defaultOffsetFetchFailureCount = 5
)

type topicOffsetFetcher interface {
	GetOffset(topic string, partitionID int32, position int64) (int64, error)
}

type consumerOffsetFetcher interface {
	ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)

	Close() error
}

type offset struct {
	Partition int32
	// Last committed offset of the consumer group.
	Consumer int64
	// Next offset to be produced in the topic.
	Topic int64
}

type fetcherScheduler struct {
	ticker            *time.Ticker
	standardInterval  time.Duration
	shortenedInterval time.Duration
	failureCount      int
	maxFailureCount   int
}

type consumerMonitor struct {
	connectionString        string
	consumerGroup           string
	consumerOffsetFetcher   consumerOffsetFetcher
	topicOffsetFetcher      topicOffsetFetcher
	scheduler               fetcherScheduler
	connectionResetDisabled bool
	// Key is Topic. Values are Partitions.
	subscriptions map[string][]int32
	topicsLock    *sync.RWMutex
	topics        []*Topic
	unknownStatus bool
	logger        *logger.UPPLogger
}

func newConsumerMonitor(config ConsumerConfig, consumerFetcher consumerOffsetFetcher, topicFetcher topicOffsetFetcher, topics []*Topic, logger *logger.UPPLogger) *consumerMonitor {
	offsetFetchInterval := config.OffsetFetchInterval
	if offsetFetchInterval <= 0 || offsetFetchInterval > maxOffsetFetchInterval {
		offsetFetchInterval = defaultOffsetFetchInterval
	}
	maxFailureCount := config.OffsetFetchMaxFailureCount
	if maxFailureCount <= 0 || maxFailureCount > maxOffsetFetchFailureCount {
		maxFailureCount = defaultOffsetFetchFailureCount
	}

	return &consumerMonitor{
		connectionString: config.BrokersConnectionString,
		consumerGroup:    config.ConsumerGroup,
		scheduler: fetcherScheduler{
			standardInterval:  offsetFetchInterval,
			shortenedInterval: offsetFetchInterval / 3,
			maxFailureCount:   maxFailureCount,
		},
		consumerOffsetFetcher:   consumerFetcher,
		topicOffsetFetcher:      topicFetcher,
		connectionResetDisabled: config.DisableMonitoringConnectionReset,
		subscriptions:           map[string][]int32{},
		topicsLock:              &sync.RWMutex{},
		topics:                  topics,
		logger:                  logger,
	}
}

func (m *consumerMonitor) run(ctx context.Context, subscriptionEvents chan *subscriptionEvent) {
	m.scheduler.ticker = time.NewTicker(m.scheduler.standardInterval)
	defer m.scheduler.ticker.Stop()

	log := m.logger.WithField("process", "ConsumerMonitor")

	for {
		select {
		case <-ctx.Done():
			// Terminate the fetchers and exit.
			// Note that both fetchers share the same connection so calling Close() once is enough.
			if err := m.consumerOffsetFetcher.Close(); err != nil {
				log.WithError(err).Error("Failed to close offset fetcher connections")
			}

			log.Info("Terminating consumer monitor...")
			return

		case event := <-subscriptionEvents:
			log.
				WithField("subscribed", event.subscribed).
				WithField("topic", event.topic).
				WithField("partition", event.partition).
				Info("Subscription event received")

			m.updateSubscriptions(event)

		case <-m.scheduler.ticker.C:
			if len(m.subscriptions) == 0 {
				log.Warn("Consumer is not currently subscribed for any topics")
				continue
			}

			offsets, err := m.fetchOffsets()
			if err != nil {
				log.WithError(err).Error("Failed to fetch consumer offsets")

				m.scheduler.ticker.Reset(m.scheduler.shortenedInterval)

				m.scheduler.failureCount++
				if m.scheduler.failureCount < m.scheduler.maxFailureCount {
					continue
				}

				log.Errorf("Fetching offsets failed %d times in a row. Consumer status data is stale.",
					m.scheduler.failureCount)

				if m.connectionResetDisabled {
					m.clearConsumerStatus()
					continue
				}

				// It's necessary to restart the connection due to:
				// * bug producing broken pipe errors: https://github.com/Shopify/sarama/issues/1796;
				// * consumer rebalancing causing coordinator errors;
				// * other issues unknown at this point and time.
				log.Info("Attempting to re-establish monitoring connection...")

				consumerOffsetFetcher, topicOffsetFetcher, err := newConsumerGroupOffsetFetchers(m.connectionString)
				if err != nil {
					log.WithError(err).Warn("Failed to establish new monitoring connection")

					m.clearConsumerStatus()
					continue
				}

				// Terminate the old connection and replace it.
				_ = m.consumerOffsetFetcher.Close()

				m.consumerOffsetFetcher = consumerOffsetFetcher
				m.topicOffsetFetcher = topicOffsetFetcher

				log.Info("Established new monitoring connection")

				// Re-attempt to fetch offsets and clear the status on failure.
				offsets, err = m.fetchOffsets()
				if err != nil {
					log.WithError(err).Error("Failed to fetch consumer offsets after resetting the connection")

					m.clearConsumerStatus()
					continue
				}
			}

			log.WithField("offsets", offsets).Debug("Offsets fetched successfully")

			m.updateConsumerStatus(offsets)

			if m.scheduler.failureCount != 0 {
				// Revert the scheduling change.
				m.scheduler.failureCount = 0
				m.scheduler.ticker.Reset(m.scheduler.standardInterval)
			}
		}
	}
}

func (m *consumerMonitor) updateSubscriptions(event *subscriptionEvent) {
	if event.subscribed {
		m.subscriptions[event.topic] = append(m.subscriptions[event.topic], event.partition)
		return
	}

	partitions, ok := m.subscriptions[event.topic]
	if !ok {
		return
	}

	for i, p := range partitions {
		if p == event.partition {
			partitions = append(partitions[:i], partitions[i+1:]...)
			break
		}
	}

	if len(partitions) == 0 {
		delete(m.subscriptions, event.topic)
		return
	}

	m.subscriptions[event.topic] = partitions
}

// Fetch and return the latest committed offsets by the consumer group
// as well as those at the end of the log for the respective topics.
func (m *consumerMonitor) fetchOffsets() (map[string][]offset, error) {
	fetchedOffsets, err := m.consumerOffsetFetcher.ListConsumerGroupOffsets(m.consumerGroup, m.subscriptions)
	if err != nil {
		return nil, fmt.Errorf("error fetching consumer group offsets from client: %w", err)
	}

	if fetchedOffsets.Err != sarama.ErrNoError {
		return nil, fmt.Errorf("error fetching consumer group offsets from server: %w", fetchedOffsets.Err)
	}

	topicOffsets := map[string][]offset{}
	for topic := range m.subscriptions {
		partitions, ok := fetchedOffsets.Blocks[topic]
		if !ok {
			return nil, fmt.Errorf("requested consumer offsets for topic %q were not fetched", topic)
		}

		var offsets []offset
		for partition, block := range partitions {
			if block.Err != sarama.ErrNoError {
				return nil, fmt.Errorf("error fetching consumer group offsets for partition %d of topic %q from server: %w",
					partition, topic, block.Err)
			}

			topicOffset, err := m.topicOffsetFetcher.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return nil, fmt.Errorf("error fetching topic offset for partition %d of topic %q: %w",
					partition, topic, err)
			}

			offsets = append(offsets, offset{
				Partition: partition,
				Consumer:  block.Offset,
				Topic:     topicOffset,
			})
		}

		topicOffsets[topic] = offsets
	}

	return topicOffsets, nil
}

func (m *consumerMonitor) updateConsumerStatus(fetchedOffsets map[string][]offset) {
	m.topicsLock.Lock()
	defer m.topicsLock.Unlock()

	for _, topic := range m.topics {
		offsets, ok := fetchedOffsets[topic.Name]
		if !ok {
			// No active subscriptions for the given topic at the time.
			topic.partitionLag = map[int32]int64{}
			continue
		}

		for _, offset := range offsets {
			// Only store the lag if it exceeds the configured threshold or is invalid.
			// The next offset in the topic will always be greater than or equal to the last committed offset of the consumer.
			lag := offset.Topic - offset.Consumer
			if lag > topic.lagTolerance || lag < 0 {
				topic.partitionLag[offset.Partition] = lag
			} else {
				// Clear the latest lag entry to flag the partition as healthy.
				delete(topic.partitionLag, offset.Partition)
			}
		}
	}

	m.unknownStatus = false
}

func (m *consumerMonitor) clearConsumerStatus() {
	m.topicsLock.Lock()
	defer m.topicsLock.Unlock()

	m.unknownStatus = true
	for _, topic := range m.topics {
		topic.partitionLag = map[int32]int64{}
	}
}

func (m *consumerMonitor) consumerStatus() error {
	m.topicsLock.RLock()
	defer m.topicsLock.RUnlock()

	if m.unknownStatus {
		return fmt.Errorf("consumer status is unknown")
	}

	var statusMessages []string

	for _, topic := range m.topics {
		for partition, lag := range topic.partitionLag {
			var message string
			if lag < 0 {
				message = fmt.Sprintf("could not determine lag for partition %d of topic %q", partition, topic.Name)
			} else {
				message = fmt.Sprintf("consumer is lagging behind for partition %d of topic %q with %d messages", partition, topic.Name, lag)
			}

			statusMessages = append(statusMessages, message)
		}
	}

	if len(statusMessages) == 0 {
		return nil
	}

	sort.Strings(statusMessages)
	return fmt.Errorf("consumer is not healthy: %s", strings.Join(statusMessages, " ; "))
}
