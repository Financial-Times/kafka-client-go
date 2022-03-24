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

type offsetFetcher interface {
	ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)

	Close() error
}

type fetcherScheduler struct {
	ticker            *time.Ticker
	standardInterval  time.Duration
	shortenedInterval time.Duration
	failureCount      int
	maxFailureCount   int
}

type consumerMonitor struct {
	consumerGroup string
	offsetFetcher offsetFetcher
	scheduler     fetcherScheduler
	// Key1 is Topic. Key2 is Partition. Value is Offset.
	subscriptions map[string]map[int32]int64
	topicsLock    *sync.RWMutex
	topics        []*Topic
	unknownStatus bool
	logger        *logger.UPPLogger
}

func newConsumerMonitor(config ConsumerConfig, fetcher offsetFetcher, topics []*Topic, logger *logger.UPPLogger) *consumerMonitor {
	offsetFetchInterval := config.OffsetFetchInterval
	if offsetFetchInterval <= 0 || offsetFetchInterval > 10*time.Minute {
		offsetFetchInterval = 3 * time.Minute
	}
	maxFailureCount := config.OffsetFetchMaxFailureCount
	if maxFailureCount <= 0 || maxFailureCount > 10 {
		maxFailureCount = 5
	}

	return &consumerMonitor{
		consumerGroup: config.ConsumerGroup,
		scheduler: fetcherScheduler{
			standardInterval:  offsetFetchInterval,
			shortenedInterval: offsetFetchInterval / 3,
			maxFailureCount:   maxFailureCount,
		},
		offsetFetcher: fetcher,
		subscriptions: map[string]map[int32]int64{},
		topicsLock:    &sync.RWMutex{},
		topics:        topics,
		logger:        logger,
	}
}

func (m *consumerMonitor) run(ctx context.Context, subscriptionEvents chan *subscriptionEvent) {
	m.scheduler.ticker = time.NewTicker(m.scheduler.standardInterval)
	defer m.scheduler.ticker.Stop()

	log := m.logger.WithField("process", "ConsumerMonitor")

	for {
		select {
		case <-ctx.Done():
			// Terminate the fetcher and exit.
			if err := m.offsetFetcher.Close(); err != nil {
				log.WithError(err).Error("Failed to close offset fetcher connection")
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
			subscriptions := m.currentSubscriptions()
			if len(subscriptions) == 0 {
				log.Warn("Consumer is not currently subscribed for any topics")
				continue
			}

			offsets, err := m.fetchOffsets(subscriptions)
			if err != nil {
				log.WithError(err).Error("Failed to fetch consumer offsets")

				m.scheduler.failureCount++
				if m.scheduler.failureCount >= m.scheduler.maxFailureCount {
					log.Errorf("Fetching offsets failed %d times in a row. Consumer status data is stale.",
						m.scheduler.failureCount)

					m.clearConsumerStatus()
				}

				m.scheduler.ticker.Reset(m.scheduler.shortenedInterval)
				continue
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
	subscriptions, ok := m.subscriptions[event.topic]
	if !ok {
		subscriptions = map[int32]int64{}
		m.subscriptions[event.topic] = subscriptions
	}

	if event.subscribed {
		subscriptions[event.partition] = 0
		return
	}

	delete(subscriptions, event.partition)
	if len(subscriptions) == 0 {
		delete(m.subscriptions, event.topic)
	}
}

func (m *consumerMonitor) currentSubscriptions() map[string][]int32 {
	subscriptions := map[string][]int32{}

	for topic, partitions := range m.subscriptions {
		for partition := range partitions {
			subscriptions[topic] = append(subscriptions[topic], partition)
		}
	}

	return subscriptions
}

func (m *consumerMonitor) fetchOffsets(subscriptions map[string][]int32) (map[string]map[int32]int64, error) {
	fetchedOffsets, err := m.offsetFetcher.ListConsumerGroupOffsets(m.consumerGroup, subscriptions)
	if err != nil {
		return nil, fmt.Errorf("error fetching consumer group offsets from client: %w", err)
	}

	if fetchedOffsets.Err != sarama.ErrNoError {
		return nil, fmt.Errorf("error fetching consumer group offsets from server: %w", fetchedOffsets.Err)
	}

	offsets := map[string]map[int32]int64{}
	for topic := range subscriptions {
		partitions, ok := fetchedOffsets.Blocks[topic]
		if !ok {
			return nil, fmt.Errorf("requested offsets for topic %q were not fetched", topic)
		}

		offsets[topic] = map[int32]int64{}
		for partition, block := range partitions {
			if block.Err != sarama.ErrNoError {
				return nil, fmt.Errorf("error fetching consumer group offsets for partition %d of topic %q from server: %w",
					partition, topic, block.Err)
			}

			offsets[topic][partition] = block.Offset
		}
	}

	return offsets, nil
}

func (m *consumerMonitor) updateConsumerStatus(fetchedOffsets map[string]map[int32]int64) {
	m.topicsLock.Lock()
	defer m.topicsLock.Unlock()

	for _, topic := range m.topics {
		subscriptions, ok := m.subscriptions[topic.Name]
		if !ok {
			// No active subscriptions for the given topic at the time.
			topic.partitionLag = map[int32]int64{}
			continue
		}

		fetchedOffsets, ok := fetchedOffsets[topic.Name]
		if !ok {
			topic.offsetsNotFetched = true
			continue
		}

		for partition, storedOffset := range subscriptions {
			fetchedOffset := fetchedOffsets[partition]

			// Only store the lag if it exceeds the configured threshold.
			lag := fetchedOffset - storedOffset
			if lag > topic.lagTolerance && storedOffset != 0 {
				topic.partitionLag[partition] = lag
			} else {
				// Clear the latest lag entry to flag the partition as healthy.
				delete(topic.partitionLag, partition)
			}

			// Replace the stored offset with the latest fetched one.
			subscriptions[partition] = fetchedOffset
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
		if topic.offsetsNotFetched {
			statusMessages = append(statusMessages, fmt.Sprintf("offsets for topic %q could not be fetched", topic.Name))
			continue
		}

		for partition, lag := range topic.partitionLag {
			message := fmt.Sprintf("consumer is lagging behind for partition %d of topic %q with %d messages",
				partition, topic.Name, lag)
			statusMessages = append(statusMessages, message)
		}
	}

	if len(statusMessages) == 0 {
		return nil
	}

	sort.Strings(statusMessages)
	return fmt.Errorf("consumer is not healthy: %s", strings.Join(statusMessages, " ; "))
}
