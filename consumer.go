package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
)

var (
	ErrConsumerNotConnected = fmt.Errorf("consumer is not connected to Kafka")
	ErrMonitorNotConnected  = fmt.Errorf("consumer monitor is not connected to Kafka")
)

const (
	maxConnectionRetryInterval     = 5 * time.Minute
	defaultConnectionRetryInterval = 1 * time.Minute

	topicReplicaSuffix = "Replica"
)

// LagTechnicalSummary is used as technical summary in consumer monitoring healthchecks.
const LagTechnicalSummary string = "Messages awaiting handling exceed the configured lag tolerance. Check if Kafka consumer is stuck."

// Consumer which will keep trying to reconnect to Kafka on a specified interval.
// The underlying consumer group is created lazily when message listening is started.
type Consumer struct {
	config            ConsumerConfig
	topics            []*Topic
	consumerGroupLock *sync.RWMutex
	consumerGroup     sarama.ConsumerGroup
	monitorLock       *sync.RWMutex
	monitor           *consumerMonitor
	logger            *logger.UPPLogger
	closed            chan struct{}
}

type ConsumerConfig struct {
	BrokersConnectionString string
	ConsumerGroup           string
	// Time interval between each connection attempt.
	// Only used for subsequent attempts if the initial one fails.
	// Default value (1 minute) would be used if not set or exceeds 5 minutes.
	ConnectionRetryInterval time.Duration
	// Time interval between each offset fetching request.
	// Default value (3 minutes) would be used if not set or exceeds 10 minutes.
	OffsetFetchInterval time.Duration
	// Total count of offset fetching request failures until consumer status is marked as unknown.
	// Default value (5) would be used if not set or exceeds 10.
	// Note: A single failure will result in follow-up requests to be sent on
	// shorter interval than the value of OffsetFetchInterval until successful.
	OffsetFetchMaxFailureCount int
	// Whether to disable the automatic reset of the sarama.ClusterAdmin
	// monitoring connection upon exceeding the OffsetFetchMaxFailureCount threshold.
	// Default value is false.
	// Note: Resetting the connection is necessary for the current version of Sarama (1.33.0)
	// due to numerous issues originating from the library. This flag is currently only used in tests.
	DisableMonitoringConnectionReset bool
	Options                          *sarama.Config
}

func NewConsumer(config ConsumerConfig, topics []*Topic, log *logger.UPPLogger) *Consumer {
	consumer := &Consumer{
		config:            config,
		topics:            topics,
		consumerGroupLock: &sync.RWMutex{},
		monitorLock:       &sync.RWMutex{},
		logger:            log,
		closed:            make(chan struct{}),
	}

	return consumer
}

// connect will attempt to create a new consumer continuously until successful.
func (c *Consumer) connect() {
	log := c.logger.
		WithField("brokers", c.config.BrokersConnectionString).
		WithField("topics", c.topics).
		WithField("consumer_group", c.config.ConsumerGroup)

	connectionRetryInterval := c.config.ConnectionRetryInterval
	if connectionRetryInterval <= 0 || connectionRetryInterval > maxConnectionRetryInterval {
		connectionRetryInterval = defaultConnectionRetryInterval
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for {
			consumerGroup, err := newConsumerGroup(c.config)
			if err == nil {
				log.Info("Established Kafka consumer group connection")
				c.setConsumerGroup(consumerGroup)
				return
			}

			log.WithError(err).Warn("Error creating Kafka consumer group")
			time.Sleep(connectionRetryInterval)
		}
	}()

	go func() {
		defer wg.Done()

		for {
			consumerFetcher, topicFetcher, err := newConsumerGroupOffsetFetchers(c.config.BrokersConnectionString)
			if err == nil {
				log.Info("Established Kafka offset fetcher connections")
				monitor := newConsumerMonitor(c.config, consumerFetcher, topicFetcher, c.topics, c.logger)
				c.setMonitor(monitor)
				return
			}

			log.WithError(err).Warn("Error creating Kafka offset fetchers")
			time.Sleep(connectionRetryInterval)
		}
	}()

	wg.Wait()
}

func (c *Consumer) setConsumerGroup(consumerGroup sarama.ConsumerGroup) {
	c.consumerGroupLock.Lock()
	defer c.consumerGroupLock.Unlock()

	c.consumerGroup = consumerGroup
}

// isConnected returns whether the consumer group is set.
// It is only set if a successful connection is established.
func (c *Consumer) isConnected() bool {
	c.consumerGroupLock.RLock()
	defer c.consumerGroupLock.RUnlock()

	return c.consumerGroup != nil
}

func (c *Consumer) setMonitor(monitor *consumerMonitor) {
	c.monitorLock.Lock()
	defer c.monitorLock.Unlock()

	c.monitor = monitor
}

// isMonitorConnected returns whether the consumer monitor is set.
// It is only set if a successful connection is established.
func (c *Consumer) isMonitorConnected() bool {
	c.monitorLock.RLock()
	defer c.monitorLock.RUnlock()

	return c.monitor != nil
}

func (c *Consumer) consumeMessages(ctx context.Context, topics []string, handler *consumerHandler) {
	log := c.logger.WithField("process", "Consumer")

	go func() {
		for err := range c.consumerGroup.Errors() {
			log.WithError(err).Error("Error consuming message")
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("Terminating consumer...")
			return

		default:
			if err := c.consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.WithError(err).Error("Error occurred during consumer group lifecycle")
			}
		}
	}
}

// Start will attempt to establish consumer group and consumer monitor connections until successful.
// Once those are established, message consumption and consumer monitoring processes are started.
//
// Each message will be handled using the provided handler.
//
// The consumer monitoring process is using a separate Kafka connection and will:
//  1. Request the offsets for a topic and the respective claimed partitions on a given time interval from the Kafka broker;
//  2. Deduce the message lag by subtracting the last committed consumer group offset from the next topic offset;
//  2. Store the partition lag if such is present;
//  4. Report a status error on MonitorCheck() calls.
//
// Close() calls will terminate both the message consumption and the consumer monitoring processes.
func (c *Consumer) Start(messageHandler func(message FTMessage)) {
	if !c.isConnected() {
		c.connect()
	}

	ctx, cancel := context.WithCancel(context.Background())
	subscriptions := make(chan *subscriptionEvent, 50)

	handler := newConsumerHandler(subscriptions, messageHandler)

	topics := make([]string, 0, len(c.topics)*2)
	for _, topic := range c.topics {
		topics = append(topics, topic.Name)

		if topic.hasReplica {
			topics = append(topics, topic.Name+topicReplicaSuffix)
		}
	}

	go c.consumeMessages(ctx, topics, handler)
	go c.monitor.run(ctx, subscriptions)

	go func() {
		<-c.closed

		// Terminate the message consumption and consumer monitoring.
		cancel()
	}()

	c.logger.Info("Starting consumer...")
}

// Close closes the consumer connection to Kafka if the consumer is connected.
func (c *Consumer) Close() error {
	if c.isConnected() {
		close(c.closed)

		return c.consumerGroup.Close()
	}

	return nil
}

// ConnectivityCheck checks whether a connection to Kafka can be established.
func (c *Consumer) ConnectivityCheck() error {
	if !c.isConnected() {
		return ErrConsumerNotConnected
	}

	config := ConsumerConfig{
		BrokersConnectionString: c.config.BrokersConnectionString,
		ConsumerGroup:           fmt.Sprintf("healthcheck-%d", rand.Intn(100)),
		Options:                 c.config.Options,
	}
	consumerGroup, err := newConsumerGroup(config)
	if err != nil {
		return err
	}

	_ = consumerGroup.Close()

	return nil
}

// MonitorCheck checks whether the consumer group is lagging behind when reading messages.
func (c *Consumer) MonitorCheck() error {
	if !c.isMonitorConnected() {
		return ErrMonitorNotConnected
	}

	return c.monitor.consumerStatus()
}

func newConsumerGroup(config ConsumerConfig) (sarama.ConsumerGroup, error) {
	if config.Options == nil {
		config.Options = DefaultConsumerOptions()
	}

	brokers := strings.Split(config.BrokersConnectionString, ",")
	return sarama.NewConsumerGroup(brokers, config.ConsumerGroup, config.Options)
}

func newConsumerGroupOffsetFetchers(brokerConnectionString string) (consumerOffsetFetcher, topicOffsetFetcher, error) {
	options := sarama.NewConfig()
	options.Version = sarama.V2_8_1_0

	brokers := strings.Split(brokerConnectionString, ",")
	client, err := sarama.NewClient(brokers, options)
	if err != nil {
		return nil, nil, err
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		_ = client.Close()
		return nil, nil, err
	}

	return admin, client, nil
}

// DefaultConsumerOptions returns a new sarama configuration with predefined default settings.
func DefaultConsumerOptions() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.MaxProcessingTime = 10 * time.Second
	config.Consumer.Return.Errors = true
	return config
}
