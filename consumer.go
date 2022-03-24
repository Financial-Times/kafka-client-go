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
	// Default is 1 minute. Maximum is 5 minutes.
	ConnectionRetryInterval time.Duration
	// Time interval between each offset fetching request. Default is 3 minutes.
	OffsetFetchInterval time.Duration
	// Total count of offset fetching request failures until consumer status is marked as unknown.
	// Default is 5. Note: A single failure will result in follow-up requests to be sent on
	// shorter interval than the value of OffsetFetchInterval until successful.
	OffsetFetchMaxFailureCount int
	Options                    *sarama.Config
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
	if connectionRetryInterval <= 0 || connectionRetryInterval > 5*time.Minute {
		connectionRetryInterval = time.Minute
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
			offsetFetcher, err := newConsumerGroupOffsetFetcher(c.config)
			if err == nil {
				log.Info("Established Kafka offset fetcher connection")
				monitor := newConsumerMonitor(c.config, offsetFetcher, c.topics, c.logger)
				c.setMonitor(monitor)
				return
			}

			log.WithError(err).Warn("Error creating Kafka offset fetcher")
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
//  2. Deduce the message lag by subtracting the fetched and previously stored offsets;
//  2. Store the partition lag if such is present and overwrite the previously stored offsets;
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

	topics := make([]string, 0, len(c.topics))
	for _, topic := range c.topics {
		topics = append(topics, topic.Name)
	}

	go c.consumeMessages(ctx, topics, handler)
	go c.monitor.run(ctx, subscriptions)

	go func() {
		<-c.closed

		// Terminate the message consumption and consumer monitoring.
		cancel()
		return
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

func newConsumerGroupOffsetFetcher(config ConsumerConfig) (offsetFetcher, error) {
	options := sarama.NewConfig()
	options.Version = sarama.V2_8_1_0

	brokers := strings.Split(config.BrokersConnectionString, ",")
	return sarama.NewClusterAdmin(brokers, options)
}

// DefaultConsumerOptions returns a new sarama configuration with predefined default settings.
func DefaultConsumerOptions() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.MaxProcessingTime = 10 * time.Second
	config.Consumer.Return.Errors = true
	return config
}
