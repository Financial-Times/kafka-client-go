package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
)

// LagTechnicalSummary is used as technical summary in consumer monitoring healthchecks.
const LagTechnicalSummary string = "Messages awaiting handling exceed the configured lag tolerance. Check if Kafka consumer is stuck."

type Consumer struct {
	config           ConsumerConfig
	topics           []*Topic
	consumerGroup    sarama.ConsumerGroup
	monitor          *consumerMonitor
	clusterDescriber clusterDescriber
	logger           *logger.UPPLogger
	closed           chan struct{}
}

type ConsumerConfig struct {
	ClusterArn              *string
	BrokersConnectionString string
	ConsumerGroup           string
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
	// Note: Resetting the connection is necessary for the current version of Sarama (1.37.2)
	// due to numerous issues originating from the library. This flag is currently only used in tests.
	DisableMonitoringConnectionReset bool
	Options                          *sarama.Config
}

func NewConsumer(config ConsumerConfig, topics []*Topic, log *logger.UPPLogger) (*Consumer, error) {
	if config.Options == nil {
		config.Options = DefaultConsumerOptions()
	}

	brokers := strings.Split(config.BrokersConnectionString, ",")

	consumerGroup, err := sarama.NewConsumerGroup(brokers, config.ConsumerGroup, config.Options)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	consumerFetcher, topicFetcher, err := newConsumerGroupOffsetFetchers(config.BrokersConnectionString)
	if err != nil {
		return nil, fmt.Errorf("creating consumer offset fetchers: %w", err)
	}

	var describer clusterDescriber
	if config.ClusterArn != nil {
		describer, err = newClusterDescriber(config.ClusterArn)
		if err != nil {
			return nil, fmt.Errorf("creating cluster describer: %w", err)
		}
	} else {
		log.Warning("Cluster ARN not provided! Maintenance may cause false positive consumer errors")
	}

	consumer := &Consumer{
		config:           config,
		topics:           topics,
		logger:           log,
		consumerGroup:    consumerGroup,
		monitor:          newConsumerMonitor(config, consumerFetcher, topicFetcher, topics, log),
		clusterDescriber: describer,
		closed:           make(chan struct{}),
	}

	return consumer, nil
}

func (c *Consumer) consumeMessages(ctx context.Context, topics []string, handler *consumerHandler) {
	log := c.logger.WithField("process", "Consumer")

	go func() {
		for {
			select {
			case <-ctx.Done():
				return

			case err := <-c.consumerGroup.Errors():
				log.WithError(err).Error("Error consuming message")
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Info("Terminating consumer...")
			return

		default:
			if err := c.consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.WithError(err).Warn("Error occurred during consumer group lifecycle")
			}
		}
	}
}

// Start will start the message consumption and consumer monitoring processes.
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
	ctx, cancel := context.WithCancel(context.Background())
	subscriptions := make(chan *subscriptionEvent, 50)

	handler := newConsumerHandler(subscriptions, messageHandler)

	topics := make([]string, 0, len(c.topics))
	for _, topic := range c.topics {
		topics = append(topics, topic.Name)
	}

	c.logger.WithField("topics", topics).Info("Starting consumer...")

	go c.consumeMessages(ctx, topics, handler)
	go c.monitor.run(ctx, subscriptions)

	go func() {
		<-c.closed

		// Terminate the message consumption and consumer monitoring.
		cancel()
	}()
}

// Close closes the connection to Kafka.
func (c *Consumer) Close() error {
	close(c.closed)

	return c.consumerGroup.Close()
}

// ConnectivityCheck checks whether a connection to Kafka can be established.
func (c *Consumer) ConnectivityCheck() error {
	brokers := strings.Split(c.config.BrokersConnectionString, ",")

	if err := checkConnectivity(brokers); err != nil {
		if c.config.ClusterArn != nil {
			return verifyHealthErrorSeverity(err, c.clusterDescriber, c.config.ClusterArn)
		}

		return err
	}

	return nil
}

// MonitorCheck checks whether the consumer group is lagging behind when reading messages.
func (c *Consumer) MonitorCheck() error {
	err := c.monitor.consumerStatus()
	if err != nil {
		if err == ErrUnknownConsumerStatus && c.config.ClusterArn != nil {
			return verifyHealthErrorSeverity(err, c.clusterDescriber, c.config.ClusterArn)
		}

		return err
	}

	return nil
}

func (c *Consumer) DynamicMonitorCheck() error {
	if len(c.monitor.subscriptions) == 0 {
		return fmt.Errorf("Consumer is not currently subscribed for any topics")
	}

	offsets, err := c.monitor.fetchOffsets()
	if err != nil {
		// It's necessary to restart the connection due to:
		// * bug producing broken pipe errors: https://github.com/Shopify/sarama/issues/1796;
		// * consumer rebalancing causing coordinator errors;
		// * other issues unknown at this point and time
		consumerOffsetFetcher, topicOffsetFetcher, err := newConsumerGroupOffsetFetchers(c.monitor.connectionString)
		if err != nil {
			c.monitor.clearConsumerStatus()
		}

		// Terminate the old connection and replace it.
		_ = c.monitor.consumerOffsetFetcher.Close()

		c.monitor.consumerOffsetFetcher = consumerOffsetFetcher
		c.monitor.topicOffsetFetcher = topicOffsetFetcher

		// Re-attempt to fetch offsets and clear the status on failure.
		offsets, err = c.monitor.fetchOffsets()
		if err != nil {
			c.monitor.clearConsumerStatus()
		}
	}
	c.monitor.updateConsumerStatus(offsets)

	err = c.monitor.consumerStatus()
	if err != nil {
		if err == ErrUnknownConsumerStatus && c.config.ClusterArn != nil {
			return verifyHealthErrorSeverity(err, c.clusterDescriber, c.config.ClusterArn)
		}

		return err
	}

	return nil
}

func newConsumerGroupOffsetFetchers(brokerConnectionString string) (consumerOffsetFetcher, topicOffsetFetcher, error) {
	brokers := strings.Split(brokerConnectionString, ",")
	client, err := sarama.NewClient(brokers, sarama.NewConfig())
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
