package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
)

// LagTechnicalSummary is used as technical summary in consumer monitoring healthchecks.
const LagTechnicalSummary string = "Messages awaiting handling exceed the configured lag tolerance. Check if Kafka consumer is stuck."

const (
	clusterConfigTimeout      = 5 * time.Second
	clusterDescriptionTimeout = 10 * time.Second
)

type clusterDescriber interface {
	DescribeClusterV2(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error)
}

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

func NewConsumer(cfg ConsumerConfig, topics []*Topic, log *logger.UPPLogger) (*Consumer, error) {
	consumerGroup, err := newConsumerGroup(cfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group: %w", err)
	}

	consumerFetcher, topicFetcher, err := newConsumerGroupOffsetFetchers(cfg.BrokersConnectionString)
	if err != nil {
		return nil, fmt.Errorf("creating consumer offset fetchers: %w", err)
	}

	var describer clusterDescriber
	if cfg.ClusterArn != nil {
		describer, err = newClusterDescriber()
		if err != nil {
			return nil, fmt.Errorf("creating cluster describer: %w", err)
		}
	} else {
		log.Warning("Cluster ARN not provided! Maintenance may cause false positive consumer monitor errors")
	}

	consumer := &Consumer{
		config:           cfg,
		topics:           topics,
		logger:           log,
		consumerGroup:    consumerGroup,
		monitor:          newConsumerMonitor(cfg, consumerFetcher, topicFetcher, topics, log),
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
	cfg := ConsumerConfig{
		BrokersConnectionString: c.config.BrokersConnectionString,
		ConsumerGroup:           fmt.Sprintf("healthcheck-%d", rand.Intn(100)),
		Options:                 c.config.Options,
	}
	consumerGroup, err := newConsumerGroup(cfg)
	if err != nil {
		return err
	}

	_ = consumerGroup.Close()

	return nil
}

func (c *Consumer) isMaintenanceOngoing() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterDescriptionTimeout)
	defer cancel()

	cluster, err := c.clusterDescriber.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{
		ClusterArn: c.config.ClusterArn,
	})
	if err != nil {
		return false, err
	}

	return cluster.ClusterInfo.State == types.ClusterStateMaintenance, nil
}

// MonitorCheck checks whether the consumer group is lagging behind when reading messages.
func (c *Consumer) MonitorCheck() error {
	err := c.monitor.consumerStatus()
	if err != ErrUnknownConsumerStatus || c.config.ClusterArn == nil {
		return err
	}

	ongoingMaintenance, err := c.isMaintenanceOngoing()
	if err != nil {
		return fmt.Errorf("cluster status is unknown: %w", err)
	}

	if ongoingMaintenance {
		// Skip unknown status errors until the cluster is back to 'ACTIVE' state.
		//
		// During maintenance all brokers are being failed over from one at a time.
		// This may trigger frequent resets of the offset fetchers connection and
		// in turn cause false positive alerts for unknown status.
		return nil
	}

	return ErrUnknownConsumerStatus
}

func newConsumerGroup(config ConsumerConfig) (sarama.ConsumerGroup, error) {
	if config.Options == nil {
		config.Options = DefaultConsumerOptions()
	}

	brokers := strings.Split(config.BrokersConnectionString, ",")
	return sarama.NewConsumerGroup(brokers, config.ConsumerGroup, config.Options)
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

func newClusterDescriber() (clusterDescriber, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterConfigTimeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return kafka.NewFromConfig(cfg), nil
}

// DefaultConsumerOptions returns a new sarama configuration with predefined default settings.
func DefaultConsumerOptions() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.MaxProcessingTime = 10 * time.Second
	cfg.Consumer.Return.Errors = true
	return cfg
}
