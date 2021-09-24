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

const errConsumerNotConnected = "consumer is not connected to Kafka"

// Consumer which will keep trying to reconnect to Kafka on a specified interval.
// The underlying consumer group is created lazily when message listening is started.
type Consumer struct {
	config            ConsumerConfig
	consumerGroupLock *sync.RWMutex
	consumerGroup     sarama.ConsumerGroup
	retryInterval     time.Duration
	logger            *logger.UPPLogger
	closed            chan struct{}
}

type ConsumerConfig struct {
	BrokersConnectionString string
	ConsumerGroup           string
	Topics                  []string
	Options                 *sarama.Config
}

func NewConsumer(config ConsumerConfig, log *logger.UPPLogger, retryInterval time.Duration) *Consumer {
	consumer := &Consumer{
		config:            config,
		consumerGroupLock: &sync.RWMutex{},
		retryInterval:     retryInterval,
		logger:            log,
		closed:            make(chan struct{}),
	}
	return consumer
}

// connect will attempt to create a new consumer continuously until successful.
func (c *Consumer) connect() {
	connectorLog := c.logger.
		WithField("brokers", c.config.BrokersConnectionString).
		WithField("topics", c.config.Topics).
		WithField("consumer_group", c.config.ConsumerGroup)

	for {
		consumerGroup, err := newConsumerGroup(c.config)
		if err == nil {
			connectorLog.Info("Connected to Kafka consumer group")
			c.setConsumerGroup(consumerGroup)
			break
		}

		connectorLog.WithError(err).Warn("Error creating Kafka consumer group")
		time.Sleep(c.retryInterval)
	}
}

// setConsumerGroup sets the Consumer's consumer group.
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

// StartListening is a blocking call that tries to establish a connection to Kafka and then starts listening.
func (c *Consumer) StartListening(messageHandler func(message FTMessage)) {
	if !c.isConnected() {
		c.connect()
	}

	handler := newConsumerHandler(c.logger, messageHandler)

	go func() {
		for err := range c.consumerGroup.Errors() {
			c.logger.WithError(err).
				WithField("method", "StartListening").
				Error("Error processing message")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		for {
			if err := c.consumerGroup.Consume(ctx, c.config.Topics, handler); err != nil {
				c.logger.WithError(err).
					WithField("method", "StartListening").
					Error("Error starting consumer")
			}
			// Check if context was cancelled, signaling that the consumer should stop.
			if ctx.Err() != nil {
				return
			}
		}
	}()

	go func() {
		for {
			select {
			case <-handler.ready:
				c.logger.Debug("New consumer group session starting...")
			case <-c.closed:
				// Terminate the message consumption.
				cancel()
				return
			}
		}
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
		return fmt.Errorf(errConsumerNotConnected)
	}

	config := ConsumerConfig{
		BrokersConnectionString: c.config.BrokersConnectionString,
		ConsumerGroup:           fmt.Sprintf("healthcheck-%d", rand.Intn(100)),
		Topics:                  c.config.Topics,
		Options:                 c.config.Options,
	}
	consumerGroup, err := newConsumerGroup(config)
	if err != nil {
		return err
	}

	_ = consumerGroup.Close()

	return nil
}

func newConsumerGroup(config ConsumerConfig) (sarama.ConsumerGroup, error) {
	if config.Options == nil {
		config.Options = DefaultConsumerOptions()
	}

	brokers := strings.Split(config.BrokersConnectionString, ",")
	return sarama.NewConsumerGroup(brokers, config.ConsumerGroup, config.Options)
}

// DefaultConsumerOptions returns a new sarama configuration with predefined default settings.
func DefaultConsumerOptions() *sarama.Config {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.MaxProcessingTime = 10 * time.Second
	return config
}
