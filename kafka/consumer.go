package kafka

import (
	"fmt"
	"strings"
	"sync"
	"time"

	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka/consumergroup"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kazoo-go"
)

const errConsumerNotConnected = "consumer is not connected to Kafka"

type ConsumerGrouper interface {
	Errors() <-chan error
	Messages() <-chan *sarama.ConsumerMessage
	CommitUpto(message *sarama.ConsumerMessage) error
	Close() error
	Closed() bool
}

type Consumer interface {
	StartListening(messageHandler func(message FTMessage) error)
	Shutdown()
	ConnectivityCheck() error
}

type MessageConsumer struct {
	topics         []string
	consumerGroup  string
	zookeeperNodes []string
	consumer       ConsumerGrouper
	config         *consumergroup.Config
	errCh          chan error
	logger         *logger.UPPLogger
}

type perseverantConsumer struct {
	sync.RWMutex
	zookeeperConnectionString string
	consumerGroup             string
	topics                    []string
	config                    *consumergroup.Config
	consumer                  Consumer
	retryInterval             time.Duration
	errCh                     *chan error
	logger                    *logger.UPPLogger
}

type Config struct {
	ZookeeperConnectionString string
	ConsumerGroup             string
	Topics                    []string
	ConsumerGroupConfig       *consumergroup.Config
	Err                       chan error
	Logger                    *logger.UPPLogger
}

func NewConsumer(config Config) (Consumer, error) {

	config.Logger.Debug("Creating new consumer")

	zookeeperNodes, chroot := kazoo.ParseConnectionString(config.ZookeeperConnectionString)

	if config.ConsumerGroupConfig == nil {
		config.ConsumerGroupConfig = DefaultConsumerConfig()
		config.ConsumerGroupConfig.Zookeeper.Chroot = chroot
	} else if config.ConsumerGroupConfig.Zookeeper.Chroot != chroot {

		errorMessage := "Mismatch in Zookeeper config while creating Kafka consumer"
		config.Logger.
			WithField("method", "NewConsumer").
			WithField("configuredChroot", config.ConsumerGroupConfig.Zookeeper.Chroot).
			WithField("parsedChroot", chroot).
			Error(errorMessage)
		return nil, fmt.Errorf(errorMessage)
	}

	consumer, err := consumergroup.JoinConsumerGroup(config.ConsumerGroup, config.Topics, zookeeperNodes, config.ConsumerGroupConfig)
	if err != nil {
		config.Logger.WithError(err).
			WithField("method", "NewConsumer").
			Error("Error creating Kafka consumer")
		return nil, err
	}

	return &MessageConsumer{
		topics:         config.Topics,
		consumerGroup:  config.ConsumerGroup,
		zookeeperNodes: zookeeperNodes,
		consumer:       consumer,
		config:         config.ConsumerGroupConfig,
		errCh:          config.Err,
		logger:         config.Logger,
	}, nil
}

func NewPerseverantConsumer(zookeeperConnectionString string, consumerGroup string, topics []string, config *consumergroup.Config, retryInterval time.Duration, errCh *chan error, logger *logger.UPPLogger) (Consumer, error) {
	consumer := &perseverantConsumer{
		RWMutex:                   sync.RWMutex{},
		zookeeperConnectionString: zookeeperConnectionString,
		consumerGroup:             consumerGroup,
		topics:                    topics,
		config:                    config,
		retryInterval:             retryInterval,
		errCh:                     errCh,
		logger:                    logger,
	}
	return consumer, nil
}

func (c *MessageConsumer) StartListening(messageHandler func(message FTMessage) error) {

	go func() {

		c.logger.Debug("Start listening for consumer errors")
		for err := range c.consumer.Errors() {

			c.logger.WithError(err).
				WithField("method", "StartListening").
				Error("Error proccessing message")

			if c.errCh != nil {
				c.errCh <- err
			}
		}
	}()

	go func() {
		for message := range c.consumer.Messages() {
			c.logger.Debug("start listening for messages")

			ftMsg := rawToFTMessage(message.Value)
			err := messageHandler(ftMsg)
			if err != nil {

				c.logger.WithError(err).
					WithField("method", "StartListening").
					WithField("messageKey", message.Key).
					Error("Error processing message")

				if c.errCh != nil {
					c.errCh <- err
				}
			}
			c.consumer.CommitUpto(message)
		}
	}()
}

func (c *MessageConsumer) Shutdown() {
	if err := c.consumer.Close(); err != nil {

		c.logger.WithError(err).
			WithField("method", "Shutdown").
			Error("Error closing consumer")

		if c.errCh != nil {
			c.errCh <- err
		}
	}
}

func (c *MessageConsumer) ConnectivityCheck() error {
	// establishing (or failing to establish) a new connection (with a distinct consumer group) is a reasonable check
	// as experiment shows the consumer's existing connection is automatically repaired after any interruption
	config := Config{
		ZookeeperConnectionString: strings.Join(c.zookeeperNodes, ","),
		ConsumerGroup:             c.consumerGroup + "-healthcheck",
		Topics:                    c.topics,
		ConsumerGroupConfig:       c.config,
		Logger:                    c.logger,
	}
	healthcheckConsumer, err := NewConsumer(config)
	if err != nil {
		return err
	}
	defer healthcheckConsumer.Shutdown()

	return nil
}

func (c *perseverantConsumer) connect() {

	connectorLog := c.logger.WithField("zookeeper", c.zookeeperConnectionString).
		WithField("topics", c.topics).
		WithField("consumerGroup", c.consumerGroup)
	for {
		var errCh chan error
		if c.errCh != nil {
			errCh = *c.errCh
		}

		consumer, err := NewConsumer(Config{
			ZookeeperConnectionString: c.zookeeperConnectionString,
			ConsumerGroup:             c.consumerGroup,
			Topics:                    c.topics,
			ConsumerGroupConfig:       c.config,
			Err:                       errCh,
			Logger:                    c.logger,
		})

		if err == nil {
			connectorLog.Info("connected to Kafka consumer")
			c.setConsumer(consumer)
			break
		}

		connectorLog.WithError(err).Warn(errConsumerNotConnected)
		time.Sleep(c.retryInterval)
	}
}

func (c *perseverantConsumer) setConsumer(consumer Consumer) {
	c.Lock()
	defer c.Unlock()

	c.consumer = consumer
}

func (c *perseverantConsumer) isConnected() bool {
	c.RLock()
	defer c.RUnlock()

	return c.consumer != nil
}

func (c *perseverantConsumer) StartListening(messageHandler func(message FTMessage) error) {
	if !c.isConnected() {
		c.connect()
	}

	c.RLock()
	defer c.RUnlock()

	c.consumer.StartListening(messageHandler)
}

func (c *perseverantConsumer) Shutdown() {
	c.RLock()
	defer c.RUnlock()

	if c.isConnected() {
		c.consumer.Shutdown()
	}
}

func (c *perseverantConsumer) ConnectivityCheck() error {
	c.RLock()
	defer c.RUnlock()

	if !c.isConnected() {
		return fmt.Errorf(errConsumerNotConnected)
	}

	return c.consumer.ConnectivityCheck()
}

func DefaultConsumerConfig() *consumergroup.Config {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	return config
}
