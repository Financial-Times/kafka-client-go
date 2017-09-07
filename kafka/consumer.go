package kafka

import (
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
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
}

type perseverantConsumer struct {
	sync.RWMutex
	zookeeperConnectionString string
	consumerGroup             string
	topics                    []string
	config                    *consumergroup.Config
	consumer                  Consumer
}

func NewConsumer(zookeeperConnectionString string, consumerGroup string, topics []string, config *consumergroup.Config) (Consumer, error) {
	zookeeperNodes, chroot := kazoo.ParseConnectionString(zookeeperConnectionString)

	if config == nil {
		config = DefaultConsumerConfig()
		config.Zookeeper.Chroot = chroot
	} else if config.Zookeeper.Chroot != chroot {
		log.WithFields(log.Fields{
			"method":            "NewConsumer",
			"configuredChroot": config.Zookeeper.Chroot,
			"parsedChroot":     chroot,
		}).Error("Mismatch in Zookeeper config while creating Kafka consumer")
		return nil, errors.New("Mismatch in Zookeeper config while creating Kafka consumer")
	}

	consumer, err := consumergroup.JoinConsumerGroup(consumerGroup, topics, zookeeperNodes, config)
	if err != nil {
		log.WithError(err).WithField("method", "NewConsumer").Error("Error creating Kafka consumer")
		return nil, err
	}

	return &MessageConsumer{
		topics:         topics,
		consumerGroup:  consumerGroup,
		zookeeperNodes: zookeeperNodes,
		consumer:       consumer,
		config:         config,
	}, nil
}

func NewPerseverantConsumer(zookeeperConnectionString string, consumerGroup string, topics []string, config *consumergroup.Config, initialDelay time.Duration, retryInterval time.Duration) (Consumer, error) {
	consumer := &perseverantConsumer{sync.RWMutex{}, zookeeperConnectionString, consumerGroup, topics, config, nil}

	go func() {
		if initialDelay > 0 {
			time.Sleep(initialDelay)
		}
		consumer.connect(retryInterval)
	}()

	return consumer, nil
}

func (c *MessageConsumer) StartListening(messageHandler func(message FTMessage) error) {
	go func() {
		for err := range c.consumer.Errors() {
			log.WithError(err).WithField("method", "StartListening").Error("Error proccessing message")
		}
	}()

	go func() {
		for message := range c.consumer.Messages() {
			ftMsg, err := rawToFTMessage(message.Value)
			if err != nil {
				log.WithError(err).WithField("method", "StartListening").Error("Error converting Kafka message body to FTMessage")
			}
			err = messageHandler(ftMsg)
			if err != nil {
				log.WithError(err).WithField("method", "StartListening").WithField("messageKey", message.Key).Error("Error processing message")
			}
			c.consumer.CommitUpto(message)
		}
	}()
}

func (c *MessageConsumer) Shutdown() {
	if err := c.consumer.Close(); err != nil {
		log.WithError(err).WithField("method", "Shutdown").Error("Error closing the consumer")
	}
}

func (c *MessageConsumer) ConnectivityCheck() error {
	// establishing (or failing to establish) a new connection (with a distinct consumer group) is a reasonable check
	// as experiment shows the consumer's existing connection is automatically repaired after any interruption
	healthcheckConsumer, err := NewConsumer(strings.Join(c.zookeeperNodes, ","), c.consumerGroup+"-healthcheck", c.topics, c.config)
	if err != nil {
		return err
	}
	defer healthcheckConsumer.Shutdown()

	return nil
}

func (c *perseverantConsumer) connect(retryInterval time.Duration) {
	connectorLog := log.WithField("zookeeper", c.zookeeperConnectionString).WithField("topics", c.topics).WithField("consumerGroup", c.consumerGroup)
	for {
		consumer, err := NewConsumer(c.zookeeperConnectionString, c.consumerGroup, c.topics, c.config)
		if err == nil {
			connectorLog.Info("connected to Kafka consumer")
			c.setConsumer(consumer)
			break
		}

		connectorLog.WithError(err).Warn(errConsumerNotConnected)
		time.Sleep(retryInterval)
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
		c.connect(time.Minute)
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
		return errors.New(errConsumerNotConnected)
	}

	return c.consumer.ConnectivityCheck()
}

func DefaultConsumerConfig() *consumergroup.Config {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	return config
}
