package kafka

import (
	"time"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"
	"github.com/wvanbergen/kazoo-go"
)

type ConsumerGrouper interface {
	Errors() <-chan error
	Messages() <-chan *sarama.ConsumerMessage
	CommitUpto(message *sarama.ConsumerMessage) error
	Close() error
}

type Consumer interface {
	StartListening(messageHandler func(message FTMessage) error)
	Shutdown()
}

type MessageConsumer struct {
	topics         []string
	consumerGroup  string
	zookeeperNodes []string
	consumer       ConsumerGrouper
}

func NewConsumer(zookeeperConnectionString string, consumerGroup string, topics []string, config *consumergroup.Config) (Consumer, error) {

	if config != nil {
		config = DefaultConsumerConfig()
	}

	var zookeeperNodes []string

	zookeeperNodes, config.Zookeeper.Chroot = kazoo.ParseConnectionString(zookeeperConnectionString)

	consumer, err := consumergroup.JoinConsumerGroup(consumerGroup, topics, zookeeperNodes, config)
	if err != nil {
		log.WithError(err).WithField("method", "NewConsumer").Error("Error creating Kafka consumer")
		return &MessageConsumer{}, err
	}

	return &MessageConsumer{
		topics:         topics,
		consumerGroup:  consumerGroup,
		zookeeperNodes: zookeeperNodes,
		consumer:       consumer,
	}, nil
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

func DefaultConsumerConfig() *consumergroup.Config {
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second
	return config
}
