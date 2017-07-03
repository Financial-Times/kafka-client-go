package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
)

type Producer interface {
	SendMessage(message FTMessage) error
}

type MessageProducer struct {
	brokers  []string
	topic    string
	producer sarama.SyncProducer
}

func NewProducer(brokers string, topic string, config *sarama.Config) (Producer, error) {

	if config == nil {
		config = DefaultProducerConfig()
	}

	brokerSlice := strings.Split(brokers, ",")

	sp, err := sarama.NewSyncProducer(brokerSlice, config)
	if err != nil {
		log.WithError(err).WithField("method", "NewProducer").Error("Error creating the producer")
		return &MessageProducer{}, err
	}

	return &MessageProducer{
		brokers:  brokerSlice,
		topic:    topic,
		producer: sp,
	}, nil
}

func (c *MessageProducer) SendMessage(message FTMessage) error {
	_, _, err := c.producer.SendMessage(&sarama.ProducerMessage{
		Topic: c.topic,
		Value: sarama.StringEncoder(message.Build()),
	})
	if err != nil {
		log.WithError(err).WithField("method", "SendMessage").Error("Error sending a Kafka message")
	}
	return err
}

func DefaultProducerConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return config
}
