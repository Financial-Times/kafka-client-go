package kafka

import (
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const errProducerNotConnected = "producer is not connected to Kafka"

type Producer interface {
	SendMessage(message FTMessage) error
	ConnectivityCheck() error
}

type MessageProducer struct {
	brokers  []string
	topic    string
	config   *sarama.Config
	producer sarama.SyncProducer
}

type perseverantProducer struct {
	sync.RWMutex
	brokers  string
	topic    string
	config   *sarama.Config
	producer Producer
}

func NewProducer(brokers string, topic string, config *sarama.Config) (Producer, error) {

	if config == nil {
		config = DefaultProducerConfig()
	}

	brokerSlice := strings.Split(brokers, ",")

	sp, err := sarama.NewSyncProducer(brokerSlice, config)
	if err != nil {
		log.WithError(err).WithField("method", "NewProducer").Error("Error creating the producer")
		return nil, err
	}

	return &MessageProducer{
		brokers:  brokerSlice,
		topic:    topic,
		config:   config,
		producer: sp,
	}, nil
}

func NewPerseverantProducer(brokers string, topic string, config *sarama.Config, initialDelay time.Duration, retryInterval time.Duration) (Producer, error) {
	producer := &perseverantProducer{sync.RWMutex{}, brokers, topic, config, nil}

	go func() {
		if initialDelay > 0 {
			time.Sleep(initialDelay)
		}
		producer.connect(retryInterval)
	}()

	return producer, nil
}

func (p *MessageProducer) SendMessage(message FTMessage) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.StringEncoder(message.Build()),
	})
	if err != nil {
		log.WithError(err).WithField("method", "SendMessage").Error("Error sending a Kafka message")
	}
	return err
}

func (p *MessageProducer) ConnectivityCheck() error {
	// like the consumer check, establishing a new connection gives us some degree of confidence
	_, err := NewProducer(strings.Join(p.brokers, ","), p.topic, p.config)

	return err
}

func (p *perseverantProducer) connect(retryInterval time.Duration) {
	connectorLog := log.WithField("brokers", p.brokers).WithField("topic", p.topic)
	for {
		producer, err := NewProducer(p.brokers, p.topic, p.config)
		if err == nil {
			connectorLog.Info("connected to Kafka producer")
			p.setProducer(producer)
			break
		}

		connectorLog.WithError(err).Warn(errProducerNotConnected)
		time.Sleep(retryInterval)
	}
}

func (p *perseverantProducer) setProducer(producer Producer) {
	p.Lock()
	defer p.Unlock()

	p.producer = producer
}

func (p *perseverantProducer) isConnected() bool {
	p.RLock()
	defer p.RUnlock()

	return p.producer != nil
}

func (p *perseverantProducer) SendMessage(message FTMessage) error {
	if !p.isConnected() {
		return errors.New(errProducerNotConnected)
	}

	p.RLock()
	defer p.RUnlock()

	return p.producer.SendMessage(message)
}

func (p *perseverantProducer) ConnectivityCheck() error {
	if !p.isConnected() {
		return errors.New(errProducerNotConnected)
	}

	_, err := NewProducer(p.brokers, p.topic, p.config)

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
