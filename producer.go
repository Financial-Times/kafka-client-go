package kafka

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
)

var ErrProducerNotConnected = fmt.Errorf("producer is not connected to Kafka")

// Producer which will keep trying to reconnect to Kafka on a specified interval.
// The underlying producer is created in a separate go-routine when the Producer is initialized.
type Producer struct {
	config       ProducerConfig
	producerLock *sync.RWMutex
	producer     sarama.SyncProducer
	logger       *logger.UPPLogger
}

type ProducerConfig struct {
	BrokersConnectionString string
	Topic                   string
	// Time interval between each connection attempt.
	// Only used for subsequent attempts if the initial one fails.
	// Default is 1 minute. Maximum is 5 minutes.
	ConnectionRetryInterval time.Duration
	Options                 *sarama.Config
}

func NewProducer(config ProducerConfig, logger *logger.UPPLogger) *Producer {
	producer := &Producer{
		config:       config,
		producerLock: &sync.RWMutex{},
		logger:       logger,
	}

	go producer.connect()

	return producer
}

// connect tries to establish a connection to Kafka and will retry endlessly.
func (p *Producer) connect() {
	log := p.logger.
		WithField("brokers", p.config.BrokersConnectionString).
		WithField("topic", p.config.Topic)

	connectionRetryInterval := p.config.ConnectionRetryInterval
	if connectionRetryInterval <= 0 || connectionRetryInterval > 5*time.Minute {
		connectionRetryInterval = time.Minute
	}

	for {
		producer, err := newProducer(p.config)
		if err == nil {
			log.Info("Connected to Kafka producer")
			p.setProducer(producer)
			break
		}

		log.WithError(err).Warn("Error creating Kafka producer")
		time.Sleep(connectionRetryInterval)
	}
}

// setProducer sets the underlying producer instance.
func (p *Producer) setProducer(producer sarama.SyncProducer) {
	p.producerLock.Lock()
	defer p.producerLock.Unlock()

	p.producer = producer
}

// isConnected returns whether the producer is set.
// It is only set if a successful connection is established.
func (p *Producer) isConnected() bool {
	p.producerLock.RLock()
	defer p.producerLock.RUnlock()

	return p.producer != nil
}

// SendMessage checks if the producer is connected, then sends a message to Kafka.
func (p *Producer) SendMessage(message FTMessage) error {
	if !p.isConnected() {
		return ErrProducerNotConnected
	}

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.config.Topic,
		Value: sarama.StringEncoder(message.Build()),
	})

	return err
}

// Close closes the connection to Kafka if the producer is connected.
func (p *Producer) Close() error {
	if p.isConnected() {
		return p.producer.Close()
	}

	return nil
}

// ConnectivityCheck checks whether a connection to Kafka can be established.
func (p *Producer) ConnectivityCheck() error {
	if !p.isConnected() {
		return ErrProducerNotConnected
	}

	producer, err := newProducer(p.config)
	if err != nil {
		return err
	}

	_ = producer.Close()

	return nil
}

func newProducer(config ProducerConfig) (sarama.SyncProducer, error) {
	if config.Options == nil {
		config.Options = DefaultProducerOptions()
	}

	brokers := strings.Split(config.BrokersConnectionString, ",")
	return sarama.NewSyncProducer(brokers, config.Options)
}

// DefaultProducerOptions creates a new Sarama producer configuration with default values.
func DefaultProducerOptions() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.MaxMessageBytes = 16777216
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	return config
}
