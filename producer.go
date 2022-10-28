package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
)

var ErrProducerNotConnected = fmt.Errorf("producer is not connected to Kafka")

type messageValidator interface {
	Validate(schemaName string, schemaVersion int64, payload interface{}) error
}

// Producer which will keep trying to reconnect to Kafka on a specified interval.
// The underlying producer is created in a separate go-routine when the Producer is initialized.
type Producer struct {
	config        ProducerConfig
	producerLock  *sync.RWMutex
	producer      sarama.SyncProducer
	validatorLock *sync.RWMutex
	validator     messageValidator
	logger        *logger.UPPLogger
}

type ProducerConfig struct {
	BrokersConnectionString string
	Topic                   string
	// Time interval between each connection attempt.
	// Only used for subsequent attempts if the initial one fails.
	// Default value (1 minute) would be used if not set or exceeds 5 minutes.
	ConnectionRetryInterval time.Duration
	SchemaRegistry          string
	Options                 *sarama.Config
}

func NewProducer(config ProducerConfig, logger *logger.UPPLogger) *Producer {
	producer := &Producer{
		config:        config,
		producerLock:  &sync.RWMutex{},
		validatorLock: &sync.RWMutex{},
		logger:        logger,
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
	if connectionRetryInterval <= 0 || connectionRetryInterval > maxConnectionRetryInterval {
		connectionRetryInterval = defaultConnectionRetryInterval
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

	if p.config.SchemaRegistry == "" {
		return
	}

	// TODO: Revamp connection and change public APIs
	for {
		ctx := context.Background()

		validator, err := newSchemaValidator(ctx, p.config.SchemaRegistry)
		if err == nil {
			log.Info("Created schema validator")
			p.setValidator(validator)
			return
		}

		log.WithError(err).Warn("Error creating schema validator")
		time.Sleep(connectionRetryInterval)
	}
}

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

func (p *Producer) setValidator(validator messageValidator) {
	p.validatorLock.Lock()
	defer p.validatorLock.Unlock()

	p.validator = validator
}

func (p *Producer) isValidatorConnected() bool {
	p.validatorLock.RLock()
	defer p.validatorLock.RUnlock()

	return p.validator != nil
}

func (p *Producer) validateMessage(payload interface{}, headers Headers) error {
	if !p.isValidatorConnected() {
		return fmt.Errorf("validator not connected")
	}

	schemaName, ok := headers[SchemaNameHeader]
	if !ok {
		return fmt.Errorf("schema not provided")
	}

	schemaVersion, ok := headers[SchemaVersionHeader]
	if !ok {
		return fmt.Errorf("version not provided for schema: %s", schemaName)
	}

	version, err := strconv.Atoi(schemaVersion)
	if err != nil {
		return fmt.Errorf("invalid schema version: %s", schemaVersion)
	}

	return p.validator.Validate(schemaName, int64(version), payload)
}

// SendMessage checks if the producer is connected and sends a message to Kafka.
func (p *Producer) SendMessage(key string, value interface{}, headers Headers) error {
	if !p.isConnected() {
		return ErrProducerNotConnected
	}

	// Validate the message if schema is configured.
	if p.config.SchemaRegistry != "" {
		if err := p.validateMessage(value, headers); err != nil {
			return fmt.Errorf("validating message: %w", err)
		}
	}

	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshalling message body: %w", err)
	}

	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic:   p.config.Topic,
		Headers: headers.toRecordHeaders(),
		Key:     sarama.StringEncoder(key),
		Value:   sarama.ByteEncoder(payload),
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
