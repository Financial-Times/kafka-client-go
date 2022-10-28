package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
)

type messageValidator interface {
	Validate(schemaName string, schemaVersion int64, payload interface{}) error
}

type Producer struct {
	config           ProducerConfig
	producer         sarama.SyncProducer
	validator        messageValidator
	clusterDescriber clusterDescriber
}

type ProducerConfig struct {
	ClusterArn              *string
	BrokersConnectionString string
	Topic                   string
	SchemaRegistry          string
	Options                 *sarama.Config
}

func NewProducer(config ProducerConfig) (*Producer, error) {
	if config.Options == nil {
		config.Options = DefaultProducerOptions()
	}

	brokers := strings.Split(config.BrokersConnectionString, ",")

	producer, err := sarama.NewSyncProducer(brokers, config.Options)
	if err != nil {
		return nil, fmt.Errorf("creating producer: %w", err)
	}

	var validator messageValidator

	if config.SchemaRegistry != "" {
		validator, err = newSchemaValidator(context.Background(), config.SchemaRegistry)
		if err != nil {
			return nil, fmt.Errorf("creating schema validator: %w", err)
		}
	}

	var describer clusterDescriber
	if config.ClusterArn != nil {
		describer, err = newClusterDescriber(config.ClusterArn)
		if err != nil {
			return nil, fmt.Errorf("creating cluster describer: %w", err)
		}
	}

	return &Producer{
		config:           config,
		producer:         producer,
		validator:        validator,
		clusterDescriber: describer,
	}, nil
}

func (p *Producer) validateMessage(payload interface{}, headers Headers) error {
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

// Close closes the connection to Kafka.
func (p *Producer) Close() error {
	return p.producer.Close()
}

// ConnectivityCheck checks whether a connection to Kafka can be established.
func (p *Producer) ConnectivityCheck() error {
	brokers := strings.Split(p.config.BrokersConnectionString, ",")

	if err := checkConnectivity(brokers); err != nil {
		if p.config.ClusterArn != nil {
			return verifyHealthErrorSeverity(err, p.clusterDescriber, p.config.ClusterArn)
		}

		return err
	}

	return nil
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
