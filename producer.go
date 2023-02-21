package kafka

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

type Producer struct {
	config           ProducerConfig
	producer         sarama.SyncProducer
	clusterDescriber clusterDescriber
}

type ProducerConfig struct {
	ClusterArn              *string
	BrokersConnectionString string
	Topic                   string
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
		clusterDescriber: describer,
	}, nil
}

// SendMessage publishes a message to Kafka.
func (p *Producer) SendMessage(message FTMessage) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.config.Topic,
		Value: sarama.StringEncoder(message.Build()),
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
