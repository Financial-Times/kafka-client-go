package kafka

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBrokers = "localhost:29092"
	testTopic   = "testTopic"
)

func NewMockProducer(t *testing.T, brokers string, topic string) *Producer {
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	return &Producer{
		config: ProducerConfig{
			BrokersConnectionString: brokers,
			Topic:                   topic,
		},
		producerLock: &sync.RWMutex{},
		producer:     producer,
	}
}

func TestProducer_SendMessage(t *testing.T) {
	producer := NewMockProducer(t, testBrokers, testTopic)

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}
	assert.NoError(t, producer.SendMessage(msg))

	assert.NoError(t, producer.Close())
}

func TestProducer_SendMessage_NoConnection(t *testing.T) {
	producer := Producer{
		producerLock: &sync.RWMutex{},
	}

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}

	err := producer.SendMessage(msg)
	assert.EqualError(t, err, errProducerNotConnected)
}

func TestProducer_InvalidConnection(t *testing.T) {
	server := httptest.NewServer(nil)
	kURL := server.URL[strings.LastIndex(server.URL, "/")+1:]
	server.Close()

	config := ProducerConfig{
		BrokersConnectionString: kURL,
		Topic:                   testTopic,
		Options:                 DefaultProducerOptions(),
	}

	_, err := newProducer(config)
	assert.Error(t, err)
}

func NewKafkaProducer(topic string) *Producer {
	log := logger.NewUPPLogger("test", "INFO")
	config := ProducerConfig{
		BrokersConnectionString: testBrokers,
		Topic:                   topic,
		Options:                 DefaultProducerOptions(),
	}

	producer := NewProducer(config, log, 0, time.Second)

	time.Sleep(time.Second) // Let connection take place.

	return producer
}

func TestProducer_KafkaConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer := NewKafkaProducer(testTopic)
	require.NoError(t, producer.ConnectivityCheck())

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}
	assert.NoError(t, producer.SendMessage(msg))

	assert.NoError(t, producer.Close())
}
