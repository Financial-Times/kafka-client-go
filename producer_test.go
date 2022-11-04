package kafka

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBrokers = "localhost:29092"
	testTopic   = "testTopic"
)

func newMockProducer(t *testing.T, brokers string, topic string) *Producer {
	producer := mocks.NewSyncProducer(t, nil)
	producer.ExpectSendMessageAndSucceed()

	return &Producer{
		config: ProducerConfig{
			BrokersConnectionString: brokers,
			Topic:                   topic,
		},
		producer: producer,
	}
}

func TestProducer_SendMessage(t *testing.T) {
	producer := newMockProducer(t, testBrokers, testTopic)

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}
	assert.NoError(t, producer.SendMessage(msg))

	assert.NoError(t, producer.Close())
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

	_, err := NewProducer(config)
	assert.Error(t, err)
}

func newTestProducer(t *testing.T, topic string) *Producer {
	config := ProducerConfig{
		BrokersConnectionString: testBrokers,
		Topic:                   topic,
		Options:                 DefaultProducerOptions(),
	}

	producer, err := NewProducer(config)
	require.NoError(t, err)

	return producer
}

func TestProducer_KafkaConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer := newTestProducer(t, testTopic)
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
