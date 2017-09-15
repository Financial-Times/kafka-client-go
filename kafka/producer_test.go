package kafka

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	testBrokers = "127.0.0.1:9092"
	testTopic   = "testTopic"
)

type mockProducer struct {
	mock.Mock
}

func (p *mockProducer) SendMessage(message FTMessage) error {
	args := p.Called(message)
	return args.Error(0)
}

func (p *mockProducer) ConnectivityCheck() error {
	args := p.Called()
	return args.Error(0)
}

func NewTestProducer(t *testing.T, brokers string, topic string) (Producer, error) {
	msp := mocks.NewSyncProducer(t, nil)
	brokerSlice := strings.Split(brokers, ",")

	msp.ExpectSendMessageAndSucceed()

	return &MessageProducer{
		brokers:  brokerSlice,
		topic:    topic,
		producer: msp,
	}, nil
}

func TestNewProducer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer, err := NewProducer(testBrokers, testTopic, DefaultProducerConfig())

	assert.NoError(t, err)

	err = producer.ConnectivityCheck()
	assert.NoError(t, err)
}

func TestNewProducerBadUrl(t *testing.T) {
	server := httptest.NewServer(nil)
	kUrl := server.URL[strings.LastIndex(server.URL, "/")+1:]
	server.Close()

	_, err := NewProducer(kUrl, testTopic, DefaultProducerConfig())

	assert.Error(t, err)
}

func TestClient_SendMessage(t *testing.T) {
	kc, _ := NewTestProducer(t, testBrokers, testTopic)
	kc.SendMessage(NewFTMessage(nil, "Body"))
}

func TestNewPerseverantProducer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer, err := NewPerseverantProducer(testBrokers, testTopic, nil, 0, time.Second)
	assert.NoError(t, err)

	time.Sleep(time.Second)

	err = producer.ConnectivityCheck()
	assert.NoError(t, err)
}

func TestNewPerseverantProducrNotConnected(t *testing.T) {
	server := httptest.NewServer(nil)
	kUrl := server.URL[strings.LastIndex(server.URL, "/")+1:]
	server.Close()

	producer, err := NewPerseverantProducer(kUrl, testTopic, nil, 0, time.Second)
	assert.NoError(t, err)

	err = producer.ConnectivityCheck()
	assert.EqualError(t, err, errProducerNotConnected)
}

func TestNewPerseverantProducerWithInitialDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer, err := NewPerseverantProducer(testBrokers, testTopic, nil, time.Second, time.Second)
	assert.NoError(t, err)

	err = producer.ConnectivityCheck()
	assert.EqualError(t, err, errProducerNotConnected)

	time.Sleep(2 * time.Second)
	err = producer.ConnectivityCheck()
	assert.NoError(t, err)
}

func TestPerseverantProducerForwardsToProducer(t *testing.T) {
	mp := mockProducer{}
	mp.On("SendMessage", mock.AnythingOfType("kafka.FTMessage")).Return(nil)
	p := perseverantProducer{producer: &mp}

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}

	actual := p.SendMessage(msg)
	assert.NoError(t, actual)
	mp.AssertExpectations(t)
}

func TestPerseverantProducerNotConnectedCannotSendMessages(t *testing.T) {
	p := perseverantProducer{}

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}

	actual := p.SendMessage(msg)
	assert.EqualError(t, actual, errProducerNotConnected)
}
