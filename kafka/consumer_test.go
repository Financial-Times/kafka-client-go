package kafka

import (
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wvanbergen/kafka/consumergroup"
)

const (
	zookeeperConnectionString = "127.0.0.1:2181"
	testConsumerGroup         = "testgroup"
)

func TestNewConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Zookeeper.")
	}
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetNewest
	config.Offsets.ProcessingTimeout = 10 * time.Second

	consumer, err := NewConsumer(zookeeperConnectionString, testConsumerGroup, []string{testTopic}, config)
	assert.NoError(t, err)

	err = consumer.ConnectivityCheck()
	assert.NoError(t, err)

	consumer.Shutdown()
}

func TestNewPerseverantConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Zookeeper.")
	}

	consumer, err := NewPerseverantConsumer(zookeeperConnectionString, testConsumerGroup, []string{testTopic}, nil, 0, time.Second)
	assert.NoError(t, err)

	consumer.Shutdown()
}

func TestNewPerseverantConsumerNotConnected(t *testing.T) {
	server := httptest.NewServer(nil)
	zkUrl := server.URL[strings.LastIndex(server.URL, "/")+1:]
	server.Close()

	consumer, err := NewPerseverantConsumer(zkUrl, testConsumerGroup, []string{testTopic}, nil, 0, time.Second)
	assert.NoError(t, err)

	err = consumer.ConnectivityCheck()
	assert.EqualError(t, err, errConsumerNotConnected)

	consumer.Shutdown()
}

func TestNewPerseverantConsumerWithInitialDelay(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Zookeeper.")
	}

	consumer, err := NewPerseverantConsumer(zookeeperConnectionString, testConsumerGroup, []string{testTopic}, nil, time.Second, time.Second)
	assert.NoError(t, err)

	err = consumer.ConnectivityCheck()
	assert.EqualError(t, err, errConsumerNotConnected)

	time.Sleep(2 * time.Second)
	err = consumer.ConnectivityCheck()
	assert.NoError(t, err)

	consumer.Shutdown()
}

type MockConsumerGroup struct {
	messages       []string
	errors         []string
	committedCount int
	IsShutdown     bool
}

func (cg *MockConsumerGroup) Errors() <-chan error {
	outChan := make(chan error, 100)
	go func() {
		defer close(outChan)
		for _, v := range cg.errors {
			outChan <- errors.New(v)
		}
	}()
	return outChan
}

func (cg *MockConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	outChan := make(chan *sarama.ConsumerMessage, 100)
	go func() {
		defer close(outChan)
		for _, v := range cg.messages {
			outChan <- &sarama.ConsumerMessage{
				Value: []byte(v),
			}
		}
	}()

	return outChan
}

func (cg *MockConsumerGroup) CommitUpto(message *sarama.ConsumerMessage) error {

	return nil
}
func (cg *MockConsumerGroup) Close() error {
	cg.IsShutdown = true
	return nil
}
func (cg *MockConsumerGroup) Closed() bool {
	return cg.IsShutdown
}

func NewTestConsumer() Consumer {
	return &MessageConsumer{
		topics:         []string{"topic"},
		consumerGroup:  "group",
		zookeeperNodes: []string{"node"},
		consumer: &MockConsumerGroup{
			messages:   []string{"Message1", "Message2"},
			errors:     []string{},
			IsShutdown: false,
		},
	}
}

func TestMessageConsumer_StartListening(t *testing.T) {
	var count int32
	consumer := NewTestConsumer()
	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	time.Sleep(1 * time.Second)
	var expected int32
	expected = 2
	assert.Equal(t, expected, atomic.LoadInt32(&count))
}
