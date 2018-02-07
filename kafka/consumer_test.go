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

	errCh := make(chan error, 1)
	defer close(errCh)

	consumer, err := NewConsumer(Config{
		ZookeeperConnectionString: zookeeperConnectionString,
		ConsumerGroup:             testConsumerGroup,
		Topics:                    []string{testTopic},
		ConsumerGroupConfig:       config,
		Err:                       errCh,
	})
	assert.NoError(t, err)

	err = consumer.ConnectivityCheck()
	assert.NoError(t, err)

	select {
	case actualError := <-errCh:
		assert.NotNil(t, actualError, "Was not expecting error from consumer.")
	default:
	}

	consumer.Shutdown()
}

func TestErrorDuringShutdown(t *testing.T) {
	consumer, errCh := NewTestConsumerWithErrChan()
	defer close(errCh)

	consumer.Shutdown()

	var actualError error
	select {
	case actualError = <-errCh:
		assert.NotNil(t, actualError, "Was expecting non-nil error on consumer shutdown")
	default:
		assert.NotNil(t, actualError, "Was expecting error on consumer shutdown")
	}
}

func TestConsumerNotConnectedConnectivityCheckError(t *testing.T) {
	server := httptest.NewServer(nil)
	zkUrl := server.URL[strings.LastIndex(server.URL, "/")+1:]
	server.Close()

	consumer := MessageConsumer{zookeeperNodes: []string{zkUrl}, consumerGroup: testConsumerGroup, topics: []string{testTopic}, config: nil}

	err := consumer.ConnectivityCheck()
	assert.Error(t, err)
}

func TestNewPerseverantConsumer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Zookeeper.")
	}

	consumer, err := NewPerseverantConsumer(zookeeperConnectionString, testConsumerGroup, []string{testTopic}, nil, time.Second)
	assert.NoError(t, err)

	err = consumer.ConnectivityCheck()
	assert.EqualError(t, err, errConsumerNotConnected)

	consumer.StartListening(func(msg FTMessage) error { return nil })

	err = consumer.ConnectivityCheck()
	assert.NoError(t, err)

	// avoid race condition within consumer_group library by delaying the shutdown
	// (it's not a normal case to create a consumer and immediately close it)
	time.Sleep(time.Second)

	consumer.Shutdown()
}

type MockConsumerGroup struct {
	messages        []string
	errors          []string
	committedCount  int
	IsShutdown      bool
	errorOnShutdown bool
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
	if cg.errorOnShutdown {
		return errors.New("foobar")
	}
	return nil
}

func (cg *MockConsumerGroup) Closed() bool {
	return cg.IsShutdown
}

func NewTestConsumerWithErrChan() (Consumer, chan error) {
	errCh := make(chan error, 1)
	return &MessageConsumer{
		topics:         []string{"topic"},
		consumerGroup:  "group",
		zookeeperNodes: []string{"node"},
		consumer: &MockConsumerGroup{
			messages:        []string{"Message1", "Message2"},
			errors:          []string{},
			IsShutdown:      false,
			errorOnShutdown: true,
		},
		errCh: errCh,
	}, errCh
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

func TestMessageConsumerContinuesWhenHandlerReturnsError(t *testing.T) {
	var count int32
	consumer := NewTestConsumer()
	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return errors.New("test error")
	})
	time.Sleep(1 * time.Second)
	var expected int32
	expected = 2
	assert.Equal(t, expected, atomic.LoadInt32(&count))
}

func TestPerseverantConsumerListensToConsumer(t *testing.T) {
	var count int32
	consumer := perseverantConsumer{consumer: NewTestConsumer()}
	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	time.Sleep(1 * time.Second)
	var expected int32
	expected = 2
	assert.Equal(t, expected, atomic.LoadInt32(&count))

	consumer.Shutdown()
}
