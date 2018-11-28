package kafka

import (
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Financial-Times/kafka/consumergroup"
	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

const (
	zookeeperConnectionString = "127.0.0.1:2181"
	testConsumerGroup         = "testgroup"
)

var expectedErrors = []error{errors.New("Booster Separation Failure"), errors.New("Payload missing")}
var messages = []*sarama.ConsumerMessage{{Value: []byte("Message1")}, {Value: []byte("Message2")}}

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

	consumer, err := NewPerseverantConsumer(zookeeperConnectionString, testConsumerGroup, []string{testTopic}, nil, time.Second, nil)
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
	messages        []*sarama.ConsumerMessage
	errors          []error
	committedCount  int
	IsShutdown      bool
	errorOnShutdown bool
}

func (cg *MockConsumerGroup) Errors() <-chan error {
	outChan := make(chan error, 100)
	go func() {
		defer close(outChan)
		for _, v := range cg.errors {
			outChan <- v
		}
	}()
	return outChan
}

func (cg *MockConsumerGroup) Messages() <-chan *sarama.ConsumerMessage {
	outChan := make(chan *sarama.ConsumerMessage, 100)
	go func() {
		defer close(outChan)
		for _, v := range cg.messages {
			outChan <- v
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
	errCh := make(chan error, len(expectedErrors))
	return &MessageConsumer{
		topics:         []string{"topic"},
		consumerGroup:  "group",
		zookeeperNodes: []string{"node"},
		consumer: &MockConsumerGroup{
			messages:        messages,
			errors:          []error{},
			IsShutdown:      false,
			errorOnShutdown: true,
		},
		errCh: errCh,
	}, errCh
}

func NewTestConsumerWithErrors() (Consumer, chan error) {
	errCh := make(chan error, len(expectedErrors))
	return &MessageConsumer{
		topics:         []string{"topic"},
		consumerGroup:  "group",
		zookeeperNodes: []string{"node"},
		consumer: &MockConsumerGroup{
			messages:   messages,
			errors:     expectedErrors,
			IsShutdown: false,
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
			messages:   messages,
			errors:     []error{},
			IsShutdown: false,
		},
	}
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

func TestMessageConsumer_StartListeningConsumerErrors(t *testing.T) {
	var count int32
	consumer, errChan := NewTestConsumerWithErrors()
	defer close(errChan)

	var actualErrors []error
	stopChan := make(chan struct{})
	stoppedChan := make(chan struct{})
	go func() {
		defer close(stoppedChan)
		for {
			select {
			case actualError := <-errChan:
				actualErrors = append(actualErrors, actualError)
			case <-stopChan:
				return
			}
		}
	}()

	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	time.Sleep(1 * time.Second)

	close(stopChan)
	<-stoppedChan
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))
	assert.Equal(t, expectedErrors, actualErrors, "Didn't get the expected errors from the consumer.")
}

func TestMessageConsumer_StartListeningHandlerErrors(t *testing.T) {
	var count int32
	consumer, errChan := NewTestConsumerWithErrChan()
	defer close(errChan)

	var actualErrors []error
	stopChan := make(chan struct{})
	stoppedChan := make(chan struct{})
	go func() {
		defer close(stoppedChan)
		for {
			select {
			case actualError := <-errChan:
				actualErrors = append(actualErrors, actualError)
			case <-stopChan:
				return
			}
		}
	}()

	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return expectedErrors[atomic.LoadInt32(&count)-1]
	})
	time.Sleep(1 * time.Second)

	close(stopChan)
	<-stoppedChan
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))
	assert.Equal(t, expectedErrors, actualErrors, "Didn't get the expected errors from the consumer handler.")
}

func TestMessageConsumer_StartListening(t *testing.T) {
	var count int32
	consumer := NewTestConsumer()
	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))
}

func TestMessageConsumerContinuesWhenHandlerReturnsError(t *testing.T) {
	var count int32
	consumer := NewTestConsumer()
	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return errors.New("test error")
	})
	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))
}

func TestPerseverantConsumerListensToConsumer(t *testing.T) {
	var count int32
	consumer := perseverantConsumer{consumer: NewTestConsumer()}
	consumer.StartListening(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return nil
	})
	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))

	consumer.Shutdown()
}

func TestPerserverantConsumer__Consume(t *testing.T) {
	var count int32
	consumer := perseverantConsumer{consumer: NewTestConsumer()}

	consumer.Consume(func(msg FTMessage) error {
		atomic.AddInt32(&count, 1)
		return nil
	})

	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))

	consumer.Shutdown()
}
