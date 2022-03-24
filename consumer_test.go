package kafka

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConsumerGroup = "testGroup"

var messages = []*sarama.ConsumerMessage{{Value: []byte("Message1")}, {Value: []byte("Message2")}}

func TestConsumerGroup_KafkaConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	config := ConsumerConfig{
		BrokersConnectionString: testBrokers,
		ConsumerGroup:           testConsumerGroup,
		Options:                 DefaultConsumerOptions(),
	}

	consumerGroup, err := newConsumerGroup(config)
	require.NoError(t, err)

	assert.NoError(t, consumerGroup.Close())
}

func TestConsumer_InvalidConnection(t *testing.T) {
	log := logger.NewUPPLogger("test", "INFO")
	consumer := Consumer{
		config: ConsumerConfig{
			BrokersConnectionString: "unknown:9092",
			ConsumerGroup:           testConsumerGroup,
		},
		topics:            []*Topic{NewTopic(testTopic)},
		consumerGroupLock: &sync.RWMutex{},
		logger:            log,
	}

	assert.Error(t, consumer.ConnectivityCheck())
}

func NewKafkaConsumer(topic string) *Consumer {
	log := logger.NewUPPLogger("test", "INFO")
	config := ConsumerConfig{
		BrokersConnectionString: testBrokers,
		ConsumerGroup:           testConsumerGroup,
		ConnectionRetryInterval: time.Second,
		Options:                 DefaultConsumerOptions(),
	}
	topics := []*Topic{NewTopic(topic)}

	return NewConsumer(config, topics, log)
}

func TestKafkaConsumer_Start(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	consumer := NewKafkaConsumer(testTopic)

	go consumer.Start(func(msg FTMessage) {})
	time.Sleep(5 * time.Second)

	require.NoError(t, consumer.ConnectivityCheck())

	assert.NoError(t, consumer.Close())
}

type MockConsumerGroupClaim struct {
	messages []*sarama.ConsumerMessage
}

func (c *MockConsumerGroupClaim) Topic() string {
	return ""
}

func (c *MockConsumerGroupClaim) Partition() int32 {
	return 0
}

func (c *MockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

func (c *MockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

func (c *MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	outChan := make(chan *sarama.ConsumerMessage, len(c.messages))
	defer close(outChan)

	for _, v := range c.messages {
		outChan <- v
	}

	return outChan
}

type MockConsumerGroup struct {
	messages []*sarama.ConsumerMessage
}

func (cg *MockConsumerGroup) Errors() <-chan error {
	return make(chan error)
}

func (cg *MockConsumerGroup) Close() error {
	return nil
}

func (cg *MockConsumerGroup) Pause(map[string][]int32) {}

func (cg *MockConsumerGroup) Resume(map[string][]int32) {}

func (cg *MockConsumerGroup) PauseAll() {}

func (cg *MockConsumerGroup) ResumeAll() {}

func (cg *MockConsumerGroup) Consume(_ context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	for _, v := range cg.messages {
		session := &MockConsumerGroupSession{}
		claim := &MockConsumerGroupClaim{
			messages: []*sarama.ConsumerMessage{v},
		}

		_ = handler.ConsumeClaim(session, claim)
	}

	// We block here to simulate the behavior of the library
	c := make(chan struct{})
	<-c
	return nil
}

type MockConsumerGroupSession struct{}

func (m *MockConsumerGroupSession) Claims() map[string][]int32 {
	return make(map[string][]int32)
}

func (m *MockConsumerGroupSession) MemberID() string {
	return ""
}

func (m *MockConsumerGroupSession) GenerationID() int32 {
	return 1
}

func (m *MockConsumerGroupSession) MarkOffset(string, int32, int64, string) {}

func (m *MockConsumerGroupSession) Commit() {}

func (m *MockConsumerGroupSession) ResetOffset(string, int32, int64, string) {}

func (m *MockConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {}

func (m *MockConsumerGroupSession) Context() context.Context {
	return context.Background()
}

func NewMockConsumer() *Consumer {
	log := logger.NewUPPLogger("test", "INFO")

	return &Consumer{
		config: ConsumerConfig{
			ConsumerGroup:           "group",
			BrokersConnectionString: "node",
		},
		topics: []*Topic{
			{
				Name: testTopic,
			},
		},
		consumerGroupLock: &sync.RWMutex{},
		consumerGroup: &MockConsumerGroup{
			messages: messages,
		},
		monitorLock: &sync.RWMutex{},
		monitor: &consumerMonitor{
			subscriptions: map[string]map[int32]int64{},
			offsetFetcher: &offsetFetcherMock{},
			scheduler: fetcherScheduler{
				standardInterval: time.Minute,
			},
			logger: log,
		},
		logger: log,
		closed: make(chan struct{}),
	}
}

func TestConsumer_Start(t *testing.T) {
	var count int32
	consumer := NewMockConsumer()

	consumer.Start(func(msg FTMessage) {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(1 * time.Second)
	assert.Equal(t, int32(len(messages)), atomic.LoadInt32(&count))

	assert.NoError(t, consumer.Close())
}
