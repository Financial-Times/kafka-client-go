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

type mockConsumerGroupClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (c *mockConsumerGroupClaim) Topic() string {
	return ""
}

func (c *mockConsumerGroupClaim) Partition() int32 {
	return 0
}

func (c *mockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

func (c *mockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

func (c *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

type mockConsumerGroup struct {
	messages []*sarama.ConsumerMessage
}

func (cg *mockConsumerGroup) Errors() <-chan error {
	return make(chan error)
}

func (cg *mockConsumerGroup) Close() error {
	return nil
}

func (cg *mockConsumerGroup) Pause(map[string][]int32) {}

func (cg *mockConsumerGroup) Resume(map[string][]int32) {}

func (cg *mockConsumerGroup) PauseAll() {}

func (cg *mockConsumerGroup) ResumeAll() {}

func (cg *mockConsumerGroup) Consume(_ context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	messages := make(chan *sarama.ConsumerMessage, len(cg.messages))
	for _, message := range cg.messages {
		messages <- message
	}
	defer close(messages)

	ctx, cancel := context.WithCancel(context.Background())
	session := &mockConsumerGroupSession{
		ctx: ctx,
	}
	claim := &mockConsumerGroupClaim{
		messages: messages,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = handler.ConsumeClaim(session, claim)
	}()

	// Wait for message processing.
	time.Sleep(time.Second)

	cancel()
	wg.Wait()

	return nil
}

type mockConsumerGroupSession struct {
	ctx context.Context
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32 {
	return make(map[string][]int32)
}

func (m *mockConsumerGroupSession) MemberID() string {
	return ""
}

func (m *mockConsumerGroupSession) GenerationID() int32 {
	return 1
}

func (m *mockConsumerGroupSession) MarkOffset(string, int32, int64, string) {}

func (m *mockConsumerGroupSession) Commit() {}

func (m *mockConsumerGroupSession) ResetOffset(string, int32, int64, string) {}

func (m *mockConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {}

func (m *mockConsumerGroupSession) Context() context.Context {
	return m.ctx
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
		consumerGroup: &mockConsumerGroup{
			messages: messages,
		},
		monitorLock: &sync.RWMutex{},
		monitor: &consumerMonitor{
			subscriptions:         map[string][]int32{},
			consumerOffsetFetcher: &consumerOffsetFetcherMock{},
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
