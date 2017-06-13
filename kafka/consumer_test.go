package kafka

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wvanbergen/kafka/consumergroup"
)

var (
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

	_, err := NewConsumer(zookeeperConnectionString, testConsumerGroup, []string{testTopic}, config)
	assert.NoError(t, err)

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
	count := 0
	consumer := NewTestConsumer()
	consumer.StartListening(func(msg FTMessage) error {
		count++
		return nil
	})
	time.Sleep(1 * time.Second)
	assert.Equal(t, 2, count)
}

func TestMessageConsumer_ConnectivityCheck(t *testing.T) {
	tc := NewTestConsumer()
	err := tc.ConnectivityCheck()
	assert.NoError(t, err)
	tc.Shutdown()
	err = tc.ConnectivityCheck()
	assert.Error(t, err)
}
