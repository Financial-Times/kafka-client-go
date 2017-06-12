package kafka

import (
	"strings"
	"testing"

	"github.com/Shopify/sarama/mocks"
	"github.com/stretchr/testify/assert"
)

const testBrokers = "test1:1,test2:2"
const testTopic = "testTopic"

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

func Test_NewKafkaClient_BrokerError(t *testing.T) {

	_, err := NewProducer(testBrokers, testTopic)

	assert.Error(t, err)
}

func TestClient_SendMessage(t *testing.T) {
	kc, _ := NewTestProducer(t, testBrokers, testTopic)
	kc.SendMessage(NewFTMessage(nil, "Body"))
}
