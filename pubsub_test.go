package kafka

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2EPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	const e2eTestTopic = "e2eTestTopic"

	producer := NewKafkaProducer(e2eTestTopic)
	require.NoError(t, producer.ConnectivityCheck())

	consumedMessagesLock := &sync.RWMutex{}
	consumedMessages := []Message{}

	messageHandler := func(message Message) {
		consumedMessagesLock.Lock()
		consumedMessages = append(consumedMessages, message)
		consumedMessagesLock.Unlock()
	}

	consumer := NewKafkaConsumer(e2eTestTopic)

	go consumer.Start(messageHandler)
	time.Sleep(5 * time.Second) // Let partition claiming take place.

	require.NoError(t, consumer.ConnectivityCheck())

	producedMessages := []Message{
		{
			Key:   "id1",
			Value: []byte(`{"data":"message 1"}`),
			Headers: map[string]string{
				"key1": "value1",
			},
		},
		{
			Key:   "id2",
			Value: []byte(`{"data":"message 2"}`),
			Headers: map[string]string{
				"key2": "value2",
				"key3": "value3",
			},
		},
	}
	for _, message := range producedMessages {
		assert.NoError(t, producer.SendMessage(message.Key, message.Value, message.Headers))
	}

	time.Sleep(time.Second) // Let message handling take place.

	consumedMessagesLock.RLock()
	assert.Equal(t, producedMessages, consumedMessages)
	consumedMessagesLock.RUnlock()

	assert.NoError(t, producer.Close())
	assert.NoError(t, consumer.Close())
}
