package kafka

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const e2eTestTopic = "e2eTestTopic"

type testMessage struct {
	key     string
	value   map[string]string
	headers map[string]string
}

func TestE2EPubSub(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer := NewKafkaProducer(e2eTestTopic)
	require.NoError(t, producer.ConnectivityCheck())

	consumedMessagesLock := &sync.RWMutex{}
	consumedMessages := []testMessage{}

	messageHandler := func(message Message) {
		consumedMessagesLock.Lock()
		defer consumedMessagesLock.Unlock()

		var value map[string]string
		require.NoError(t, json.Unmarshal(message.Value, &value))

		consumedMessages = append(consumedMessages, testMessage{
			key:     message.Key,
			value:   value,
			headers: message.Headers,
		})
	}

	consumer := NewKafkaConsumer(e2eTestTopic)

	go consumer.Start(messageHandler)
	time.Sleep(5 * time.Second) // Let partition claiming take place.

	require.NoError(t, consumer.ConnectivityCheck())

	producedMessages := []testMessage{
		{
			key: "id1",
			value: map[string]string{
				"data": "message 1",
			},
			headers: map[string]string{
				"key1": "value1",
			},
		},
		{
			key: "id2",
			value: map[string]string{
				"data": "message 2",
			},
			headers: map[string]string{
				"key2": "value2",
				"key3": "value3",
			},
		},
	}
	for _, message := range producedMessages {
		assert.NoError(t, producer.SendMessage(message.key, message.value, message.headers))
	}

	time.Sleep(time.Second) // Let message handling take place.

	consumedMessagesLock.RLock()
	assert.Equal(t, producedMessages, consumedMessages)
	consumedMessagesLock.RUnlock()

	assert.NoError(t, producer.Close())
	assert.NoError(t, consumer.Close())
}
