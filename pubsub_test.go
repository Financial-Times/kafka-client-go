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

	producer := newTestProducer(t, e2eTestTopic)
	require.NoError(t, producer.ConnectivityCheck())

	producedMessages := []FTMessage{
		{
			Headers: map[string]string{},
			Body:    "message 1",
			Topic:   e2eTestTopic,
		},
		{
			Headers: map[string]string{},
			Body:    "message 2",
			Topic:   e2eTestTopic,
		},
	}

	consumedMessagesLock := &sync.RWMutex{}
	consumedMessages := []FTMessage{}

	messageHandler := func(message FTMessage) {
		consumedMessagesLock.Lock()
		consumedMessages = append(consumedMessages, message)
		consumedMessagesLock.Unlock()
	}

	consumer := newTestConsumer(t, e2eTestTopic)
	require.NoError(t, consumer.ConnectivityCheck())

	consumer.Start(messageHandler)
	time.Sleep(5 * time.Second) // Let partition claiming take place.

	for _, message := range producedMessages {
		assert.NoError(t, producer.SendMessage(message))
	}

	time.Sleep(5 * time.Second) // Let message handling take place.

	consumedMessagesLock.RLock()
	assert.Equal(t, producedMessages, consumedMessages)
	consumedMessagesLock.RUnlock()

	assert.NoError(t, producer.Close())
	assert.NoError(t, consumer.Close())
}
