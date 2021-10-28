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

	producedMessages := []FTMessage{
		NewFTMessage(map[string]string{}, "message 1"),
		NewFTMessage(map[string]string{}, "message 2"),
	}

	consumedMessagesLock := &sync.RWMutex{}
	consumedMessages := []FTMessage{}

	messageHandler := func(message FTMessage) {
		consumedMessagesLock.Lock()
		consumedMessages = append(consumedMessages, message)
		consumedMessagesLock.Unlock()
	}

	consumer := NewKafkaConsumer(e2eTestTopic)

	go consumer.StartListening(messageHandler)
	time.Sleep(5 * time.Second) // Let partition claiming take place.

	require.NoError(t, consumer.ConnectivityCheck())

	for _, message := range producedMessages {
		assert.NoError(t, producer.SendMessage(message))
	}

	time.Sleep(time.Second) // Let message handling take place.

	consumedMessagesLock.RLock()
	assert.Equal(t, producedMessages, consumedMessages)
	consumedMessagesLock.RUnlock()

	assert.NoError(t, producer.Close())
	assert.NoError(t, consumer.Close())
}
