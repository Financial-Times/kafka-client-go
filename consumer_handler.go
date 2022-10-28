package kafka

import "github.com/Shopify/sarama"

// consumerHandler represents a Sarama consumer group consumer.
type consumerHandler struct {
	subscriptions chan *subscriptionEvent
	handler       func(message Message)
}

func newConsumerHandler(subscriptions chan *subscriptionEvent, handler func(message Message)) *consumerHandler {
	return &consumerHandler{
		subscriptions: subscriptions,
		handler:       handler,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim claims a single topic partition and handles the messages that get added in it.
func (c *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	topic := claim.Topic()
	partition := claim.Partition()

	c.subscriptions <- &subscriptionEvent{
		subscribed: true,
		topic:      topic,
		partition:  partition,
	}

	for message := range claim.Messages() {
		c.handler(rawToMessage(message))
		session.MarkMessage(message, "")
	}

	c.subscriptions <- &subscriptionEvent{
		topic:     topic,
		partition: partition,
	}

	return nil
}
