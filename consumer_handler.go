package kafka

import "github.com/Shopify/sarama"

// consumerHandler represents a Sarama consumer group consumer.
type consumerHandler struct {
	subscriptions chan *subscriptionEvent
	handler       func(message FTMessage)
}

func newConsumerHandler(subscriptions chan *subscriptionEvent, handler func(message FTMessage)) *consumerHandler {
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
	defer func() {
		c.subscriptions <- &subscriptionEvent{
			subscribed: false,
			topic:      topic,
			partition:  partition,
		}
	}()

	for {
		select {
		case message := <-claim.Messages():
			ftMsg := rawToFTMessage(message.Value, topic)
			c.handler(ftMsg)
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
