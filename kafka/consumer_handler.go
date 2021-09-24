package kafka

import (
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
)

// consumerHandler represents a Sarama consumer group consumer.
type consumerHandler struct {
	ready   chan struct{}
	logger  *logger.UPPLogger
	handler func(message FTMessage)
}

// newConsumerHandler creates a new consumerHandler.
func newConsumerHandler(logger *logger.UPPLogger, handler func(message FTMessage)) *consumerHandler {
	return &consumerHandler{
		ready:   make(chan struct{}),
		logger:  logger,
		handler: handler,
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim.
func (c *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	c.ready <- struct{}{}
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited.
func (c *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim claims a single topic partition and handles the messages that get added in it.
func (c *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c.logger.
		WithField("method", "ConsumeClaim").
		Infof("Claimed partition %d", claim.Partition())

	for message := range claim.Messages() {
		ftMsg := rawToFTMessage(message.Value)
		c.handler(ftMsg)
		session.MarkMessage(message, "")
	}

	c.logger.
		WithField("method", "ConsumeClaim").
		Infof("Released claim on partition %d", claim.Partition())

	return nil
}
