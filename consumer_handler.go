package kafka

import (
	"context"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/IBM/sarama"
)

const (
	initialHandlerRetryDelay = time.Second
	maxHandlerRetryDelay     = 30 * time.Second
)

// ErrorHandlerRetryConfig configures retries used by StartWithErrorHandler.
// Non-positive values use the default retry delays.
type ErrorHandlerRetryConfig struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// consumerHandler represents a Sarama consumer group consumer.
type consumerHandler struct {
	subscriptions chan *subscriptionEvent
	handler       func(message FTMessage)
	errorHandler  func(message FTMessage) error
	logger        *logger.UPPLogger
	retryInitial  time.Duration
	retryMax      time.Duration
	retryWait     func(context.Context, time.Duration) bool
}

func newConsumerHandler(log *logger.UPPLogger, subscriptions chan *subscriptionEvent, handler func(message FTMessage), errorHandler func(message FTMessage) error, retryConfig *ErrorHandlerRetryConfig) *consumerHandler {
	var retryInitial, retryMax time.Duration
	if retryConfig != nil {
		retryInitial = retryConfig.InitialDelay
		retryMax = retryConfig.MaxDelay
	}
	if retryInitial <= 0 {
		retryInitial = initialHandlerRetryDelay
	}
	if retryMax <= 0 {
		retryMax = maxHandlerRetryDelay
	}
	if retryInitial > retryMax {
		retryInitial = retryMax
	}

	return &consumerHandler{
		subscriptions: subscriptions,
		handler:       handler,
		errorHandler:  errorHandler,
		logger:        log,
		retryInitial:  retryInitial,
		retryMax:      retryMax,
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
			if c.errorHandler != nil {
				if !c.handleWithRetry(session, message, ftMsg) {
					return nil
				}
				continue
			}
			c.handler(ftMsg)
			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}

// handleWithRetry holds the current partition at the failed message. It doesn't
// read from the claim again until the handler succeeds, preserving offset order.
func (c *consumerHandler) handleWithRetry(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage, ftMsg FTMessage) bool {
	delay := c.retryInitial
	for {
		if err := c.errorHandler(ftMsg); err == nil {
			session.MarkMessage(message, "")
			return true
		} else if c.logger != nil {
			c.logger.WithError(err).WithField("topic", message.Topic).WithField("partition", message.Partition).WithField("offset", message.Offset).Error("Message handler failed; retrying")
		}

		if !c.waitBeforeRetry(session.Context(), delay) {
			return false
		}

		delay *= 2
		if delay > c.retryMax {
			delay = c.retryMax
		}
	}
}

func (c *consumerHandler) waitBeforeRetry(ctx context.Context, delay time.Duration) bool {
	if c.retryWait != nil {
		return c.retryWait(ctx, delay)
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
