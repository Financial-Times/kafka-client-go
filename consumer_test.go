package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/IBM/sarama"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConsumerGroup = "testGroup"
const testARN = "arn:aws:kafka:eu-west-1:123456789012:cluster/testCluster/12345678-1234-1234-1234-123456789012-1"

var testMessages = []*sarama.ConsumerMessage{{Value: []byte("Message1")}, {Value: []byte("Message2")}}

type clusterDescriberMock struct {
	describeCluster func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error)
}

func strAsPtr(s string) *string {
	return &s
}

func (cd *clusterDescriberMock) DescribeClusterV2(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error) {
	if cd.describeCluster != nil {
		return cd.describeCluster(ctx, input, optFns...)
	}

	panic("DescribeClusterV2 not implemented")
}

func newTestConsumer(t *testing.T, topic string) *Consumer {
	log := logger.NewUPPLogger("test", "INFO")
	config := ConsumerConfig{
		BrokersConnectionString: testBrokers,
		ConsumerGroup:           testConsumerGroup,
		Options:                 DefaultConsumerOptions(),
	}
	topics := []*Topic{NewTopic(topic)}

	consumer, err := NewConsumer(config, topics, log)
	require.NoError(t, err)

	return consumer
}

func TestConsumer_Connectivity(t *testing.T) {
	tests := []struct {
		name               string
		requiresConnection bool
		newConsumer        func() *Consumer
		expectedErr        string
	}{
		{
			name:               "valid connection",
			requiresConnection: true,
			newConsumer: func() *Consumer {
				return newTestConsumer(t, testTopic)
			},
		},
		{
			name:               "invalid connection causes kafka error",
			requiresConnection: false,
			newConsumer: func() *Consumer {
				return &Consumer{
					config: ConsumerConfig{
						BrokersConnectionString: "unknown:9092",
					},
				}
			},
			expectedErr: "kafka: client has run out of available brokers to talk to",
		},
		{
			name:               "invalid connection causes cluster status error",
			requiresConnection: false,
			newConsumer: func() *Consumer {
				return &Consumer{
					config: ConsumerConfig{
						ClusterArn:              strAsPtr(testARN),
						BrokersConnectionString: "unknown:9092",
					},
					clusterDescriber: &clusterDescriberMock{
						describeCluster: func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error) {
							return nil, fmt.Errorf("status error")
						},
					},
				}
			},
			expectedErr: "cluster status is unknown: status error",
		},
		{
			name:               "invalid connection is ignored during cluster maintenance",
			requiresConnection: false,
			newConsumer: func() *Consumer {
				return &Consumer{
					config: ConsumerConfig{
						ClusterArn:              strAsPtr(testARN),
						BrokersConnectionString: "unknown:9092",
					},
					clusterDescriber: &clusterDescriberMock{
						describeCluster: func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error) {
							return &kafka.DescribeClusterV2Output{
								ClusterInfo: &types.Cluster{
									State: types.ClusterStateMaintenance,
								},
							}, nil
						},
					},
				}
			},
			expectedErr: "",
		},
		{
			name:               "invalid connection causes kafka error when the cluster is active",
			requiresConnection: false,
			newConsumer: func() *Consumer {
				return &Consumer{
					config: ConsumerConfig{
						ClusterArn:              strAsPtr(testARN),
						BrokersConnectionString: "unknown:9092",
					},
					clusterDescriber: &clusterDescriberMock{
						describeCluster: func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error) {
							return &kafka.DescribeClusterV2Output{
								ClusterInfo: &types.Cluster{
									State: types.ClusterStateActive,
								},
							}, nil
						},
					},
				}
			},
			expectedErr: "kafka: client has run out of available brokers to talk to",
		},
		{
			name:               "connectivity times out when the cluster is active",
			requiresConnection: false,
			newConsumer: func() *Consumer {
				brokerID := int32(1)
				broker := sarama.NewMockBroker(t, brokerID)
				broker.SetLatency(connectivityTimeout + time.Second)

				return &Consumer{
					config: ConsumerConfig{
						ClusterArn:              strAsPtr(testARN),
						BrokersConnectionString: broker.Addr(),
					},
					clusterDescriber: &clusterDescriberMock{
						describeCluster: func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error) {
							return &kafka.DescribeClusterV2Output{
								ClusterInfo: &types.Cluster{
									State: types.ClusterStateActive,
								},
							}, nil
						},
					},
				}
			},
			expectedErr: "kafka connectivity timed out",
		},
		{
			name:               "connectivity timeout is ignored during cluster maintenance",
			requiresConnection: false,
			newConsumer: func() *Consumer {
				brokerID := int32(1)
				broker := sarama.NewMockBroker(t, brokerID)
				broker.SetLatency(connectivityTimeout + time.Second)

				return &Consumer{
					config: ConsumerConfig{
						ClusterArn:              strAsPtr(testARN),
						BrokersConnectionString: broker.Addr(),
					},
					clusterDescriber: &clusterDescriberMock{
						describeCluster: func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error) {
							return &kafka.DescribeClusterV2Output{
								ClusterInfo: &types.Cluster{
									State: types.ClusterStateMaintenance,
								},
							}, nil
						},
					},
				}
			},
			expectedErr: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.requiresConnection && testing.Short() {
				t.Skip("Skipping test as it requires a connection to Kafka.")
				return
			}

			consumer := test.newConsumer()
			connectivityErr := consumer.ConnectivityCheck()

			if test.expectedErr != "" {
				require.Error(t, connectivityErr)
				assert.Contains(t, connectivityErr.Error(), test.expectedErr)
				return
			}

			assert.NoError(t, connectivityErr)
		})
	}
}

type mockConsumerGroupClaim struct {
	messages chan *sarama.ConsumerMessage
	topic    string
}

func (c *mockConsumerGroupClaim) Topic() string {
	return c.topic
}

func (c *mockConsumerGroupClaim) Partition() int32 {
	return 0
}

func (c *mockConsumerGroupClaim) InitialOffset() int64 {
	return 0
}

func (c *mockConsumerGroupClaim) HighWaterMarkOffset() int64 {
	return 0
}

func (c *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	return c.messages
}

type mockConsumerGroup struct {
	messages []*sarama.ConsumerMessage
}

func (cg *mockConsumerGroup) Errors() <-chan error {
	return make(chan error)
}

func (cg *mockConsumerGroup) Close() error {
	return nil
}

func (cg *mockConsumerGroup) Pause(map[string][]int32) {}

func (cg *mockConsumerGroup) Resume(map[string][]int32) {}

func (cg *mockConsumerGroup) PauseAll() {}

func (cg *mockConsumerGroup) ResumeAll() {}

func (cg *mockConsumerGroup) Consume(_ context.Context, _ []string, handler sarama.ConsumerGroupHandler) error {
	messages := make(chan *sarama.ConsumerMessage, len(cg.messages))
	for _, message := range cg.messages {
		messages <- message
	}
	defer close(messages)

	ctx, cancel := context.WithCancel(context.Background())
	session := &mockConsumerGroupSession{
		ctx: ctx,
	}
	claim := &mockConsumerGroupClaim{
		messages: messages,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = handler.ConsumeClaim(session, claim)
	}()

	// Wait for message processing.
	time.Sleep(time.Second)

	cancel()
	wg.Wait()

	return nil
}

type mockConsumerGroupSession struct {
	ctx   context.Context
	marks []*sarama.ConsumerMessage
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32 {
	return make(map[string][]int32)
}

func (m *mockConsumerGroupSession) MemberID() string {
	return ""
}

func (m *mockConsumerGroupSession) GenerationID() int32 {
	return 1
}

func (m *mockConsumerGroupSession) MarkOffset(string, int32, int64, string) {}

func (m *mockConsumerGroupSession) Commit() {}

func (m *mockConsumerGroupSession) ResetOffset(string, int32, int64, string) {}

func (m *mockConsumerGroupSession) MarkMessage(message *sarama.ConsumerMessage, _ string) {
	m.marks = append(m.marks, message)
}

func (m *mockConsumerGroupSession) Context() context.Context {
	return m.ctx
}

func newMockConsumer() *Consumer {
	log := logger.NewUPPLogger("test", "INFO")

	return &Consumer{
		consumerGroup: &mockConsumerGroup{
			messages: testMessages,
		},
		monitor: &consumerMonitor{
			subscriptions:         map[string][]int32{},
			consumerOffsetFetcher: &consumerOffsetFetcherMock{},
			scheduler: fetcherScheduler{
				standardInterval: time.Minute,
			},
			logger: log,
		},
		logger: log,
		closed: make(chan struct{}),
	}
}

func TestConsumer_Workflow(t *testing.T) {
	var count int32
	consumer := newMockConsumer()

	consumer.Start(func(msg FTMessage) {
		atomic.AddInt32(&count, 1)
	})

	time.Sleep(1 * time.Second) // Let message handling take place.

	assert.Equal(t, len(testMessages), int(atomic.LoadInt32(&count)))

	assert.NoError(t, consumer.Close())
}

func TestConsumer_WorkflowWithErrorHandlerAndDefaultRetryConfig(t *testing.T) {
	var count int32
	consumer := newMockConsumer()

	consumer.StartWithErrorHandler(func(msg FTMessageWithMetadata) error {
		atomic.AddInt32(&count, 1)
		return nil
	}, nil)

	time.Sleep(time.Second) // Let message handling take place.

	assert.Equal(t, len(testMessages), int(atomic.LoadInt32(&count)))
	assert.NoError(t, consumer.Close())
}

func TestConsumerHandler_ErrorHandlerRetriesInOrderWithCappedBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	session := &mockConsumerGroupSession{ctx: ctx}
	messages := make(chan *sarama.ConsumerMessage, 2)
	first := &sarama.ConsumerMessage{Topic: "topic", Partition: 0, Offset: 10, Value: []byte("\r\n\r\nfirst")}
	second := &sarama.ConsumerMessage{Topic: "topic", Partition: 0, Offset: 11, Value: []byte("\r\n\r\nsecond")}
	messages <- first
	messages <- second
	claim := &mockConsumerGroupClaim{messages: messages, topic: "topic"}
	log := logger.NewUPPLogger("test", "INFO")
	var calls []int64
	var retryDelays []time.Duration
	firstAttempts := 0
	secondAttempts := 0
	processed := make(chan struct{})
	handler := newConsumerHandler(log, make(chan *subscriptionEvent, 2), nil, func(message FTMessageWithMetadata) error {
		expectedOffset := int64(10)
		if message.Body == "second" {
			expectedOffset = 11
		}
		if message.Topic != "topic" || message.Partition != 0 || message.Offset != expectedOffset {
			t.Errorf("unexpected Kafka position: topic=%q partition=%d offset=%d, want topic=%q partition=0 offset=%d", message.Topic, message.Partition, message.Offset, "topic", expectedOffset)
		}
		switch message.Body {
		case "first":
			calls = append(calls, 10)
			firstAttempts++
			if firstAttempts < 8 {
				return fmt.Errorf("temporary write failure")
			}
		case "second":
			if len(session.marks) != 1 || session.marks[0] != first {
				t.Errorf("later offset handled before the earlier message was marked: marks=%v", session.marks)
			}
			calls = append(calls, 11)
			secondAttempts++
			if secondAttempts == 1 {
				return fmt.Errorf("temporary write failure")
			}
			close(processed)
		}
		return nil
	}, &ErrorHandlerRetryConfig{InitialDelay: 5 * time.Millisecond, MaxDelay: 10 * time.Millisecond})
	handler.retryWait = func(ctx context.Context, delay time.Duration) bool {
		retryDelays = append(retryDelays, delay)
		return ctx.Err() == nil
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = handler.ConsumeClaim(session, claim)
	}()
	select {
	case <-processed:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("handler did not finish retries")
	}
	cancel()
	<-done

	assert.Equal(t, []int64{10, 10, 10, 10, 10, 10, 10, 10, 11, 11}, calls)
	require.Len(t, session.marks, 2)
	assert.Same(t, first, session.marks[0])
	assert.Same(t, second, session.marks[1])
	assert.Equal(t, []time.Duration{
		5 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond,
		10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond, // capped retries
		5 * time.Millisecond, // reset after the first message succeeds
	}, retryDelays)
}

func TestConsumerHandler_ErrorHandlerFailureIsReplayedOnNewSession(t *testing.T) {
	message := &sarama.ConsumerMessage{Topic: "topic", Partition: 0, Offset: 42, Value: []byte("body")}
	firstCtx, firstCancel := context.WithCancel(context.Background())
	firstSession := &mockConsumerGroupSession{ctx: firstCtx}
	firstCalls := 0
	firstHandler := newConsumerHandler(nil, make(chan *subscriptionEvent, 2), nil, func(FTMessageWithMetadata) error {
		firstCalls++
		firstCancel()
		return fmt.Errorf("write failed")
	}, &ErrorHandlerRetryConfig{InitialDelay: time.Millisecond, MaxDelay: time.Millisecond})
	_ = firstHandler.ConsumeClaim(firstSession, &mockConsumerGroupClaim{messages: messageChannel(message)})
	assert.Equal(t, 1, firstCalls)
	assert.Empty(t, firstSession.marks)

	secondCtx, secondCancel := context.WithCancel(context.Background())
	secondSession := &mockConsumerGroupSession{ctx: secondCtx}
	secondHandler := newConsumerHandler(nil, make(chan *subscriptionEvent, 2), nil, func(FTMessageWithMetadata) error {
		secondCancel()
		return nil
	}, nil)
	_ = secondHandler.ConsumeClaim(secondSession, &mockConsumerGroupClaim{messages: messageChannel(message)})
	require.Len(t, secondSession.marks, 1)
	assert.Same(t, message, secondSession.marks[0])
}

func TestConsumerHandler_RetryDelaysAreConfigurable(t *testing.T) {
	newHandler := func(config *ErrorHandlerRetryConfig) *consumerHandler {
		return newConsumerHandler(nil, make(chan *subscriptionEvent, 1), nil, func(FTMessageWithMetadata) error { return nil }, config)
	}

	defaults := newHandler(nil)
	assert.Equal(t, time.Second, defaults.retryInitial)
	assert.Equal(t, 30*time.Second, defaults.retryMax)

	custom := newHandler(&ErrorHandlerRetryConfig{
		InitialDelay: 250 * time.Millisecond,
		MaxDelay:     4 * time.Second,
	})
	assert.Equal(t, 250*time.Millisecond, custom.retryInitial)
	assert.Equal(t, 4*time.Second, custom.retryMax)

	clamped := newHandler(&ErrorHandlerRetryConfig{
		InitialDelay: 2 * time.Second,
		MaxDelay:     time.Second,
	})
	assert.Equal(t, time.Second, clamped.retryInitial)
	assert.Equal(t, time.Second, clamped.retryMax)
}

func messageChannel(message *sarama.ConsumerMessage) chan *sarama.ConsumerMessage {
	messages := make(chan *sarama.ConsumerMessage, 1)
	messages <- message
	return messages
}
