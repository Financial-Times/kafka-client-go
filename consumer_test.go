package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testConsumerGroup = "testGroup"

var testMessages = []*sarama.ConsumerMessage{{Value: []byte("Message1")}, {Value: []byte("Message2")}}

type clusterDescriberMock struct {
	describeCluster func(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error)
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
						ClusterArn:              new(string),
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
						ClusterArn:              new(string),
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
						ClusterArn:              new(string),
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
						ClusterArn:              new(string),
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
						ClusterArn:              new(string),
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
}

func (c *mockConsumerGroupClaim) Topic() string {
	return ""
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
	ctx context.Context
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

func (m *mockConsumerGroupSession) MarkMessage(*sarama.ConsumerMessage, string) {}

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

	assert.Equal(t, int32(len(testMessages)), atomic.LoadInt32(&count))

	assert.NoError(t, consumer.Close())
}
