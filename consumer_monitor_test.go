package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type offsetFetcherMock struct {
	listConsumerGroupOffsetsF func(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)
	closeF                    func() error
}

func (f *offsetFetcherMock) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	if f.listConsumerGroupOffsetsF != nil {
		return f.listConsumerGroupOffsetsF(group, topicPartitions)
	}

	panic("offsetFetcherMock.ListConsumerGroupOffsets is not implemented")
}

func (f *offsetFetcherMock) Close() error {
	if f.closeF != nil {
		return f.closeF()
	}

	return nil
}

func TestConsumerMonitor_Lifecycle(t *testing.T) {
	offsetFetcher := &offsetFetcherMock{}
	log := logger.NewUPPLogger("test", "PANIC")
	monitor := newConsumerMonitor(ConsumerConfig{}, offsetFetcher, nil, log)

	ctx, cancel := context.WithCancel(context.Background())
	subscriptions := make(chan *subscriptionEvent, 10)

	timeout := time.After(3 * time.Second)
	done := make(chan bool)

	go func() {
		monitor.run(ctx, subscriptions) // Blocking call.
		done <- true
	}()

	time.Sleep(time.Second) // Simulate work.

	cancel() // Trigger the monitor termination.

	select {
	case <-done:
	case <-timeout:
		assert.Fail(t, "Failed to terminate monitor")
	}
}

func TestConsumerMonitor_FetchOffsets(t *testing.T) {
	tests := []struct {
		name               string
		offsetFetcher      offsetFetcher
		subscriptionsParam map[string][]int32
		offsetsResults     map[string]map[int32]int64
		errMessage         string
	}{
		{
			name: "fetching offsets fails with client error",
			offsetFetcher: &offsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return nil, fmt.Errorf("client error")
				},
			},
			errMessage: "error fetching consumer group offsets from client: client error",
		},
		{
			name: "fetching offsets fails with server error",
			offsetFetcher: &offsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Err: 13,
					}, nil
				},
			},
			errMessage: "error fetching consumer group offsets from server: kafka server",
		},
		{
			name: "fetching specific offset fails with server error",
			offsetFetcher: &offsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							"testTopic": {
								1: &sarama.OffsetFetchResponseBlock{
									Offset: 500,
								},
								2: &sarama.OffsetFetchResponseBlock{
									Err: 3,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				"testTopic": {1, 2},
			},
			errMessage: "error fetching consumer group offsets for partition 2 of topic \"testTopic\" from server: kafka server",
		},
		{
			name: "fetching offsets fails due to unexpected response",
			offsetFetcher: &offsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							"testTopic": {
								1: &sarama.OffsetFetchResponseBlock{
									Offset: 500,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				"testTopic2": {3},
			},
			errMessage: "requested offsets for topic \"testTopic2\" were not fetched",
		},
		{
			name: "fetching offsets is successful",
			offsetFetcher: &offsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							"testTopic": {
								1: &sarama.OffsetFetchResponseBlock{
									Offset: 500,
								},
								2: &sarama.OffsetFetchResponseBlock{
									Offset: 600,
								},
							},
							"testTopic2": {
								32: &sarama.OffsetFetchResponseBlock{
									Offset: 1500,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				"testTopic":  {1, 2},
				"testTopic2": {32},
			},
			offsetsResults: map[string]map[int32]int64{
				"testTopic": {
					1: 500,
					2: 600,
				},
				"testTopic2": {
					32: 1500,
				},
			},
		},
	}

	config := ConsumerConfig{
		ConsumerGroup: testConsumerGroup,
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				assert.NoError(t, test.offsetFetcher.Close())
			}()

			monitor := newConsumerMonitor(config, test.offsetFetcher, nil, nil)

			offsets, err := monitor.fetchOffsets(test.subscriptionsParam)
			if test.errMessage != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errMessage)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.offsetsResults, offsets)
		})
	}
}

type mockBrokerWrapper struct {
	broker   *sarama.MockBroker
	handlers map[string]sarama.MockResponse
}

func newMockBrokerWrapper(t *testing.T) *mockBrokerWrapper {
	brokerId := int32(1)
	broker := sarama.NewMockBroker(t, brokerId)

	handlers := map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(brokerId).
			SetBroker(broker.Addr(), brokerId),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, testConsumerGroup, broker),
	}
	broker.SetHandlerByMap(handlers)

	return &mockBrokerWrapper{
		broker:   broker,
		handlers: handlers,
	}
}

func (w *mockBrokerWrapper) setOffsetFetchResponse(offsetResponse *sarama.MockOffsetFetchResponse) {
	w.handlers["OffsetFetchRequest"] = offsetResponse
	w.broker.SetHandlerByMap(w.handlers)
}

func TestConsumerMonitor_Workflow(t *testing.T) {
	fetchInterval := 1650 * time.Millisecond
	fetchHandlingInterval := 1800 * time.Millisecond

	config := ConsumerConfig{
		ConsumerGroup:              testConsumerGroup,
		OffsetFetchInterval:        fetchInterval,
		OffsetFetchMaxFailureCount: 2,
	}

	updates := []struct {
		subscription   *subscriptionEvent
		offsetResponse *sarama.MockOffsetFetchResponse
		statusError    error
	}{
		{
			subscription: &subscriptionEvent{
				subscribed: true,
				topic:      "testTopic",
				partition:  2,
			},
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 15, "", 0),
			statusError: nil,
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 50, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 35 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 78, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 28 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 102, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 24 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 112, "", 0),
			statusError: nil,
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 143, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 31 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).SetError(1),
			statusError:    fmt.Errorf("consumer status is unknown"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 143, "", 0),
			statusError: nil,
		},
		{
			subscription: &subscriptionEvent{
				subscribed: true,
				topic:      "testTopic2",
				partition:  5,
			},
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 253, "", 0).
				SetOffset(testConsumerGroup, "testTopic2", 5, 40, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 110 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 259, "", 0).
				SetOffset(testConsumerGroup, "testTopic2", 5, 91, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 5 of topic \"testTopic2\" with 51 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 265, "", 0).
				SetOffset(testConsumerGroup, "testTopic2", 5, 113, "", 0),
			statusError: nil,
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 2, 295, "", 0).
				SetOffset(testConsumerGroup, "testTopic2", 5, 173, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 30 messages ; " +
				"consumer is lagging behind for partition 5 of topic \"testTopic2\" with 60 messages"),
		},
		{
			subscription: &subscriptionEvent{
				subscribed: false,
				topic:      "testTopic",
				partition:  2,
			},
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic2", 5, 273, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 5 of topic \"testTopic2\" with 100 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic2", 5, 293, "", 0),
			statusError: nil,
		},
		{
			subscription: &subscriptionEvent{
				subscribed: false,
				topic:      "testTopic2",
				partition:  5,
			},
			offsetResponse: sarama.NewMockOffsetFetchResponse(t),
			statusError:    nil,
		},
		{
			subscription: &subscriptionEvent{
				subscribed: true,
				topic:      "testTopic",
				partition:  4,
			},
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 4, 1023, "", 0),
			statusError: nil,
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 4, 0, "", 5),
			statusError: fmt.Errorf("consumer status is unknown"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 4, 1200, "", 0),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 4 of topic \"testTopic\" with 177 messages"),
		},
		{
			offsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, "testTopic", 4, 1220, "", 0),
			statusError: nil,
		},
	}

	brokerWrapper := newMockBrokerWrapper(t)

	offsetFetcher, err := sarama.NewClusterAdmin([]string{brokerWrapper.broker.Addr()}, nil)
	require.NoError(t, err)

	log := logger.NewUPPLogger("monitor_test", "INFO")
	topics := []*Topic{
		NewTopic("testTopic", WithLagTolerance(20)),
		NewTopic("testTopic2", WithLagTolerance(50)),
	}
	monitor := newConsumerMonitor(config, offsetFetcher, topics, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subscriptions := make(chan *subscriptionEvent, 10)

	go monitor.run(ctx, subscriptions)

	assert.NoError(t, monitor.consumerStatus()) // Initial status is healthy.

	for _, update := range updates {
		if update.subscription != nil {
			subscriptions <- update.subscription
		}

		brokerWrapper.setOffsetFetchResponse(update.offsetResponse)

		time.Sleep(fetchHandlingInterval) // Wait for the new response to be fetched and handled.

		require.Equal(t, update.statusError, monitor.consumerStatus())
	}
}
