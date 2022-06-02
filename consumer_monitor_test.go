package kafka

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	monitorTopic1 = "testTopic"
	monitorTopic2 = "testTopic2"

	monitorTopic1LagTolerance = 20
	monitorTopic2LagTolerance = 50

	monitorPartition1 = 1
	monitorPartition2 = 2
)

type topicOffsetFetcherMock struct {
	getOffsetF func(topic string, partitionID int32, position int64) (int64, error)
}

func (f *topicOffsetFetcherMock) GetOffset(topic string, partitionID int32, position int64) (int64, error) {
	if f.getOffsetF != nil {
		return f.getOffsetF(topic, partitionID, position)
	}

	panic("topicOffsetFetcherMock.GetOffset is not implemented")
}

type consumerOffsetFetcherMock struct {
	listConsumerGroupOffsetsF func(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error)
	closeF                    func() error
}

func (f *consumerOffsetFetcherMock) ListConsumerGroupOffsets(group string, topicPartitions map[string][]int32) (*sarama.OffsetFetchResponse, error) {
	if f.listConsumerGroupOffsetsF != nil {
		return f.listConsumerGroupOffsetsF(group, topicPartitions)
	}

	panic("consumerOffsetFetcherMock.ListConsumerGroupOffsets is not implemented")
}

func (f *consumerOffsetFetcherMock) Close() error {
	if f.closeF != nil {
		return f.closeF()
	}

	return nil
}

func TestConsumerMonitor_Lifecycle(t *testing.T) {
	topicOffsetFetcher := &topicOffsetFetcherMock{}
	consumerOffsetFetcher := &consumerOffsetFetcherMock{}
	log := logger.NewUPPLogger("test", "PANIC")
	monitor := newConsumerMonitor(ConsumerConfig{}, consumerOffsetFetcher, topicOffsetFetcher, nil, log)

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
		topicFetcher       topicOffsetFetcher
		consumerFetcher    consumerOffsetFetcher
		subscriptionsParam map[string][]int32
		offsetsResults     map[string][]offset
		errMessage         string
	}{
		{
			name: "fetching consumer offsets fails with client error",
			consumerFetcher: &consumerOffsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return nil, fmt.Errorf("client error")
				},
			},
			errMessage: "error fetching consumer group offsets from client: client error",
		},
		{
			name: "fetching consumer offsets fails with server error",
			consumerFetcher: &consumerOffsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Err: 13,
					}, nil
				},
			},
			errMessage: "error fetching consumer group offsets from server: kafka server",
		},
		{
			name: "fetching specific consumer offset fails with server error",
			topicFetcher: &topicOffsetFetcherMock{
				getOffsetF: func(string, int32, int64) (int64, error) {
					return 0, nil
				},
			},
			consumerFetcher: &consumerOffsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							monitorTopic1: {
								monitorPartition1: &sarama.OffsetFetchResponseBlock{
									Offset: 500,
								},
								monitorPartition2: &sarama.OffsetFetchResponseBlock{
									Err: 3,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				monitorTopic1: {monitorPartition1, monitorPartition2},
			},
			errMessage: "error fetching consumer group offsets for partition 2 of topic \"testTopic\" from server: kafka server",
		},
		{
			name: "fetching consumer offsets fails due to unexpected response",
			consumerFetcher: &consumerOffsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							monitorTopic1: {
								monitorPartition1: &sarama.OffsetFetchResponseBlock{
									Offset: 500,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				monitorTopic2: {3},
			},
			errMessage: "requested consumer offsets for topic \"testTopic2\" were not fetched",
		},
		{
			name: "fetching topic offset fails",
			topicFetcher: &topicOffsetFetcherMock{
				getOffsetF: func(string, int32, int64) (int64, error) {
					return 0, fmt.Errorf("client error")
				},
			},
			consumerFetcher: &consumerOffsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							monitorTopic2: {
								monitorPartition2: &sarama.OffsetFetchResponseBlock{
									Offset: 333,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				monitorTopic2: {monitorPartition2},
			},
			errMessage: "error fetching topic offset for partition 2 of topic \"testTopic2\": client error",
		},
		{
			name: "fetching offsets is successful",
			topicFetcher: &topicOffsetFetcherMock{
				getOffsetF: func(string, int32, int64) (int64, error) {
					return 750, nil
				},
			},
			consumerFetcher: &consumerOffsetFetcherMock{
				listConsumerGroupOffsetsF: func(string, map[string][]int32) (*sarama.OffsetFetchResponse, error) {
					return &sarama.OffsetFetchResponse{
						Blocks: map[string]map[int32]*sarama.OffsetFetchResponseBlock{
							monitorTopic1: {
								monitorPartition1: &sarama.OffsetFetchResponseBlock{
									Offset: 500,
								},
								monitorPartition2: &sarama.OffsetFetchResponseBlock{
									Offset: 600,
								},
							},
							monitorTopic2: {
								monitorPartition2: &sarama.OffsetFetchResponseBlock{
									Offset: 150,
								},
							},
						},
					}, nil
				},
			},
			subscriptionsParam: map[string][]int32{
				monitorTopic1: {monitorPartition1, monitorPartition2},
				monitorTopic2: {monitorPartition2},
			},
			offsetsResults: map[string][]offset{
				monitorTopic1: {
					{
						Partition: monitorPartition1,
						Consumer:  500,
						Topic:     750,
					},
					{
						Partition: monitorPartition2,
						Consumer:  600,
						Topic:     750,
					},
				},
				monitorTopic2: {
					{
						Partition: monitorPartition2,
						Consumer:  150,
						Topic:     750,
					},
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
				assert.NoError(t, test.consumerFetcher.Close())
			}()

			monitor := newConsumerMonitor(config, test.consumerFetcher, test.topicFetcher, nil, nil)
			monitor.subscriptions = test.subscriptionsParam

			offsets, err := monitor.fetchOffsets()
			if test.errMessage != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errMessage)
				return
			}

			require.NoError(t, err)

			for _, offsets := range offsets {
				sort.Slice(offsets, func(i, j int) bool {
					return offsets[i].Partition < offsets[j].Partition
				})
			}
			assert.True(t, reflect.DeepEqual(test.offsetsResults, offsets))
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
			SetBroker(broker.Addr(), brokerId).
			SetLeader(monitorTopic1, monitorPartition1, brokerId).
			SetLeader(monitorTopic1, monitorPartition2, brokerId).
			SetLeader(monitorTopic2, monitorPartition1, brokerId).
			SetLeader(monitorTopic2, monitorPartition2, brokerId),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(t).
			SetCoordinator(sarama.CoordinatorGroup, testConsumerGroup, broker),
	}
	broker.SetHandlerByMap(handlers)

	return &mockBrokerWrapper{
		broker:   broker,
		handlers: handlers,
	}
}

func (w *mockBrokerWrapper) setOffsetFetchResponses(consumerOffsetResponse *sarama.MockOffsetFetchResponse, topicOffsetResponse *sarama.MockOffsetResponse) {
	w.handlers["OffsetFetchRequest"] = consumerOffsetResponse
	if topicOffsetResponse != nil {
		w.handlers["OffsetRequest"] = topicOffsetResponse
	}
	w.broker.SetHandlerByMap(w.handlers)
}

func TestConsumerMonitor_Workflow(t *testing.T) {
	fetchInterval := 550 * time.Millisecond
	fetchHandlingInterval := 600 * time.Millisecond

	config := ConsumerConfig{
		ConsumerGroup:                    testConsumerGroup,
		OffsetFetchInterval:              fetchInterval,
		OffsetFetchMaxFailureCount:       2,
		DisableMonitoringConnectionReset: true,
	}

	updates := []struct {
		subscription           *subscriptionEvent
		consumerOffsetResponse *sarama.MockOffsetFetchResponse
		topicOffsetResponse    *sarama.MockOffsetResponse
		statusError            error
	}{
		{
			subscription: &subscriptionEvent{
				subscribed: true,
				topic:      monitorTopic1,
				partition:  monitorPartition2,
			},
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 15, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 20),
			statusError: nil,
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 50, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 85),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 35 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 90, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 95),
			statusError: nil,
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).SetError(1),
			statusError:            fmt.Errorf("consumer status is unknown"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 120, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 120),
			statusError: nil,
		},
		{
			subscription: &subscriptionEvent{
				subscribed: true,
				topic:      monitorTopic2,
				partition:  monitorPartition2,
			},
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 200, "", 0).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 40, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 310).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 40),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 110 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 310, "", 0).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 100, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 315).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 151),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic2\" with 51 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 320, "", 0).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 160, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 333).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 175),
			statusError: nil,
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 350, "", 0).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 180, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 380).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 240),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 30 messages ; " +
				"consumer is lagging behind for partition 2 of topic \"testTopic2\" with 60 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition2, 350, "", 0).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 0, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition2, sarama.OffsetNewest, 380).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 240),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic\" with 30 messages ; " +
				"could not determine lag for partition 2 of topic \"testTopic2\" due to uncompleted intial offset commit"),
		},
		{
			subscription: &subscriptionEvent{
				subscribed: false,
				topic:      monitorTopic1,
				partition:  monitorPartition2,
			},
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 275, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 375),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 2 of topic \"testTopic2\" with 100 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic2, monitorPartition2, 385, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic2, monitorPartition2, sarama.OffsetNewest, 400),
			statusError: nil,
		},
		{
			subscription: &subscriptionEvent{
				subscribed: false,
				topic:      monitorTopic2,
				partition:  monitorPartition2,
			},
			statusError: nil,
		},
		{
			subscription: &subscriptionEvent{
				subscribed: true,
				topic:      monitorTopic1,
				partition:  monitorPartition1,
			},
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 1200, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1205),
			statusError: nil,
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 0, "", 5),
			statusError: fmt.Errorf("consumer status is unknown"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 1205, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1226),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 1 of topic \"testTopic\" with 21 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 1226, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1246),
			statusError: nil,
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 1250, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1249),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"could not determine lag for partition 1 of topic \"testTopic\""),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 1250, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1275),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"consumer is lagging behind for partition 1 of topic \"testTopic\" with 25 messages"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 0, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1275),
			statusError: fmt.Errorf("consumer is not healthy: " +
				"could not determine lag for partition 1 of topic \"testTopic\" due to uncompleted intial offset commit"),
		},
		{
			consumerOffsetResponse: sarama.NewMockOffsetFetchResponse(t).
				SetOffset(testConsumerGroup, monitorTopic1, monitorPartition1, 1300, "", 0),
			topicOffsetResponse: sarama.NewMockOffsetResponse(t).
				SetOffset(monitorTopic1, monitorPartition1, sarama.OffsetNewest, 1300),
			statusError: nil,
		},
	}

	brokerWrapper := newMockBrokerWrapper(t)

	client, err := sarama.NewClient([]string{brokerWrapper.broker.Addr()}, nil)
	require.NoError(t, err)

	admin, err := sarama.NewClusterAdminFromClient(client)
	require.NoError(t, err)

	log := logger.NewUPPLogger("monitor_test", "INFO")
	topics := []*Topic{
		NewTopic(monitorTopic1, WithLagTolerance(monitorTopic1LagTolerance)),
		NewTopic(monitorTopic2, WithLagTolerance(monitorTopic2LagTolerance)),
	}
	monitor := newConsumerMonitor(config, admin, client, topics, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subscriptions := make(chan *subscriptionEvent, 10)

	go monitor.run(ctx, subscriptions)

	assert.NoError(t, monitor.consumerStatus()) // Initial status is healthy.

	for _, update := range updates {
		if update.subscription != nil {
			subscriptions <- update.subscription
		}

		brokerWrapper.setOffsetFetchResponses(update.consumerOffsetResponse, update.topicOffsetResponse)

		time.Sleep(fetchHandlingInterval) // Wait for the new response to be fetched and handled.

		require.Equal(t, update.statusError, monitor.consumerStatus())
	}
}
