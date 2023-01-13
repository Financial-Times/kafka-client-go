package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/Shopify/sarama/mocks"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testBrokers = "localhost:29092"
	testTopic   = "testTopic"
)

func newTestProducer(t *testing.T, topic string) *Producer {
	config := ProducerConfig{
		BrokersConnectionString: testBrokers,
		Topic:                   topic,
	}

	producer, err := NewProducer(config)
	require.NoError(t, err)

	return producer
}

func TestProducer_SendMessage(t *testing.T) {
	mockProducer := mocks.NewSyncProducer(t, nil)
	mockProducer.ExpectSendMessageAndSucceed()

	producer := &Producer{
		producer: mockProducer,
	}

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}
	assert.NoError(t, producer.SendMessage(msg))

	assert.NoError(t, producer.Close())
}

func TestProducer_Connectivity(t *testing.T) {
	tests := []struct {
		name               string
		requiresConnection bool
		newProducer        func() *Producer
		expectedErr        string
	}{
		{
			name:               "valid connection",
			requiresConnection: true,
			newProducer: func() *Producer {
				return newTestProducer(t, testTopic)
			},
		},
		{
			name:               "invalid connection causes kafka error",
			requiresConnection: false,
			newProducer: func() *Producer {
				return &Producer{
					config: ProducerConfig{
						BrokersConnectionString: "unknown:9092",
					},
				}
			},
			expectedErr: "kafka: client has run out of available brokers to talk to",
		},
		{
			name:               "invalid connection causes cluster status error",
			requiresConnection: false,
			newProducer: func() *Producer {
				return &Producer{
					config: ProducerConfig{
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
			newProducer: func() *Producer {
				return &Producer{
					config: ProducerConfig{
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
			newProducer: func() *Producer {
				return &Producer{
					config: ProducerConfig{
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
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.requiresConnection && testing.Short() {
				t.Skip("Skipping test as it requires a connection to Kafka.")
				return
			}

			producer := test.newProducer()
			connectivityErr := producer.ConnectivityCheck()

			if test.expectedErr != "" {
				require.Error(t, connectivityErr)
				assert.Contains(t, connectivityErr.Error(), test.expectedErr)
				return
			}

			assert.NoError(t, connectivityErr)
		})
	}
}

func TestProducer_Workflow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping test as it requires a connection to Kafka.")
	}

	producer := newTestProducer(t, testTopic)
	require.NoError(t, producer.ConnectivityCheck())

	msg := FTMessage{
		Headers: map[string]string{
			"X-Request-Id": "test",
		},
		Body: `{"foo":"bar"}`,
	}
	assert.NoError(t, producer.SendMessage(msg))

	assert.NoError(t, producer.Close())
}
