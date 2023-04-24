package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
)

const (
	clusterConfigTimeout      = 5 * time.Second
	clusterDescriptionTimeout = 2 * time.Second
)

type clusterDescriber interface {
	DescribeClusterV2(ctx context.Context, input *kafka.DescribeClusterV2Input, optFns ...func(*kafka.Options)) (*kafka.DescribeClusterV2Output, error)
}

func newClusterDescriber(clusterArn *string) (clusterDescriber, error) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterConfigTimeout)
	defer cancel()

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}

	client := kafka.NewFromConfig(cfg)

	// Ensure the client is properly configured.
	if _, err = retrieveClusterState(client, clusterArn); err != nil {
		return nil, fmt.Errorf("retrieving cluster state: %w", err)
	}

	return client, nil
}

// Verifies whether the Kafka cluster is available or not.
// False positive healthcheck errors are being ignored during maintenance windows.
func verifyHealthErrorSeverity(healthErr error, describer clusterDescriber, clusterArn *string) error {
	state, stateErr := retrieveClusterState(describer, clusterArn)
	if stateErr != nil {
		return fmt.Errorf("cluster status is unknown: %w", stateErr)
	}

	if isMaintenanceState(state) {
		return nil
	}

	return healthErr
}

func retrieveClusterState(describer clusterDescriber, clusterArn *string) (types.ClusterState, error) {
	parsedARN, err := arn.Parse(*clusterArn)
	if err != nil {
		return "", fmt.Errorf("error parsing cluster ARN: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), clusterDescriptionTimeout)
	defer cancel()

	cluster, err := describer.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{
		ClusterArn: clusterArn,
	}, func(opt *kafka.Options) {
		opt.Region = parsedARN.Region
	})
	if err != nil {
		return "", err
	}

	return cluster.ClusterInfo.State, nil
}

func isMaintenanceState(state types.ClusterState) bool {
	return state == types.ClusterStateMaintenance
}
