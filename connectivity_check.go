package kafka

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
)

var connectivityTimeout = 3 * time.Second

var ErrConnectivityTimedOut = fmt.Errorf("kafka connectivity timed out")

func checkConnectivity(brokers []string) error {
	errCh := make(chan error)

	go func() {
		client, err := sarama.NewClient(brokers, nil)
		if err == nil {
			_ = client.Close()
		}

		errCh <- err
	}()

	select {
	case <-time.After(connectivityTimeout):
		return ErrConnectivityTimedOut
	case err := <-errCh:
		return err
	}
}
