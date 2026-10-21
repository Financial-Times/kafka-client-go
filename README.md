# kafka-client-go

[![CircleCI](https://circleci.com/gh/Financial-Times/kafka-client-go.svg?style=svg)](https://circleci.com/gh/Financial-Times/kafka-client-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/kafka-client-go)](https://goreportcard.com/report/github.com/Financial-Times/kafka-client-go)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/kafka-client-go/badge.svg)](https://coveralls.io/github/Financial-Times/kafka-client-go)

## Description

Library for producing and consuming messages directly from Kafka.

The library is NOT using Zookeeper to connect to Kafka under the hood.

## Usage

Importing:

```go
    import "github.com/Financial-Times/kafka-client-go/v4"
```

### Producer

Creating a producer:

```go
    config := kafka.ProducerConfig{
        BrokersConnectionString: "", // Comma-separated list of Kafka brokers
        Topic:                   "", // Topic to publish to 
    }
 
    producer, err := kafka.NewProducer(config) 
    // Error handling
```

Failing to establish a connection to Kafka will result in an error.

Sending a message:

```go
    headers := map[string]string{}
    body := ""
    message := kafka.NewFTMessage(headers, body)
    
    err := producer.SendMessage(message)
    // Error handling
```

The health of the Kafka cluster can be checked by attempting to establish separate connection with the provided configuration:

```go
   err := producer.ConnectivityCheck()
   // Error handling
```

Connections should be closed by the client:

```go
    err := producer.Close()
    // Error handling
```

### Consumer

Creating a consumer:

```go
    config := kafka.ConsumerConfig{
        BrokersConnectionString: "", // Comma-separated list of Kafka brokers
        ConsumerGroup:           "", // Unique name of a consumer group
    }

    topics := []*kafka.Topic{
        kafka.NewTopic(""),                             // Topic to consume from
        kafka.NewTopic("", kafka.WithLagTolerance(50)), // Topic to consume from with custom lag tolerance used for monitoring
    }
 
    logger := logger.NewUPPLogger(...)

    consumer, err := kafka.NewConsumer(config, topics, logger)
    // Error handling
```

Failing to establish a connection to Kafka will result in an error.

Consuming messages:

```go
    handler := func(message kafka.FTMessage) {
        // Message handling
    }
    
    consumer.Start(handler)
```

Use `StartWithErrorHandler` when message processing can fail:

```go
    handler := func(message kafka.FTMessageWithMetadata) error {
        log.WithField("partition", message.Partition).WithField("offset", message.Offset).Info("Processing Kafka message")
        return writeVersion(message.FTMessage)
    }

    consumer.StartWithErrorHandler(handler, nil)
```

The consumer marks a message only after the handler returns `nil`. Return `nil`
for a message you intentionally skip. On error, it retries that message after
one second, doubling the delay up to 30 seconds by default. It does not process
later messages in that partition until the handler succeeds. If the session ends
first, Kafka can deliver the unmarked message again. Make handlers safe to retry
if their write may have succeeded before the session ended.

Pass an `ErrorHandlerRetryConfig` pointer to change the retry delays:

```go
    consumer.StartWithErrorHandler(handler, &kafka.ErrorHandlerRetryConfig{
        InitialDelay: time.Second,
        MaxDelay:     10 * time.Second,
    })
```

Non-positive values use the defaults. If the configured maximum is lower than
the initial delay, the initial delay is reduced to the maximum. The handler
receives `FTMessageWithMetadata`, which embeds the original `FTMessage` and adds
the Kafka partition and offset. The `Start` API continues to receive `FTMessage`.

All instances in a consumer group must use `StartWithErrorHandler` together. An
instance still using `Start` could receive the partition after a rebalance and
acknowledge a message without checking whether its processing succeeded.

The health of the Kafka cluster can be checked by attempting to establish separate connection with the provided configuration:

```go
   err := consumer.ConnectivityCheck()
   // Error handling
```

The health of the consumer process is also being monitored and its status can be accessed:

```go
   err := consumer.MonitorCheck()
   // Error handling
```

Connections should be closed by the client:

```go
    err := consumer.Close()
    // Error handling
```

## Testing

```shell
    go test --race -v ./...
```

Nota bene: Some tests in this project require a local Kafka (listening on port 29092).
Use the `-short` flag in order to omit those tests.

Or before executing the tests locally, to start kafka cluster in docker containers and satisfy the test requirement, execute command:

```shell
    docker compose -f docker-compose.yml up -d --build
```

And map host `unknown` to a valid IP address in your local /etc/hosts file (e.g. to IP: 127.0.0.1), as the tests require this host name to be valid.

```shell
127.0.0.1 unknown
```
