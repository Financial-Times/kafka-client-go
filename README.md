# kafka-client-go

[![CircleCI](https://circleci.com/gh/Financial-Times/kafka-client-go.svg?style=svg)](https://circleci.com/gh/Financial-Times/kafka-client-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/kafka-client-go)](https://goreportcard.com/report/github.com/Financial-Times/kafka-client-go)
[![Coverage Status](https://coveralls.io/repos/github/Financial-Times/kafka-client-go/badge.svg)](https://coveralls.io/github/Financial-Times/kafka-client-go)

## Description

Library for producing and consuming messages directly from Kafka.

Only perseverant connections (for both producer & consumer) are provided
i.e. establishing connections to Kafka is retried until successful.

Attempting to consume or produce messages or to check connectivity will fail if connection hasn't been established yet.

The library is NOT using Zookeeper to connect to Kafka under the hood.

## Usage

Importing:

```go
    import "github.com/Financial-Times/kafka-client-go/v2"
```

### Producer

Creating a producer:

```go
    config := kafka.ProducerConfig{
        BrokersConnectionString: "", // Comma-separated list of Kafka brokers
        Topic:                   "", // Topic to publish to
    }

    initialDelay := time.Second // Duration to await before trying to establish connection
    retryInterval := time.Second // Duration between each retry for establishing connection

    logger := logger.NewUPPLogger(...)
    
    producer := kafka.NewProducer(config, logger, initialDelay, retryInterval)
```

The connection to Kafka is started in a separate go routine when creating the producer.

Sending a message:

```go
    headers := map[string]string{}
    body := ""
    message := kafka.NewFTMessage(headers, body)
    
    err := producer.SendMessage(message)
    // Error handling
```

Connection should be closed by the client:

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
        Topics:                  []string{}, // Comma-separated list of topics to consume from
    }
    
    retryInterval := time.Second // Duration between each retry for establishing connection
    
    logger := logger.NewUPPLogger(...)

    consumer := kafka.NewConsumer(config, logger, retryInterval)
```

Consuming messages:

Consumer groups are lazily initialized i.e. establishing connection to Kafka is done within `StartListening()`.

```go
    handler := func(message kafka.FTMessage) {
        // Message handling
    }
    
    go consumer.StartListening(handler) // Blocking until connection is established
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

NB: Some tests in this project require a local Kafka (port 29092). Use the `-short` flag in order to omit those.
