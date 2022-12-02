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

NB: Some tests in this project require a local Kafka (port 29092). Use the `-short` flag in order to omit those.
