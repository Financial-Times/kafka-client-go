# kafka-client-go

[![CircleCI](https://circleci.com/gh/Financial-Times/kafka-client-go.svg?style=svg)](https://circleci.com/gh/Financial-Times/kafka-client-go) [![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/kafka-client-go)](https://goreportcard.com/report/github.com/Financial-Times/kafka-client-go) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/kafka-client-go/badge.svg)](https://coveralls.io/github/Financial-Times/kafka-client-go)

Library for producing and consuming messages directly from Kafka.


## To use
To import:
```
import "github.com/Financial-Times/kafka-client-go/kafka"
```

### Producer
To set up, create your producer:
```
kp, err := kafka.NewProducer(*kafkaAddresses, *kafkaTopic)
```

You can then send an FT Message:
```
message := kafka.NewFTMessage(map[string]string{}, string(concept))
err = kp.SendMessage(message)
```


### Consumer
Create the consumer and start listening:
```
kc, err := kafka.NewConsumer(*zookeeperConnectionString, *groupName, []string{*topic})
kc.StartListening(handler.ProcessKafkaMessage)
```

Make sure to close it when the app is shutting down.
```
kc.Shutdown()
```

### Perseverant Implementations
The library also provides perseverant implementations of the Consumer and Producer interfaces. These keep trying to establish connections to Kafka and Zookeeper until successful. Attempting to consume or produce messages, or calling the connectivity check methods, will fail if no connection has yet been established.

## Testing
Some tests in this project require a local Zookeeper (port 2181) / Kafka (port 9092). To omit these tests, use the `-short` option.
