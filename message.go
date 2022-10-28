package kafka

import (
	"github.com/Shopify/sarama"
)

const (
	SchemaNameHeader    = "X-Schema-Name"
	SchemaVersionHeader = "X-Schema-Version"
)

type Headers map[string]string

func NewHeaders() Headers {
	return map[string]string{}
}

func (h Headers) toRecordHeaders() []sarama.RecordHeader {
	var headers []sarama.RecordHeader

	for key, value := range h {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}

	return headers
}

type Message struct {
	Key     string
	Value   []byte
	Headers Headers
}

func rawToMessage(message *sarama.ConsumerMessage) Message {
	headers := map[string]string{}
	for _, header := range message.Headers {
		headers[string(header.Key)] = string(header.Value)
	}

	return Message{
		Key:     string(message.Key),
		Value:   message.Value,
		Headers: headers,
	}
}
