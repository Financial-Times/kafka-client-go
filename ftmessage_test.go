package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testFTMessage               = "FTMSG/1.0\r\ntest: test2\r\n\r\nTest Message"
	testComplexFTMessage        = "FTMSG/1.0\r\nContent-Type: application/vnd.ft-upp-article+json; version=1.0; charset=utf-8\r\n\r\nTest Message"
	testFTMessageHeaders        = map[string]string{"test": "test2"}
	testFTMessageBody           = "Test Message"
	testComplexFTMessageHeaders = map[string]string{"Content-Type": "application/vnd.ft-upp-article+json; version=1.0; charset=utf-8"}
)

func Test_FTMessage_Build(t *testing.T) {
	ft := NewFTMessage(testFTMessageHeaders, testFTMessageBody)

	ftm := ft.Build()
	assert.EqualValues(t, testFTMessage, ftm)
}

func TestFTMessage_Parse(t *testing.T) {
	payload := []byte(testFTMessage)
	ftMessage := rawToFTMessage(payload, testTopic)

	assert.EqualValues(t, testFTMessageHeaders, ftMessage.Headers)
	assert.EqualValues(t, testFTMessageBody, ftMessage.Body)
	assert.Equal(t, testTopic, ftMessage.Topic)
}

func TestFTMessage_Parse_NoBody(t *testing.T) {
	payload := []byte("FTMSG/1.0\r\ntest: test2")
	ftMessage := rawToFTMessage(payload, testTopic)

	assert.EqualValues(t, testFTMessageHeaders, ftMessage.Headers)
	assert.EqualValues(t, "", ftMessage.Body)
	assert.Equal(t, testTopic, ftMessage.Topic)
}

func TestFTMessage_Parse_CRLF(t *testing.T) {
	payload := []byte("FTMSG/1.0\r\ntest: test2\r\n\r\nTest Message")
	ftMessage := rawToFTMessage(payload, testTopic)

	assert.EqualValues(t, testFTMessageHeaders, ftMessage.Headers)
	assert.EqualValues(t, testFTMessageBody, ftMessage.Body)
	assert.Equal(t, testTopic, ftMessage.Topic)
}
func TestFTMessageWithComplexHeaders_Parse_CRLF(t *testing.T) {
	payload := []byte(testComplexFTMessage)
	ftMessage := rawToFTMessage(payload, testTopic)

	assert.EqualValues(t, testComplexFTMessageHeaders, ftMessage.Headers)
	assert.EqualValues(t, testFTMessageBody, ftMessage.Body)
	assert.Equal(t, testTopic, ftMessage.Topic)
}
