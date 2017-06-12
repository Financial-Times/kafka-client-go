package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	testFTMessage        = "FTMSG/1.0\ntest: test2\n\nTest Message"
	testFTMessageHeaders = map[string]string{"test": "test2"}
	testFTMessageBody    = "Test Message"
)

func Test_FTMessage_Build(t *testing.T) {
	ft := NewFTMessage(testFTMessageHeaders, testFTMessageBody)

	ftm := ft.Build()
	assert.EqualValues(t, testFTMessage, ftm)
}

func TestFTMessage_Parse(t *testing.T) {
	payload := []byte(testFTMessage)
	ftmsg, err := rawToFTMessage(payload)
	assert.NoError(t, err)

	assert.EqualValues(t, ftmsg.Headers, testFTMessageHeaders)
	assert.EqualValues(t, ftmsg.Body, testFTMessageBody)
}

func TestFTMessage_Parse_NoBody(t *testing.T) {
	payload := []byte("FTMSG/1.0\ntest: test2")
	ftmsg, err := rawToFTMessage(payload)
	assert.NoError(t, err)

	assert.EqualValues(t, ftmsg.Headers, testFTMessageHeaders)
	assert.EqualValues(t, ftmsg.Body, "")
}

func TestFTMessage_Parse_CRLF(t *testing.T) {
	payload := []byte("FTMSG/1.0\ntest: test2\r\n\r\nTest Message")
	ftmsg, err := rawToFTMessage(payload)
	assert.NoError(t, err)

	assert.EqualValues(t, ftmsg.Headers, testFTMessageHeaders)
	assert.EqualValues(t, ftmsg.Body, testFTMessageBody)
}
