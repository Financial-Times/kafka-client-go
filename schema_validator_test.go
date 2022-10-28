package kafka

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/assert"
	jsonschema "github.com/xeipuuv/gojsonschema"
)

type retrieverMock struct {
	getSchemaVersionF func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

func (r retrieverMock) GetSchemaVersion(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
	if r.getSchemaVersionF != nil {
		return r.getSchemaVersionF(ctx, input, optFns...)
	}

	panic("retrieverMock.GetSchemaVersion is not implemented")
}

func TestValidator_Validate(t *testing.T) {
	var testSchema = `
			{
			  "$schema": "http://json-schema.org/draft-07/schema#",
			  "$id": "http://upp-test-schema+json",
			  "title": "Test",
			  "type": "object",
			  "description": "Test schema",
			  "properties": {
				"uuid": {
				  "type": "string",
				  "pattern": "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"
				},
				"title": {
				  "type": "string"
				},
				"type": {
				  "type": "string",
				  "enum": [
					"A",
					"B"
				  ]
				},
				"publishedDate": {
				  "type": "string",
				  "format": "date-time"
				}
			  },
			  "required": [
				"uuid"
			  ],
			  "additionalProperties": false
			}
	`

	tests := []struct {
		name          string
		retriever     retrieverMock
		payload       interface{}
		expectedError string
	}{
		{
			name: "retrieving schema definition failure",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					return nil, fmt.Errorf("failed to get schema version")
				},
			},
			expectedError: "retrieving schema definition: failed to get schema version",
		},
		{
			name: "parsing schema definition failure",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					definition := "invalid definition"
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &definition,
					}
					return schema, nil
				},
			},
			expectedError: "parsing schema definition: invalid character 'i' looking for beginning of value",
		},
		{
			name: "missing payload uuid",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &testSchema,
					}
					return schema, nil
				},
			},
			payload: struct {
				UUID  string `json:"uuid,omitempty"`
				Title string `json:"title,omitempty"`
			}{
				Title: "Validation test title",
			},
			expectedError: "invalid payload: (root): uuid is required",
		},
		{
			name: "invalid payload uuid",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &testSchema,
					}
					return schema, nil
				},
			},
			payload: map[string]interface{}{
				"uuid": "123",
			},
			expectedError: "invalid payload: uuid: Does not match pattern '[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'",
		},
		{
			name: "invalid payload type",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &testSchema,
					}
					return schema, nil
				},
			},
			payload: map[string]interface{}{
				"uuid":  "8271b704-a113-4771-b18c-231e57e886f8",
				"title": "Validation test title",
				"type":  "C",
			},
			expectedError: "invalid payload: type: type must be one of the following: \"A\", \"B\"",
		},
		{
			name: "invalid payload publishedDate",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &testSchema,
					}
					return schema, nil
				},
			},
			payload: map[string]interface{}{
				"uuid":          "8271b704-a113-4771-b18c-231e57e886f8",
				"title":         "Validation test title",
				"publishedDate": "123",
			},
			expectedError: "invalid payload: publishedDate: Does not match format 'date-time'",
		},
		{
			name: "valid untyped payload",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &testSchema,
					}
					return schema, nil
				},
			},
			payload: map[string]interface{}{
				"uuid":          "8271b704-a113-4771-b18c-231e57e886f8",
				"title":         "Validation test title",
				"type":          "A",
				"publishedDate": "2022-10-13T20:51:41.141Z",
			},
		},
		{
			name: "valid typed payload",
			retriever: retrieverMock{
				getSchemaVersionF: func(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error) {
					schema := &glue.GetSchemaVersionOutput{
						SchemaDefinition: &testSchema,
					}
					return schema, nil
				},
			},
			payload: struct {
				UUID          string `json:"uuid"`
				Title         string `json:"title,omitempty"`
				Type          string `json:"type,omitempty"`
				PublishedDate string `json:"publishedDate"`
			}{
				UUID:          "8271b704-a113-4771-b18c-231e57e886f8",
				Type:          "A",
				PublishedDate: "2022-10-13T20:51:41.141Z",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			validator := schemaValidator{
				retriever: test.retriever,
				schemas:   map[string]*jsonschema.Schema{},
			}

			err := validator.Validate("", 0, test.payload)

			if test.expectedError == "" {
				assert.Nil(t, err)
			} else {
				assert.EqualError(t, err, test.expectedError)
			}
		})
	}
}
