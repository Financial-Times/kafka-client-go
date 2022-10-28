package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	jsonschema "github.com/xeipuuv/gojsonschema"
)

const (
	schemaRetrievalTimeout = 5 * time.Second
)

type schemaRetriever interface {
	GetSchemaVersion(ctx context.Context, input *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

type schemaValidator struct {
	retriever schemaRetriever

	// Name of a schema registry in AWS Glue.
	registryName string

	// Key is schema identifier in the format "schema-name-version-number".
	// E.g. "article-internal-v1".
	schemas map[string]*jsonschema.Schema
}

func newSchemaValidator(ctx context.Context, registryName string) (*schemaValidator, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading aws config: %w", err)
	}

	return &schemaValidator{
		retriever:    glue.NewFromConfig(cfg),
		registryName: registryName,
		schemas:      map[string]*jsonschema.Schema{},
	}, nil
}

func newSchemaIdentifier(name string, version int64) string {
	return fmt.Sprintf("%s-v%d", name, version)
}

func (v *schemaValidator) retrieveSchema(ctx context.Context, name string, version int64) (*jsonschema.Schema, error) {
	identifier := newSchemaIdentifier(name, version)

	schema, ok := v.schemas[identifier]
	if ok {
		return schema, nil
	}

	resp, err := v.retriever.GetSchemaVersion(ctx, &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			RegistryName: aws.String(v.registryName),
			SchemaName:   aws.String(name),
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{
			VersionNumber: version,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("retrieving schema definition: %w", err)
	}

	schema, err = jsonschema.NewSchema(jsonschema.NewStringLoader(*resp.SchemaDefinition))
	if err != nil {
		return nil, fmt.Errorf("parsing schema definition: %w", err)
	}

	// Store the fetched schema in memory in order to avoid re-requesting it.
	v.schemas[identifier] = schema

	return schema, nil
}

func (v *schemaValidator) Validate(schemaName string, schemaVersion int64, payload interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), schemaRetrievalTimeout)
	defer cancel()

	schema, err := v.retrieveSchema(ctx, schemaName, schemaVersion)
	if err != nil {
		return err
	}

	result, err := schema.Validate(jsonschema.NewGoLoader(payload))
	if err != nil {
		return err
	}

	if result.Valid() {
		return nil
	}

	var errors []string
	for _, err := range result.Errors() {
		errors = append(errors, err.String())
	}

	return fmt.Errorf("invalid payload: %s", strings.Join(errors, "; "))
}
