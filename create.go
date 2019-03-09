package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const (
	DefaultBillingMode   = dynamodb.BillingModeProvisioned
	DefaultReadCapacity  = int64(3)
	DefaultWriteCapacity = int64(3)
)

type key struct {
	attributeName string
	attributeType string
}

type keyOptions struct {
	hashKey  *key
	rangeKey *key
}

type attribute struct {
	Name string
	Type string
}

type tableOptions struct {
	attributes         []attribute
	keys               keyOptions
	billingMode        string
	globalIndexes      []func(billingMode string) (*dynamodb.GlobalSecondaryIndex, []attribute)
	localIndexes       []func(billingMode string) (*dynamodb.LocalSecondaryIndex, []attribute)
	projectionType     string
	readCapacityUnits  int64
	streamViewType     string
	writeCapacityUnits int64
}

type TableOption interface {
	ApplyTable(o *tableOptions)
}

type IndexOption interface {
	ApplyIndex(o *tableOptions)
}

type TableIndexOption interface {
	TableOption
	IndexOption
}

type tableIndexFunc func(o *tableOptions)

func (fn tableIndexFunc) ApplyTable(o *tableOptions) {
	fn(o)
}

func (fn tableIndexFunc) ApplyIndex(o *tableOptions) {
	fn(o)
}

func WithAttr(attributeName, attributeType string) IndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.attributes = append(o.attributes, attribute{
			Name: attributeName,
			Type: attributeType,
		})
	})
}

func WithBillingMode(mode string) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.billingMode = mode
	})
}

func WithGlobalSecondaryIndex(indexName, projectionType string, opts ...IndexOption) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.globalIndexes = append(o.globalIndexes, func(billingMode string) (*dynamodb.GlobalSecondaryIndex, []attribute) {
			options := makeTableOptions(opts)
			options.billingMode = billingMode

			return &dynamodb.GlobalSecondaryIndex{
				IndexName: aws.String(indexName),
				KeySchema: makeKeySchemaElements(options),
				Projection: &dynamodb.Projection{
					NonKeyAttributes: aws.StringSlice(makeAttributeNames(options.attributes)),
					ProjectionType:   aws.String(projectionType),
				},
				ProvisionedThroughput: makeProvisionedThroughput(options),
			}, options.attributes
		})
	})
}

func WithHashKey(attributeName, attributeType string) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.keys.hashKey = &key{
			attributeName: attributeName,
			attributeType: attributeType,
		}
	})
}

func WithLocalSecondaryIndex(indexName, projectionType string, opts ...IndexOption) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.localIndexes = append(o.localIndexes, func(billingMode string) (*dynamodb.LocalSecondaryIndex, []attribute) {
			options := makeTableOptions(opts)
			options.billingMode = billingMode

			return &dynamodb.LocalSecondaryIndex{
				IndexName: aws.String(indexName),
				KeySchema: makeKeySchemaElements(options),
				Projection: &dynamodb.Projection{
					NonKeyAttributes: aws.StringSlice(makeAttributeNames(options.attributes)),
					ProjectionType:   aws.String(projectionType),
				},
			}, options.attributes
		})
	})
}

func makeAttributeNames(attributes []attribute) []string {
	var names []string
	for _, item := range attributes {
		names = append(names, item.Name)
	}
	return names
}

func WithRangeKey(attributeName, attributeType string) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.keys.rangeKey = &key{
			attributeName: attributeName,
			attributeType: attributeType,
		}
	})
}

func WithReadCapacity(rcap int64) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.readCapacityUnits = rcap
	})
}

func WithStreamSpecification(streamViewType string) TableOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.streamViewType = streamViewType
	})
}

func WithWriteCapacity(wcap int64) TableIndexOption {
	return tableIndexFunc(func(o *tableOptions) {
		o.writeCapacityUnits = wcap
	})
}

func makeAttributeDefinitions(options tableOptions) []*dynamodb.AttributeDefinition {
	var items []*dynamodb.AttributeDefinition
	if options.keys.hashKey != nil {
		items = append(items, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(options.keys.hashKey.attributeName),
			AttributeType: aws.String(options.keys.hashKey.attributeType),
		})
	}
	if options.keys.rangeKey != nil {
		items = append(items, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(options.keys.rangeKey.attributeName),
			AttributeType: aws.String(options.keys.rangeKey.attributeType),
		})
	}
	return items
}

func makeKeySchemaElements(options tableOptions) []*dynamodb.KeySchemaElement {
	var items []*dynamodb.KeySchemaElement
	if options.keys.hashKey != nil {
		items = append(items, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(options.keys.hashKey.attributeName),
			KeyType:       aws.String(dynamodb.KeyTypeHash),
		})
	}
	if options.keys.rangeKey != nil {
		items = append(items, &dynamodb.KeySchemaElement{
			AttributeName: aws.String(options.keys.rangeKey.attributeName),
			KeyType:       aws.String(dynamodb.KeyTypeRange),
		})
	}
	return items
}

func makeProvisionedThroughput(options tableOptions) *dynamodb.ProvisionedThroughput {
	if options.billingMode == dynamodb.BillingModePayPerRequest {
		return nil
	}

	return &dynamodb.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(options.readCapacityUnits),
		WriteCapacityUnits: aws.Int64(options.writeCapacityUnits),
	}
}

func makeTableOptions(opts interface{}) tableOptions {
	options := tableOptions{
		billingMode:        DefaultBillingMode,
		readCapacityUnits:  DefaultReadCapacity,
		writeCapacityUnits: DefaultWriteCapacity,
	}

	switch v := opts.(type) {
	case []IndexOption:
		for _, opt := range v {
			opt.ApplyIndex(&options)
		}
	case []TableOption:
		for _, opt := range v {
			opt.ApplyTable(&options)
		}
	}

	return options
}

func makeCreateTableInput(tableName string, opts ...TableOption) dynamodb.CreateTableInput {
	options := makeTableOptions(opts)

	input := dynamodb.CreateTableInput{
		AttributeDefinitions:  makeAttributeDefinitions(options),
		BillingMode:           aws.String(options.billingMode),
		KeySchema:             makeKeySchemaElements(options),
		ProvisionedThroughput: makeProvisionedThroughput(options),
		TableName:             aws.String(tableName),
	}
	if options.streamViewType != "" {
		input.StreamSpecification = &dynamodb.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(options.streamViewType),
		}
	}
	for _, fn := range options.globalIndexes {
		gsi, attributes := fn(options.billingMode)
		input.GlobalSecondaryIndexes = append(input.GlobalSecondaryIndexes, gsi)
		input.AttributeDefinitions = merge(input.AttributeDefinitions, attributes...)
	}
	for _, fn := range options.localIndexes {
		lsi, attributes := fn(options.billingMode)
		input.LocalSecondaryIndexes = append(input.LocalSecondaryIndexes, lsi)
		input.AttributeDefinitions = merge(input.AttributeDefinitions, attributes...)
	}

	return input
}

func (t *Table) CreateTableIfNotExists(ctx context.Context, hashKeyName, hashKeyType string, opts ...TableOption) error {
	var mergedOpts []TableOption
	mergedOpts = append(mergedOpts, WithHashKey(hashKeyName, hashKeyType))
	mergedOpts = append(mergedOpts, opts...)

	input := makeCreateTableInput(t.tableName, mergedOpts...)
	if _, err := t.api.CreateTableWithContext(ctx, &input); err != nil {
		if v, ok := err.(awserr.Error); ok && v.Code() == dynamodb.ErrCodeResourceInUseException {
			return nil
		}
		return err
	}

	return nil
}

func merge(definitions []*dynamodb.AttributeDefinition, attributes ...attribute) []*dynamodb.AttributeDefinition {
	var (
		seen   = map[string]struct{}{}
		merged []*dynamodb.AttributeDefinition
	)

	for _, item := range definitions {
		seen[*item.AttributeName] = struct{}{}
		merged = append(merged, item)
	}

	for _, attr := range attributes {
		if _, ok := seen[attr.Name]; ok {
			continue
		}
		seen[attr.Name] = struct{}{}
		merged = append(merged, &dynamodb.AttributeDefinition{
			AttributeName: aws.String(attr.Name),
			AttributeType: aws.String(attr.Type),
		})
	}

	return merged
}
