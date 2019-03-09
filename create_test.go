package dynamo

import (
	"context"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

func Test_makeCreateTableInput(t *testing.T) {
	const (
		tableName = "blah"
		hashKey   = "hash"
		rangeKey  = "range"
	)

	t.Run("minimal", func(t *testing.T) {
		got := makeCreateTableInput(tableName,
			WithHashKey(hashKey, dynamodb.ScalarAttributeTypeS),
			WithRangeKey(rangeKey, dynamodb.ScalarAttributeTypeN),
		)
		want := dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(hashKey),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
				{
					AttributeName: aws.String(rangeKey),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeN),
				},
			},
			BillingMode: aws.String(DefaultBillingMode),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(hashKey),
					KeyType:       aws.String(dynamodb.KeyTypeHash),
				},
				{
					AttributeName: aws.String(rangeKey),
					KeyType:       aws.String(dynamodb.KeyTypeRange),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(DefaultReadCapacity),
				WriteCapacityUnits: aws.Int64(DefaultWriteCapacity),
			},
			TableName: aws.String(tableName),
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})

	t.Run("pay per request", func(t *testing.T) {
		got := makeCreateTableInput(tableName,
			WithBillingMode(dynamodb.BillingModePayPerRequest),
		)
		want := dynamodb.CreateTableInput{
			BillingMode: aws.String(dynamodb.BillingModePayPerRequest),
			TableName:   aws.String(tableName),
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})

	t.Run("custom throughput", func(t *testing.T) {
		rcap := int64(4)
		wcap := int64(5)
		got := makeCreateTableInput(tableName,
			WithReadCapacity(rcap),
			WithWriteCapacity(wcap),
		)
		want := dynamodb.CreateTableInput{
			BillingMode: aws.String(DefaultBillingMode),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(rcap),
				WriteCapacityUnits: aws.Int64(wcap),
			},
			TableName: aws.String(tableName),
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})

	t.Run("stream specification", func(t *testing.T) {
		viewType := dynamodb.StreamViewTypeKeysOnly

		got := makeCreateTableInput(tableName,
			WithStreamSpecification(viewType),
		)
		want := dynamodb.CreateTableInput{
			BillingMode: aws.String(DefaultBillingMode),
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(DefaultReadCapacity),
				WriteCapacityUnits: aws.Int64(DefaultWriteCapacity),
			},
			StreamSpecification: &dynamodb.StreamSpecification{
				StreamEnabled:  aws.Bool(true),
				StreamViewType: aws.String(viewType),
			},
			TableName: aws.String(tableName),
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})

	t.Run("lsi", func(t *testing.T) {
		attributeName := "hello"
		indexName := "index"
		projectionType := dynamodb.ProjectionTypeInclude
		got := makeCreateTableInput(tableName,
			WithLocalSecondaryIndex(indexName, projectionType, WithAttr(attributeName, dynamodb.ScalarAttributeTypeS)),
		)
		want := dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(attributeName),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			BillingMode: aws.String(DefaultBillingMode),
			LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{
				{
					IndexName: aws.String(indexName),
					Projection: &dynamodb.Projection{
						NonKeyAttributes: []*string{
							aws.String(attributeName),
						},
						ProjectionType: aws.String(projectionType),
					},
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(DefaultReadCapacity),
				WriteCapacityUnits: aws.Int64(DefaultWriteCapacity),
			},
			TableName: aws.String(tableName),
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})

	t.Run("gsi", func(t *testing.T) {
		attributeName := "hello"
		indexName := "index"
		projectionType := dynamodb.ProjectionTypeInclude
		got := makeCreateTableInput(tableName,
			WithGlobalSecondaryIndex(indexName, projectionType, WithAttr(attributeName, dynamodb.ScalarAttributeTypeS)),
		)
		want := dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(attributeName),
					AttributeType: aws.String(dynamodb.ScalarAttributeTypeS),
				},
			},
			BillingMode: aws.String(DefaultBillingMode),
			GlobalSecondaryIndexes: []*dynamodb.GlobalSecondaryIndex{
				{
					IndexName: aws.String(indexName),
					Projection: &dynamodb.Projection{
						NonKeyAttributes: []*string{
							aws.String(attributeName),
						},
						ProjectionType: aws.String(projectionType),
					},
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(DefaultReadCapacity),
						WriteCapacityUnits: aws.Int64(DefaultWriteCapacity),
					},
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(DefaultReadCapacity),
				WriteCapacityUnits: aws.Int64(DefaultWriteCapacity),
			},
			TableName: aws.String(tableName),
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %#v; want %#v", got, want)
		}
	})
}

type Mock struct {
	dynamodbiface.DynamoDBAPI
	err error
}

func (m *Mock) CreateTableWithContext(aws.Context, *dynamodb.CreateTableInput, ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return &dynamodb.CreateTableOutput{}, m.err
}

func (m *Mock) DeleteTableWithContext(aws.Context, *dynamodb.DeleteTableInput, ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return &dynamodb.DeleteTableOutput{}, m.err
}

func TestCreateTable(t *testing.T) {
	var (
		ctx         = context.Background()
		tableName   = "blah"
		hashKeyName = "id"
		hashKeyType = dynamodb.ScalarAttributeTypeS
	)

	t.Run("ok", func(t *testing.T) {
		mock := &Mock{}
		table := New(mock, tableName)
		err := table.CreateTableIfNotExists(ctx, hashKeyName, hashKeyType)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("table already exists", func(t *testing.T) {
		mock := &Mock{
			err: awserr.New(dynamodb.ErrCodeResourceInUseException, "boom", nil),
		}
		table := New(mock, tableName)
		err := table.CreateTableIfNotExists(ctx, hashKeyName, hashKeyType)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}
