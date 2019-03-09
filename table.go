package dynamo

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type Table struct {
	api       dynamodbiface.DynamoDBAPI
	tableName string
}

func (t *Table) DeleteTableIfExists(ctx context.Context) error {
	input := dynamodb.DeleteTableInput{
		TableName: aws.String(t.tableName),
	}
	if _, err := t.api.DeleteTableWithContext(ctx, &input); err != nil {
		if v, ok := err.(awserr.Error); ok && v.Code() == dynamodb.ErrCodeResourceNotFoundException {
			return nil
		}

		return err
	}

	return nil
}

func New(api dynamodbiface.DynamoDBAPI, tableName string) *Table {
	return &Table{
		api:       api,
		tableName: tableName,
	}
}
