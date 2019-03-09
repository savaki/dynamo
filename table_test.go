package dynamo

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestDeleteTable(t *testing.T) {
	var (
		ctx       = context.Background()
		tableName = "blah"
	)

	t.Run("ok", func(t *testing.T) {
		mock := &Mock{}
		table := New(mock, tableName)
		err := table.DeleteTableIfExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})

	t.Run("table already exists", func(t *testing.T) {
		mock := &Mock{
			err: awserr.New(dynamodb.ErrCodeResourceNotFoundException, "boom", nil),
		}
		table := New(mock, tableName)
		err := table.DeleteTableIfExists(ctx)
		if err != nil {
			t.Fatalf("got %v; want nil", err)
		}
	})
}
