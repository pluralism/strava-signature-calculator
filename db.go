package stravasignaturecalculator

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type StravaDatastore interface {
	GetActivitiesWithoutSignature(ctx context.Context, limit int64) ([]*ActivityDB, error)
	SetActivitiesSignatures(ctx context.Context, m map[int][]uint64) error
}

type DB struct {
	*mongo.Client
}

func NewConnection(uri string) (*DB, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, _ = context.WithTimeout(context.Background(), 2*time.Second)
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, err
	}

	return &DB{client}, nil
}

func CloseConnection(db *DB) error {
	ctx, _ := context.WithTimeout(context.Background(), 10 * time.Second)
	return db.Client.Disconnect(ctx)
}