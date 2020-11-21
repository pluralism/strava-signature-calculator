package stravasignaturecalculator

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	options2 "go.mongodb.org/mongo-driver/mongo/options"
)

type ActivityDB struct {
	ObjectId  primitive.ObjectID `bson:"_id"`
	ID        int                `bson:"activity_id"`
	Title     string             `bson:"activity_title"`
	Signature []uint64           `bson:"activity_signature"`
	LatLng    [][]float64        `bson:"latlng"`
	Distance  []float64          `bson:"distance"`
	Time      []float64          `bson:"time"`
	Status    string             `bson:"status"`
}

type SignatureDB struct {
	ActivityID int    `bson:"activitiy_id"`
	Band       int    `bson:"band"`
	Bucket     uint64 `bson:"bucket"`
}

func (db *DB) GetActivitiesWithoutSignature(ctx context.Context, limit int64) ([]*ActivityDB, error) {
	collection := db.Client.Database(StravaDbName).Collection(StravaActivitiesCollection)
	filter := bson.M{"latlng": bson.M{"$exists": true}, ActivitySignature: bson.M{"$exists": false}}
	options := &options2.FindOptions{Limit: &limit}

	cur, err := collection.Find(ctx, filter, options)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var result []*ActivityDB
	for cur.Next(ctx) {
		var activity ActivityDB
		err := cur.Decode(&activity)
		if err != nil {
			return nil, err
		}
		result = append(result, &activity)
	}

	return result, nil
}

func (db *DB) SetActivitiesSignatures(ctx context.Context, m map[int][]uint64) error {
	collection := db.Client.Database(StravaDbName).Collection(StravaActivitiesCollection)
	var ops []struct {
		filter  bson.M
		updates bson.M
	}

	for k, v := range m {
		filter := bson.M{ActivityId: k}
		update := bson.M{"$set": bson.M{ActivitySignature: v}}

		ops = append(ops, struct {
			filter  bson.M
			updates bson.M
		}{filter: filter, updates: update})
	}

	var writes []mongo.WriteModel
	for _, op := range ops {
		model := mongo.NewUpdateOneModel().SetFilter(op.filter).SetUpdate(op.updates)
		writes = append(writes, model)
	}

	_, err := collection.BulkWrite(ctx, writes)
	return err
}
