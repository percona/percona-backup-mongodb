package sharded

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (c *Cluster) stopBalancer(ctx context.Context, conn *mongo.Client) {
	err := conn.Database("admin").RunCommand(
		ctx,
		bson.D{{"balancerStop", 1}},
	).Err()
	if err != nil {
		log.Fatalf("ERROR: stopping balancer: %v", err)
	}
}
