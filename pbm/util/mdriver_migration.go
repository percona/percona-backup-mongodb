package util

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// ToV1Timestamp converts bson.Timestamp (v2) to bson.Timestamp (v1)
func ToV1Timestamp(ts bson.Timestamp) primitive.Timestamp {
	return primitive.Timestamp{T: ts.T, I: ts.I}
}

// ToV2Timestamp converts bson.Timestamp (v1) to bson.Timestamp (v2)
func ToV2Timestamp(ts primitive.Timestamp) bson.Timestamp {
	return bson.Timestamp{T: ts.T, I: ts.I}
}
