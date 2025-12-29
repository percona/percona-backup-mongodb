package bsonlib

import (
	"bytes"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
)

var ErrNoSuchField = errors.New("no such field")

// FindValueByKey returns the value of keyName in document. If keyName is not found
// in the top-level of the document, ErrNoSuchField is returned as the error.
func FindValueByKey(keyName string, document *bson.D) (interface{}, error) {
	for _, key := range *document {
		if key.Key == keyName {
			return key.Value, nil
		}
	}
	return nil, ErrNoSuchField
}

// FindSubdocumentByKey returns the value of keyName in document as a document.
// Returns an error if keyName is not found in the top-level of the document,
// or if it is found but its value is not a document.
func FindSubdocumentByKey(keyName string, document *bson.D) (bson.D, error) {
	value, err := FindValueByKey(keyName, document)
	if err != nil {
		return bson.D{}, err
	}
	doc, ok := value.(bson.D)
	if !ok {
		return bson.D{}, fmt.Errorf("field '%s' is not a document", keyName)
	}
	return doc, nil
}

// FindStringValueByKey returns the value of keyName in document as a String.
// Returns an error if keyName is not found in the top-level of the document,
// or if it is found but its value is not a string.
func FindStringValueByKey(keyName string, document *bson.D) (string, error) {
	value, err := FindValueByKey(keyName, document)
	if err != nil {
		return "", err
	}
	str, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("field present, but is not a string: %v", value)
	}
	return str, nil
}

// RemoveKey removes the given key. Returns the removed value and true if the
// key was found.
func RemoveKey(key string, document *bson.D) (interface{}, bool) {
	if document == nil {
		return nil, false
	}
	doc := *document
	for i, elem := range doc {
		if elem.Key == key {
			// Remove this key.
			*document = append(doc[:i], doc[i+1:]...)
			return elem.Value, true
		}
	}
	return nil, false
}

// IsEqual marshals two documents to raw BSON and compares them.
func IsEqual(left, right bson.D) (bool, error) {
	leftBytes, err := bson.Marshal(left)
	if err != nil {
		return false, err
	}

	rightBytes, err := bson.Marshal(right)
	if err != nil {
		return false, err
	}

	return bytes.Compare(leftBytes, rightBytes) == 0, nil
}
