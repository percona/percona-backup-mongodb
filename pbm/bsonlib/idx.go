package bsonlib

import (
	"fmt"
	"sync"

	"github.com/mongodb/mongo-tools/common/log"
	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// IndexDocument holds information about a collection's index.
type IndexDocument struct {
	Options                 bson.M `bson:",inline"`
	Key                     bson.D `bson:"key"`
	PartialFilterExpression bson.D `bson:"partialFilterExpression,omitempty"`
}

// CollectionIndexCatalog stores the current view of all indexes of a single collection.
type CollectionIndexCatalog struct {
	// Maps index name to the raw index spec.
	indexes         map[string]*IndexDocument
	simpleCollation bool
}

// IndexCatalog stores the current view of all indexes in all databases.
type IndexCatalog struct {
	sync.Mutex
	// Maps database name to collection name to CollectionIndexCatalog.
	indexes map[string]map[string]*CollectionIndexCatalog
}

// NewIndexCatalog inits an IndexCatalog
func NewIndexCatalog() *IndexCatalog {
	return &IndexCatalog{indexes: make(map[string]map[string]*CollectionIndexCatalog)}
}

// SetCollation sets if a collection has a simple collation
func (i *IndexCatalog) SetCollation(database, collection string, simpleCollation bool) {
	i.Lock()
	defer i.Unlock()
	collIndexCatalog := i.getCollectionIndexCatalog(database, collection)
	collIndexCatalog.simpleCollation = simpleCollation
}

// GetIndexes returns all the indexes for the given collection.
// When the collection has a non-simple collation an explicit simple collation
// must be added to the indexes with no "collation field. Otherwise, the index
// will wrongfully inherit the collections's collation.
// This is necessary because indexes with the simple collation do not have a
// "collation" field in the getIndexes output.
func (i *IndexCatalog) GetIndexes(database, collection string) []*IndexDocument {
	dbIndexes, found := i.indexes[database]
	if !found {
		return nil
	}
	collIndexCatalog, found := dbIndexes[collection]
	if !found {
		return nil
	}
	var syncedIndexes []*IndexDocument
	for _, index := range collIndexCatalog.indexes {
		if !collIndexCatalog.simpleCollation && !hasCollationOnIndex(index) {
			index.Options["collation"] = bson.D{{"locale", "simple"}}
		}
		syncedIndexes = append(syncedIndexes, index)
	}
	return syncedIndexes
}

func hasCollationOnIndex(index *IndexDocument) bool {
	if _, ok := index.Options["collation"]; ok {
		return true
	}
	return false
}

func (i *IndexCatalog) addIndex(database, collection, indexName string, index *IndexDocument) {
	i.Lock()
	collIndexes := i.getCollectionIndexes(database, collection)
	collIndexes[indexName] = index
	i.Unlock()
}

// AddIndex stores the given index into the index catalog. An example index:
//
//	{
//		"v": 2,
//		"key": {
//			"lastModifiedDate": 1
//		},
//		"name": "lastModifiedDate_1",
//		"ns": "test.eventlog"
//	}
func (i *IndexCatalog) AddIndex(database, collection string, index *IndexDocument) {
	indexName, ok := index.Options["name"].(string)
	if !ok {
		return
	}
	i.addIndex(database, collection, indexName, index)
}

func (i *IndexCatalog) getCollectionIndexes(database, collection string) map[string]*IndexDocument {
	dbIndexes, found := i.indexes[database]
	if !found {
		dbIndexes = make(map[string]*CollectionIndexCatalog)
		i.indexes[database] = dbIndexes
	}
	collIndexCatalog, found := dbIndexes[collection]
	if !found {
		collIndexCatalog = &CollectionIndexCatalog{
			indexes: make(map[string]*IndexDocument),
		}
		dbIndexes[collection] = collIndexCatalog
	}
	return collIndexCatalog.indexes
}

func (i *IndexCatalog) getCollectionIndexCatalog(database, collection string) *CollectionIndexCatalog {
	dbIndexes, found := i.indexes[database]
	if !found {
		dbIndexes = make(map[string]*CollectionIndexCatalog)
		i.indexes[database] = dbIndexes
	}
	collIndexCatalog, found := dbIndexes[collection]
	if !found {
		collIndexCatalog = &CollectionIndexCatalog{
			indexes: make(map[string]*IndexDocument),
		}
		dbIndexes[collection] = collIndexCatalog
	}
	return collIndexCatalog
}

// AddIndexes stores the given indexes into the index catalog.
func (i *IndexCatalog) AddIndexes(database, collection string, indexes []*IndexDocument) {
	for _, index := range indexes {
		i.AddIndex(database, collection, index)
	}
}

// DropDatabase removes a database from the index catalog.
func (i *IndexCatalog) DropDatabase(database string) {
	delete(i.indexes, database)
}

// DropCollection removes a collection from the index catalog.
func (i *IndexCatalog) DropCollection(database, collection string) {
	delete(i.indexes[database], collection)
}

// DeleteIndexes removes indexes from the index catalog. dropCmd may be,
// {"deleteIndexes": "eventlog", "index": "*"}
// or,
// {"deleteIndexes": "eventlog", "index": "name_1"}
func (i *IndexCatalog) DeleteIndexes(database, collection string, dropCmd bson.D) error {
	collIndexes := i.getCollectionIndexes(database, collection)
	if len(collIndexes) == 0 {
		// We have no indexes to drop.
		return nil
	}
	indexValue, keyError := FindValueByKey("index", &dropCmd)
	if keyError != nil {
		return nil
	}
	switch indexToDrop := indexValue.(type) {
	case string:
		if indexToDrop == "*" {
			catalog := i.getCollectionIndexCatalog(database, collection)

			var idIndexName string
			var idIndex *IndexDocument
			for name, doc := range catalog.indexes {
				keyMap, err := convertDToM(doc.Key)
				if err != nil {
					return err
				}
				if len(keyMap) == 1 {
					if _, isId := keyMap["_id"]; isId {
						idIndexName = name
						idIndex = doc
						break
					}
				}
			}

			// Drop all non-id indexes for the collection.
			i.DropCollection(database, collection)
			if idIndex != nil {
				i.addIndex(database, collection, idIndexName, idIndex)
			}
			return nil
		}

		// Drop an index by name.
		delete(collIndexes, indexToDrop)
		return nil
	case bson.D:
		var toDelete []string
		// Drop an index by key pattern.
		for key, value := range collIndexes {
			isEq, err := IsEqual(indexToDrop, value.Key)
			if err != nil {
				return fmt.Errorf("could not drop index on %s.%s, could not handle %v: "+
					"was unable to find matching index in indexCatalog. Error with equality test: %v",
					database, collection, dropCmd[0].Key, err)
			}

			if isEq {
				toDelete = append(toDelete, key)
			}
		}
		if len(toDelete) > 1 {
			return fmt.Errorf("could not drop index on %s.%s: "+
				"the key %v somehow matched more than one index in the collection", database, collection, dropCmd[0].Key)
		}
		// Could we have 0 items in toDelete? I'm not sure, so it's best to
		// avoid accessing toDelete[0].
		for _, td := range toDelete {
			delete(collIndexes, td)
		}

		log.Logvf(log.DebugHigh, "Must drop index on %s.%s by key pattern: %v", database, collection, indexToDrop)
		return nil
	default:
		return fmt.Errorf("could not drop index on %s.%s, could not handle %v: "+
			"expected string or object for 'index', found: %T, %v",
			database, collection, dropCmd[0].Key, indexToDrop, indexToDrop)
	}
}

// GetIndex returns an IndexDocument for a given index name
func (i *IndexCatalog) GetIndex(database, collection, indexName string) *IndexDocument {
	dbIndexes, found := i.indexes[database]
	if !found {
		return nil
	}
	collIndexCatalog, found := dbIndexes[collection]
	if !found {
		return nil
	}
	indexSpec, found := collIndexCatalog.indexes[indexName]
	if !found {
		return nil
	}
	return indexSpec
}

// GetIndexByIndexMod returns an index that matches the name or key pattern specified in
// a collMod command.
func (i *IndexCatalog) GetIndexByIndexMod(database, collection string, indexMod bson.D) (*IndexDocument, error) {
	// Look for "name" or "keyPattern".
	name, nameErr := FindStringValueByKey("name", &indexMod)
	keyPattern, keyPatternErr := FindSubdocumentByKey("keyPattern", &indexMod)
	switch {
	case nameErr == nil && keyPatternErr == nil:
		return nil, errors.Errorf("cannot specify both index name and keyPattern: %v", indexMod)
	case nameErr != nil && keyPatternErr != nil:
		return nil, errors.Errorf("must specify either index name (as a string) or keyPattern (as a document): %v", indexMod)
	case nameErr == nil:
		matchingIndex := i.GetIndex(database, collection, name)
		if matchingIndex == nil {
			return nil, errors.Errorf("cannot find index in indexCatalog for collMod: %v", indexMod)
		}
		return matchingIndex, nil
	case keyPatternErr == nil:
		collIndexes := i.getCollectionIndexes(database, collection)
		for _, indexSpec := range collIndexes {
			isEq, err := IsEqual(keyPattern, indexSpec.Key)
			if err != nil {
				return nil, fmt.Errorf("was unable to find matching index in indexCatalog. Error with equality test: %v", err)
			}
			if isEq {
				return indexSpec, nil
			}
		}
		return nil, errors.Errorf("cannot find index in indexCatalog for collMod: %v", indexMod)
	default:
		return nil, errors.Errorf("cannot find index in indexCatalog for collMod: %v", indexMod)
	}
}
func (i *IndexCatalog) collMod(database, collection string, indexModValue any) error {
	indexMod, ok := indexModValue.(bson.D)
	if !ok {
		return errors.Errorf("unknown collMod \"index\" modifier: %v", indexModValue)
	}

	matchingIndex, err := i.GetIndexByIndexMod(database, collection, indexMod)
	if err != nil {
		return err
	}
	if matchingIndex == nil {
		// Did not find an index to modify.
		return errors.Errorf("cannot find index in indexCatalog for collMod: %v", indexMod)
	}

	expireValue, expireKeyError := FindValueByKey("expireAfterSeconds", &indexMod)
	if expireKeyError == nil {
		newExpire, ok := expireValue.(int64)
		if !ok {
			return errors.Errorf("expireAfterSeconds must be a number (found %v of type %T): %v", expireValue, expireValue, indexMod)
		}
		err = updateExpireAfterSeconds(matchingIndex, newExpire)
		if err != nil {
			return err
		}
	}

	expireValue, hiddenKeyError := FindValueByKey("hidden", &indexMod)
	if hiddenKeyError == nil {
		newHidden, ok := expireValue.(bool)
		if !ok {
			return errors.Errorf("hidden must be a boolean (found %v of type %T): %v", expireValue, expireValue, indexMod)
		}
		updateHidden(matchingIndex, newHidden)
	}

	if expireKeyError != nil && hiddenKeyError != nil {
		return errors.Errorf("must specify expireAfterSeconds or hidden: %v", indexMod)
	}

	// Update the index.
	i.AddIndex(database, collection, matchingIndex)
	return nil
}

func updateExpireAfterSeconds(index *IndexDocument, expire int64) error {
	if _, ok := index.Options["expireAfterSeconds"]; !ok {
		return errors.Errorf("missing \"expireAfterSeconds\" in matching index: %v", index)
	}
	index.Options["expireAfterSeconds"] = expire
	return nil
}

func updateHidden(index *IndexDocument, hidden bool) {
	index.Options["hidden"] = hidden
}

// CollMod, updates the corresponding TTL index if the given collModCmd
// updates the "expireAfterSeconds" or "hiddne" fields. For example,
//
//	{
//	 "collMod": "sessions",
//	 "index": {"keyPattern": {"lastAccess": 1}, "expireAfterSeconds": 3600}}
//	}
//
// or,
//
//	{
//	 "collMod": "sessions",
//	 "index": {"name": "lastAccess_1", "expireAfterSeconds": 3600}}
//	}
func (i *IndexCatalog) CollMod(database, collection string, indexModValue any) error {
	err := i.collMod(database, collection, indexModValue)
	if err != nil {
		return fmt.Errorf("could not handle collMod on %s.%s: %v", database, collection, err)
	}
	return nil
}

// Namespaces returns all the namespaces in the IndexCatalog
func (i *IndexCatalog) Namespaces() (namespaces []Namespace) {
	for database, dbIndexMap := range i.indexes {
		for collection := range dbIndexMap {
			namespaces = append(namespaces, Namespace{database, collection})
		}
	}
	return namespaces
}

type Namespace struct {
	// Specified database and collection
	DB         string `short:"d" long:"db" value-name:"<database-name>" description:"database to use"`
	Collection string `short:"c" long:"collection" value-name:"<collection-name>" description:"collection to use"`
}

func convertDToM(d bson.D) (bson.M, error) {
	data, err := bson.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("marshal error: %w", err)
	}

	var m bson.M
	err = bson.Unmarshal(data, &m)
	if err != nil {
		return nil, fmt.Errorf("unmarshal error: %w", err)
	}

	return m, nil
}
