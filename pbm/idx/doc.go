// Package idx wraps mongo-tools' IndexCatalog for PBM's deferred index restoration.
//
// PBM loads backup definitions, updates them during oplog replay, and builds the
// remaining indexes. Legacy time-series collections need shared lookup across two aliases:
//
//	logical:  db.metrics                 {name: "sensor_1", key: {"m.sensor": 1}}
//	bucket:   db.system.buckets.metrics  {name: "sensor_1", key: {"meta.sensor": 1}}
//
// Both aliases share one entry, so bucket-targeted drops also remove definitions
// loaded from backup. Specifications retain their original keys and filters.
//
// [Catalog.AddIndex] and [Catalog.AddIndexes] accept backup metadata specifications.
// Their AddOplog counterparts also recognize legacy bucket namespaces.
//
// [NewCatalog] configures build routing for the target version. On MongoDB 8.3,
// rawData: true builds on the buckets through the logical name, using bucket keys
// without logical-to-bucket translation. [Catalog.BuildGroups] returns ready-to-build
// batches for the required targets:
//
//	definition          MongoDB 8.0 target       MongoDB 8.3 target
//	logical keys        metrics                 metrics
//	bucket keys         system.buckets.metrics  metrics, rawData: true
//
// Ordinary collections use their usual collection name without rawData. Only
// pending indexes are tracked; physical PITR does not preload indexes from data files.
//
// Access must be serialized. Use catalog methods for mutations: added documents
// are retained and getters return shared references. BuildGroups copies documents
// and top-level Options maps; nested values remain shared and read-only.
package idx
