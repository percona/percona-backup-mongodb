// Copyright (C) MongoDB, Inc. 2014-present.
// Portions Copyright © 2021 Percona, LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package restore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/common/txn"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongorestore/ns"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm"
)

var knownCommands = map[string]struct{}{
	"renameCollection": {},
	"dropDatabase":     {},
	"applyOps":         {},
	"dbCheck":          {},
	"create":           {},
	"convertToCapped":  {},
	"emptycapped":      {},
	"drop":             {},
	"createIndexes":    {},
	"deleteIndex":      {},
	"deleteIndexes":    {},
	"dropIndex":        {},
	"dropIndexes":      {},
	"collMod":          {},
	"startIndexBuild":  {},
	"abortIndexBuild":  {},
	"commitIndexBuild": {},
}

// Oplog is the oplog applyer
type Oplog struct {
	dst               *pbm.Node
	ver               *db.Version
	txnBuffer         *txn.Buffer
	needIdxWorkaround bool
	preserveUUIDopt   bool
	lastTS            primitive.Timestamp
	indexCatalog      *idx.IndexCatalog
	m                 *ns.Matcher

	preserveUUID bool
	cnamespase   string
}

// NewOplog creates an object for an oplog applying
func NewOplog(dst *pbm.Node, sv *pbm.MongoVersion, preserveUUID bool) (*Oplog, error) {
	m, err := ns.NewMatcher(append(excludeFromRestore, excludeFromOplog...))
	if err != nil {
		return nil, errors.Wrap(err, "create matcher for the collections exclude")
	}

	v := sv.Version
	if len(v) < 3 {
		for i := 3 - len(v); i > 0; i-- {
			v = append(v, 0)
		}
	}
	ver := &db.Version{v[0], v[1], v[2]}
	return &Oplog{
		dst:               dst,
		ver:               ver,
		preserveUUIDopt:   preserveUUID,
		preserveUUID:      preserveUUID,
		needIdxWorkaround: needsCreateIndexWorkaround(ver),
		indexCatalog:      idx.NewIndexCatalog(),
		m:                 m,
	}, nil
}

// SetEdge sets the edge time for the replayed operations
// E.g. all operation that happened afterthe given timestamp going to be discarded
func (o *Oplog) SetEdge(ts primitive.Timestamp) {
	o.lastTS = ts
}

// SetEdgeUnix sets the edge time for the replayed operations
// E.g. all operation that happened afterthe given timestamp going to be discarded
func (o *Oplog) SetEdgeUnix(ts int64) {
	o.SetEdge(primitive.Timestamp{T: uint32(ts), I: 0})
}

// Apply applys an oplog from a given source
func (o *Oplog) Apply(src io.ReadCloser) (lts primitive.Timestamp, err error) {
	bsonSource := db.NewDecodedBSONSource(db.NewBufferlessBSONSource(src))
	defer bsonSource.Close()

	o.txnBuffer = txn.NewBuffer()
	defer o.txnBuffer.Stop() // it basically never returns an error

	for {
		rawOplogEntry := bsonSource.LoadNext()
		if rawOplogEntry == nil {
			break
		}
		oe := db.Oplog{}
		err := bson.Unmarshal(rawOplogEntry, &oe)
		if err != nil {
			return lts, errors.Wrap(err, "reading oplog")
		}

		// finish if operation happened after the desired time frame (oe.Timestamp > o.lastTS)
		if o.lastTS.T > 0 && primitive.CompareTimestamp(oe.Timestamp, o.lastTS) == 1 {
			return lts, nil
		}

		err = o.handleOp(oe)
		if err != nil {
			return lts, err
		}

		lts = oe.Timestamp
	}

	return lts, bsonSource.Err()
}

func (o *Oplog) handleOp(oe db.Oplog) error {
	// skip if operation happened after the desired time frame (oe.Timestamp > o.lastTS)
	if o.lastTS.T > 0 && primitive.CompareTimestamp(oe.Timestamp, o.lastTS) == 1 {
		return nil
	}

	if o.m.Has(oe.Namespace) {
		return nil
	}

	// skip no-ops
	if oe.Operation == "n" {
		return nil
	}

	if oe.Operation == "c" && len(oe.Object) > 0 &&
		(oe.Object[0].Key == "startIndexBuild" || oe.Object[0].Key == "abortIndexBuild") {
		return nil
	}

	// optimization not to parse namespace if it remains the same
	if o.cnamespase != oe.Namespace {
		o.preserveUUID = o.preserveUUIDopt

		// don't preserve UUID for timeseries
		_, coll := util.SplitNamespace(oe.Namespace)
		if strings.HasPrefix(coll, "system.buckets.") {
			o.preserveUUID = false
		}

		o.cnamespase = oe.Namespace
	}

	meta, err := txn.NewMeta(oe)
	if err != nil {
		return errors.Wrap(err, "get op metadata")
	}

	if meta.IsTxn() {
		err = o.handleTxnOp(meta, oe)
		if err != nil {
			return errors.Wrap(err, "applying a transaction entry")
		}
	} else {
		err = o.handleNonTxnOp(oe)
		if err != nil {
			return errors.Wrap(err, "applying an entry")
		}
	}

	return nil
}

func (o *Oplog) handleTxnOp(meta txn.Meta, op db.Oplog) error {
	err := o.txnBuffer.AddOp(meta, op)
	if err != nil {
		return errors.Wrap(err, "buffering entry")
	}

	if meta.IsAbort() {
		err := o.txnBuffer.PurgeTxn(meta)
		if err != nil {
			return errors.Wrap(err, "cleaning up transaction buffer on abort")
		}
		return nil
	}

	if !meta.IsCommit() {
		return nil
	}

	// From here, we're applying transaction entries
	ops, errs := o.txnBuffer.GetTxnStream(meta)

Loop:
	for {
		select {
		case op, ok := <-ops:
			if !ok {
				break Loop
			}
			err = o.handleNonTxnOp(op)
			if err != nil {
				return errors.Wrap(err, "applying transaction op")
			}
		case err := <-errs:
			if err != nil {
				return errors.Wrap(err, "replaying transaction")
			}
			break Loop
		}
	}

	err = o.txnBuffer.PurgeTxn(meta)
	if err != nil {
		return errors.Wrap(err, "cleaning up transaction buffer")
	}

	return nil
}

func (o *Oplog) handleNonTxnOp(op db.Oplog) error {
	// have to handle it here one more time because before the op gets thru
	// txnBuffer its namespace is `collection.$cmd` instead of the real one
	if o.m.Has(op.Namespace) {
		return nil
	}

	op, err := o.filterUUIDs(op)
	if err != nil {
		return errors.Wrap(err, "filtering UUIDs from oplog")
	}

	if op.Operation == "c" {
		if len(op.Object) == 0 {
			return errors.Errorf("empty object value for op: %v", op)
		}
		cmdName := op.Object[0].Key

		if _, ok := knownCommands[cmdName]; !ok {
			return errors.Errorf("unknown oplog command name %v: %v", cmdName, op)
		}

		ns := strings.Split(op.Namespace, ".")
		dbName := ns[0]

		switch cmdName {
		case "commitIndexBuild":
			// commitIndexBuild was introduced in 4.4, one "commitIndexBuild" command can contain several
			// indexes, we need to convert the command to "createIndexes" command for each single index and apply
			collectionName, indexes := extractIndexDocumentFromCommitIndexBuilds(op)
			if indexes == nil {
				return errors.Errorf("failed to parse IndexDocument from commitIndexBuild in %s, %v", collectionName, op)
			}

			// if restore.OutputOptions.ConvertLegacyIndexes {
			// 	indexes = o.convertLegacyIndexes(indexes, op.Namespace)
			// }

			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}

			o.indexCatalog.AddIndexes(dbName, collName, indexes)
			return nil

		case "createIndexes":
			// server > 4.4 no longer supports applying createIndexes oplog, we need to convert the oplog to createIndexes command and execute it
			collectionName, index := extractIndexDocumentFromCreateIndexes(op)
			if index.Key == nil {
				return errors.Errorf("failed to parse IndexDocument from createIndexes in %s, %v", collectionName, op)
			}

			indexes := []*idx.IndexDocument{index}
			// if restore.OutputOptions.ConvertLegacyIndexes {
			// 	indexes = restore.convertLegacyIndexes(indexes, op.Namespace)
			// }

			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}

			o.indexCatalog.AddIndexes(dbName, collName, indexes)
			return nil

		case "dropDatabase":
			o.indexCatalog.DropDatabase(dbName)

		case "drop":
			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}
			o.indexCatalog.DropCollection(dbName, collName)

		case "applyOps":
			rawOps, ok := op.Object[0].Value.(bson.A)
			if !ok {
				return errors.Errorf("unknown format for applyOps: %#v", op.Object)
			}

			for _, rawOp := range rawOps {
				bytesOp, err := bson.Marshal(rawOp)
				if err != nil {
					return errors.Wrapf(err, "could not marshal applyOps operation: %v", rawOp)
				}
				var nestedOp db.Oplog
				err = bson.Unmarshal(bytesOp, &nestedOp)
				if err != nil {
					return errors.Wrapf(err, "could not unmarshal applyOps command: %v", rawOp)
				}

				err = o.handleOp(nestedOp)
				if err != nil {
					return errors.Wrap(err, "error applying nested op from applyOps")
				}
			}

			return nil

		case "deleteIndex", "deleteIndexes", "dropIndex", "dropIndexes":
			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}
			o.indexCatalog.DeleteIndexes(dbName, collName, op.Object)
			return nil
		case "collMod":
			if o.ver.GTE(db.Version{4, 1, 11}) {
				_, _ = bsonutil.RemoveKey("noPadding", &op.Object)
				_, _ = bsonutil.RemoveKey("usePowerOf2Sizes", &op.Object)
			}

			indexModValue, found := bsonutil.RemoveKey("index", &op.Object)
			if !found {
				break
			}
			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}
			err := o.indexCatalog.CollMod(dbName, collName, indexModValue)
			if err != nil {
				return err
			}
			// Don't apply the collMod if the only modification was for an index.
			if len(op.Object) == 1 {
				return nil
			}
		case "create":
			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}
			collation, err := bsonutil.FindSubdocumentByKey("collation", &op.Object)
			if err != nil {
				o.indexCatalog.SetCollation(dbName, collName, true)
			}
			localeValue, _ := bsonutil.FindValueByKey("locale", &collation)
			if localeValue == "simple" {
				o.indexCatalog.SetCollation(dbName, collName, true)
			}
		}
	}

	err = o.applyOps([]interface{}{op})
	if err != nil {
		opb, errm := json.Marshal(op)
		return errors.Wrapf(err, "op: %s | merr %v", opb, errm)
	}

	return nil
}

// extractIndexDocumentFromCommitIndexBuilds extracts the index specs out of  "createIndexes" oplog entry and convert to IndexDocument
// returns collection name and index spec
func extractIndexDocumentFromCreateIndexes(op db.Oplog) (string, *idx.IndexDocument) {
	collectionName := ""
	indexDocument := &idx.IndexDocument{Options: bson.M{}}
	for _, elem := range op.Object {
		if elem.Key == "createIndexes" {
			collectionName = elem.Value.(string)
		} else if elem.Key == "key" {
			indexDocument.Key = elem.Value.(bson.D)
		} else if elem.Key == "partialFilterExpression" {
			indexDocument.PartialFilterExpression = elem.Value.(bson.D)
		} else {
			indexDocument.Options[elem.Key] = elem.Value
		}
	}

	return collectionName, indexDocument
}

// extractIndexDocumentFromCommitIndexBuilds extracts the index specs out of  "commitIndexBuild" oplog entry and convert to IndexDocument
// returns collection name and index specs
func extractIndexDocumentFromCommitIndexBuilds(op db.Oplog) (string, []*idx.IndexDocument) {
	collectionName := ""
	for _, elem := range op.Object {
		if elem.Key == "commitIndexBuild" {
			collectionName = elem.Value.(string)
		}
	}
	// We need second iteration to split the indexes into single createIndex command
	for _, elem := range op.Object {
		if elem.Key == "indexes" {
			indexes := elem.Value.(bson.A)
			indexDocuments := make([]*idx.IndexDocument, len(indexes))
			for i, index := range indexes {
				var indexSpec idx.IndexDocument
				indexSpec.Options = bson.M{}
				for _, elem := range index.(bson.D) {
					if elem.Key == "key" {
						indexSpec.Key = elem.Value.(bson.D)
					} else if elem.Key == "partialFilterExpression" {
						indexSpec.PartialFilterExpression = elem.Value.(bson.D)
					} else {
						indexSpec.Options[elem.Key] = elem.Value
					}
				}
				indexDocuments[i] = &indexSpec
			}

			return collectionName, indexDocuments
		}
	}

	return collectionName, nil
}

// applyOps is a wrapper for the applyOps database command, we pass in
// a session to avoid opening a new connection for a few inserts at a time.
func (o *Oplog) applyOps(entries []interface{}) error {
	singleRes := o.dst.Session().Database("admin").RunCommand(context.TODO(), bson.D{{"applyOps", entries}})
	if err := singleRes.Err(); err != nil {
		return errors.Wrap(err, "applyOps")
	}
	res := bson.M{}
	err := singleRes.Decode(&res)
	if err != nil {
		return errors.Wrap(err, "decode singleRes")
	}
	if isFalsy(res["ok"]) {
		return errors.Errorf("applyOps command: %v", res["errmsg"])
	}

	return nil
}

// filterUUIDs removes 'ui' entries from ops, including nested applyOps ops.
// It also modifies ops that rely on 'ui'.
func (o *Oplog) filterUUIDs(op db.Oplog) (db.Oplog, error) {
	// Remove UUIDs from oplog entries
	if !o.preserveUUID {
		op.UI = nil

		// The createIndexes oplog command requires 'ui' for some server versions, so
		// in that case we fall back to an old-style system.indexes insert.
		if op.Operation == "c" && op.Object[0].Key == "createIndexes" && o.needIdxWorkaround {
			return convertCreateIndexToIndexInsert(op)
		}
	}

	// Check for and filter nested applyOps ops
	if op.Operation == "c" && isApplyOpsCmd(op.Object) {
		filtered, err := o.newFilteredApplyOps(op.Object)
		if err != nil {
			return db.Oplog{}, err
		}
		op.Object = filtered
	}

	return op, nil
}

// newFilteredApplyOps iterates over nested ops in an applyOps document and
// returns a new applyOps document that omits the 'ui' field from nested ops.
func (o *Oplog) newFilteredApplyOps(cmd bson.D) (bson.D, error) {
	ops, err := unwrapNestedApplyOps(cmd)
	if err != nil {
		return nil, err
	}

	filtered := make([]db.Oplog, len(ops))
	for i, v := range ops {
		filtered[i], err = o.filterUUIDs(v)
		if err != nil {
			return nil, err
		}
	}

	doc, err := wrapNestedApplyOps(filtered)
	if err != nil {
		return nil, err
	}

	return doc, nil
}

// server versions 3.6.0-3.6.8 and 4.0.0-4.0.2 require a 'ui' field
// in the createIndexes command.
// could be panic if `sv` is nil, but it's ok, good to catch it earlier
func needsCreateIndexWorkaround(sv *db.Version) bool {
	return (sv.GTE(db.Version{3, 6, 0}) && sv.LTE(db.Version{3, 6, 8})) ||
		(sv.GTE(db.Version{4, 0, 0}) && sv.LTE(db.Version{4, 0, 2}))
}

// convertCreateIndexToIndexInsert converts from new-style create indexes
// command to old style special index insert.
func convertCreateIndexToIndexInsert(op db.Oplog) (db.Oplog, error) {
	dbName := op.Namespace
	if i := strings.Index(dbName, "."); i > -1 {
		dbName = dbName[:i]
	}

	cmdValue := op.Object[0].Value
	collName, ok := cmdValue.(string)
	if !ok {
		return db.Oplog{}, errors.New("unknown format for createIndexes")
	}

	indexSpec := op.Object[1:]
	if len(indexSpec) < 3 {
		return db.Oplog{}, errors.New("unknown format for createIndexes, index spec " +
			"must have at least \"v\", \"key\", and \"name\" fields")
	}

	// createIndexes does not include the "ns" field but index inserts
	// do. Add it as the third field, after "v", "key", and "name".
	ns := bson.D{{"ns", fmt.Sprintf("%s.%s", dbName, collName)}}
	indexSpec = append(indexSpec[:3], append(ns, indexSpec[3:]...)...)
	op.Object = indexSpec
	op.Namespace = fmt.Sprintf("%s.system.indexes", dbName)
	op.Operation = "i"

	return op, nil
}

// isApplyOpsCmd returns true if a document seems to be an applyOps command.
func isApplyOpsCmd(cmd bson.D) bool {
	for _, v := range cmd {
		if v.Key == "applyOps" {
			return true
		}
	}
	return false
}

// nestedApplyOps models an applyOps command document
type nestedApplyOps struct {
	ApplyOps []db.Oplog `bson:"applyOps"`
}

// unwrapNestedApplyOps converts a bson.D to a typed data structure.
// Unfortunately, we're forced to convert by marshaling to bytes and
// unmarshalling.
func unwrapNestedApplyOps(doc bson.D) ([]db.Oplog, error) {
	// Doc to bytes
	bs, err := bson.Marshal(doc)
	if err != nil {
		return nil, errors.Wrap(err, "remarshal nested applyOps")
	}

	// Bytes to typed data
	var cmd nestedApplyOps
	err = bson.Unmarshal(bs, &cmd)
	if err != nil {
		return nil, errors.Wrap(err, "unwrap nested applyOps")
	}

	return cmd.ApplyOps, nil
}

// wrapNestedApplyOps converts a typed data structure to a bson.D.
// Unfortunately, we're forced to convert by marshaling to bytes and
// unmarshalling.
func wrapNestedApplyOps(ops []db.Oplog) (bson.D, error) {
	cmd := &nestedApplyOps{ApplyOps: ops}

	// Typed data to bytes
	raw, err := bson.Marshal(cmd)
	if err != nil {
		return nil, errors.Wrap(err, "rewrap nested applyOps op")
	}

	// Bytes to doc
	var doc bson.D
	err = bson.Unmarshal(raw, &doc)
	if err != nil {
		return nil, errors.Wrap(err, "reunmarshal nested applyOps op")
	}

	return doc, nil
}

// isTruthy returns true for values the server will interpret as "true".
// True values include {}, [], "", true, and any numbers != 0
func isTruthy(val interface{}) bool {
	if val == nil {
		return false
	}
	if val == (primitive.Undefined{}) {
		return false
	}

	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Map, reflect.Slice, reflect.Array, reflect.String, reflect.Struct:
		return true
	default:
		z := reflect.Zero(v.Type())
		return v.Interface() != z.Interface()
	}
}

// isFalsy returns true for values the server will interpret as "false".
// False values include numbers == 0, false, and nil
func isFalsy(val interface{}) bool {
	return !isTruthy(val)
}
