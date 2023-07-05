// Copyright (C) MongoDB, Inc. 2014-present.
// Portions Copyright © 2021 Percona, LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package oplog

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/common/txn"
	"github.com/mongodb/mongo-tools/mongorestore/ns"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/pbm/snapshot"
)

type Record = db.Oplog

// OpFilter can be used to filter out oplog records by content.
// Useful for apply only subset of operations depending on conditions
type OpFilter func(*Record) bool

func DefaultOpFilter(*Record) bool { return true }

var excludeFromOplog = []string{
	"config.rangeDeletions",
	pbm.DB + "." + pbm.TmpUsersCollection,
	pbm.DB + "." + pbm.TmpRolesCollection,
}

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

var selectedNSSupportedCommands = []string{
	"create",
	"drop",
	"createIndexes",
	"deleteIndex",
	"deleteIndexes",
	"dropIndex",
	"dropIndexes",
	"collMod",
	"startIndexBuild",
	"abortIndexBuild",
	"commitIndexBuild",
}

var dontPreserveUUID = []string{
	"admin.system.users",
	"admin.system.roles",
	"admin.system.keys",
	"*.system.buckets.*", // timeseries
}

// OplogRestore is the oplog applyer
type OplogRestore struct {
	dst               *mongo.Client
	ver               *db.Version
	needIdxWorkaround bool
	preserveUUIDopt   bool
	startTS           primitive.Timestamp
	endTS             primitive.Timestamp
	indexCatalog      *idx.IndexCatalog
	excludeNS         *ns.Matcher
	includeNS         map[string]map[string]bool
	noUUIDns          *ns.Matcher

	// dist txn prepare entities yet to be committed
	txnData map[string]Txn
	// the queue of last N committed transactions
	txnCommit *cqueue

	txn        chan pbm.RestoreTxn
	txnSyncErr chan error
	// The `T` part of the last applied op's Timestamp.
	// Keeping just `T` allows atomic use as we only care
	// if we've moved further in general. No need in
	// `I` precision.
	lastOpT uint32

	preserveUUID bool
	cnamespase   string

	unsafe bool

	filter OpFilter
}

const saveLastDistTxns = 100

// NewOplogRestore creates an object for an oplog applying
func NewOplogRestore(dst *mongo.Client, ic *idx.IndexCatalog, sv *pbm.MongoVersion, unsafe, preserveUUID bool, ctxn chan pbm.RestoreTxn, txnErr chan error) (*OplogRestore, error) {
	m, err := ns.NewMatcher(append(snapshot.ExcludeFromRestore, excludeFromOplog...))
	if err != nil {
		return nil, errors.Wrap(err, "create matcher for the collections exclude")
	}
	noUUID, err := ns.NewMatcher(dontPreserveUUID)
	if err != nil {
		return nil, errors.Wrap(err, "create matcher for the collections exclude")
	}

	v := sv.Version
	if len(v) < 3 {
		for i := 3 - len(v); i > 0; i-- {
			v = append(v, 0)
		}
	}
	if ic == nil {
		ic = idx.NewIndexCatalog()
	}
	ver := &db.Version{v[0], v[1], v[2]}
	return &OplogRestore{
		dst:               dst,
		ver:               ver,
		preserveUUIDopt:   preserveUUID,
		preserveUUID:      preserveUUID,
		needIdxWorkaround: needsCreateIndexWorkaround(ver),
		indexCatalog:      ic,
		excludeNS:         m,
		noUUIDns:          noUUID,
		txn:               ctxn,
		txnSyncErr:        txnErr,
		unsafe:            unsafe,
		filter:            DefaultOpFilter,
		txnData:           make(map[string]Txn),
		txnCommit:         newCQueue(saveLastDistTxns),
	}, nil
}

// SetOpFilter allows to restrict skip ops by specific conditions
func (o *OplogRestore) SetOpFilter(f OpFilter) {
	if f == nil {
		f = DefaultOpFilter
	}

	o.filter = f
}

// SetTimeframe sets boundaries for the replayed operations. All operations
// that happened before `start` and after `end` are going to be discarded.
// Zero `end` (primitive.Timestamp{T:0}) means all chunks will be replayed
// utill the end (no tail trim).
func (o *OplogRestore) SetTimeframe(start, end primitive.Timestamp) {
	o.startTS = start
	o.endTS = end
}

// Apply applys an oplog from a given source
func (o *OplogRestore) Apply(src io.ReadCloser) (lts primitive.Timestamp, err error) {
	bsonSource := db.NewDecodedBSONSource(db.NewBufferlessBSONSource(src))
	defer bsonSource.Close()

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

		// skip if operation happened before the desired time frame
		if primitive.CompareTimestamp(o.startTS, oe.Timestamp) == 1 {
			continue
		}

		// finish if operation happened after the desired time frame (oe.Timestamp > to)
		if o.endTS.T > 0 && primitive.CompareTimestamp(oe.Timestamp, o.endTS) == 1 {
			return lts, nil
		}

		err = o.handleOp(oe)
		if err != nil {
			return lts, err
		}

		lts = oe.Timestamp
		// keeping track of last applied (observed) clusterTime
		atomic.StoreUint32(&o.lastOpT, oe.Timestamp.T)
	}

	return lts, bsonSource.Err()
}

func (o *OplogRestore) SetIncludeNS(nss []string) {
	if len(nss) == 0 {
		o.includeNS = nil
		return
	}

	dbs := make(map[string]map[string]bool)
	for _, ns := range nss {
		d, c, _ := strings.Cut(ns, ".")
		if d == "*" {
			d = ""
		}
		if c == "*" {
			c = ""
		}

		colls := dbs[d]
		if colls == nil {
			colls = make(map[string]bool)
		}
		colls[c] = true
		dbs[d] = colls
	}

	o.includeNS = dbs
}

func (o *OplogRestore) isOpSelected(oe *Record) bool {
	if o.includeNS == nil || o.includeNS[""] != nil {
		return true
	}

	d, c, _ := strings.Cut(oe.Namespace, ".")
	colls := o.includeNS[d]
	if colls[""] || colls[c] {
		return true
	}

	if oe.Operation != "c" || c != "$cmd" {
		return false
	}

	m := oe.Object.Map()
	for _, cmd := range selectedNSSupportedCommands {
		if ns, ok := m[cmd]; ok {
			s, _ := ns.(string)
			return colls[s]
		}
	}

	return false
}

func (o *OplogRestore) LastOpTS() uint32 {
	return atomic.LoadUint32(&o.lastOpT)
}

func (o *OplogRestore) handleOp(oe db.Oplog) error {
	// skip if operation happened after the desired time frame (oe.Timestamp > o.lastTS)
	if o.endTS.T > 0 && primitive.CompareTimestamp(oe.Timestamp, o.endTS) == 1 {
		return nil
	}

	if o.excludeNS.Has(oe.Namespace) {
		return nil
	}

	// skip no-ops
	if oe.Operation == "n" {
		return nil
	}

	if !o.isOpSelected(&oe) {
		return nil
	}

	if !o.filter(&oe) {
		return nil
	}

	if oe.Operation == "c" && len(oe.Object) > 0 &&
		(oe.Object[0].Key == "startIndexBuild" || oe.Object[0].Key == "abortIndexBuild") {
		return nil
	}

	// optimization - not to parse namespace if it remains the same
	if o.cnamespase != oe.Namespace {
		o.preserveUUID = o.preserveUUIDopt

		// if this is a create operation, the namesape would be
		// inside the object to create
		if oe.Operation == "c" {
			if len(oe.Object) == 0 {
				return errors.Errorf("empty object value for op: %v", oe)
			}
			if oe.Object[0].Key == "create" && o.noUUIDns.Has(oe.Namespace+"."+oe.Object[0].Value.(string)) {
				o.preserveUUID = false
			}
		}
		// don't preserve UUID for certain namespaces
		if o.noUUIDns.Has(oe.Namespace) {
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

func isTxnOps(op *db.Oplog) bool {
	for _, v := range op.Object {
		if v.Key == "applyOps" {
			return true
		}
	}

	return false
}

func isPartial(op *db.Oplog) bool {
	for _, v := range op.Object {
		if v.Key == "partialTxn" {
			return true
		}
	}

	return false
}

type Txn struct {
	Oplog    []db.Oplog
	meta     txn.Meta
	applyOps []db.Oplog
	allOps   bool
}

// handleTxnOp accumulates transaction's ops in a buffer and then applies
// it as non-txn ops when observes commit or just purge the buffer if the transaction
// aborted.
// Distributed transactions always consist of at least two ops - `prepare`
// (with txn ops) and `commitTransaction` or abortTransaction.
// Although commitTimestamp for the same txn on different shards always the same,
// the op with `commitTransaction` may have different `ts` (op timestamp). Fo example:
//
//	rs1: { "lsid" : { "id" : UUID("f7ad867d-f9a4-444c-bf5f-7185e7038d6e"), ... },
//			"o" : { "commitTransaction" : 1, "commitTimestamp" : Timestamp(1644410656, 6) },
//			"ts" : Timestamp(1644410656, 9), ... }
//	rs2: { "lsid" : { "id" : UUID("f7ad867d-f9a4-444c-bf5f-7185e7038d6e"), ... },
//			"o" : { "commitTransaction" : 1, "commitTimestamp" : Timestamp(1644410656, 6) },
//			"ts" : Timestamp(1644410656, 8), ... }
//
// Since we sync backup/restore across shards by `ts` (`opTime`), artifacts of such transaction
// would be visible on shard `rs2` and won't appear on `rs1` given the restore time is `(1644410656, 8)`.
//
// We treat distributed transactions as non-distributed - apply ops once
// a commit message for this txn is encountered. But store uncommitted dist txns
// so applier check in the end if any of them committed on other shards
// (and commit if so)
func (o *OplogRestore) handleTxnOp(meta txn.Meta, op db.Oplog) error {
	txnID := fmt.Sprintf("%s-%d", base64.RawStdEncoding.EncodeToString([]byte(op.LSID)), *op.TxnNumber)

	if isTxnOps(&op) {
		ops, err := txnInnerOps(&op)
		if err != nil {
			return err
		}

		t := o.txnData[txnID]
		t.meta = meta
		t.allOps = !isPartial(&op)
		t.Oplog = append(t.Oplog, op)
		t.applyOps = append(t.applyOps, ops...)
		o.txnData[txnID] = t
	}
	if meta.IsAbort() {
		delete(o.txnData, txnID)
		return nil
	}

	if !meta.IsCommit() {
		return nil
	}

	// The first op of the current chunk euquals to the last of the previous.
	// It's done to ensure no gaps in between chunks. Although and oplog ops are
	// idempotent, if the duplication gonna be a commit message of the distributed
	// txn, the second commit gonna fail since we're clearing committed tnxs out
	// of the buffer. So just skip if it is a commit duplication.
	lastc := o.txnCommit.last()
	if lastc != nil && txnID == lastc.ID {
		return nil
	}

	// if the "commit" contains no data, it's a distributed transaction and we
	// preserve it and communicate later to another shards so they can apply
	// prepared txn if its commit didn't get into the oplog time range
	if !meta.IsData() {
		var cts primitive.Timestamp
		for _, v := range op.Object {
			if v.Key == "commitTimestamp" {
				cts = v.Value.(primitive.Timestamp)
			}
		}

		o.txnCommit.push(pbm.RestoreTxn{
			ID:    txnID,
			Ctime: cts,
			State: pbm.TxnCommit,
		})
	}

	// commit transaction
	err := o.applyTxn(txnID)
	if err != nil {
		b, _ := json.MarshalIndent(op, "", " ")
		return errors.Wrapf(err, "apply txn: %s", b)
	}

	delete(o.txnData, txnID)

	return nil
}

const extractErrorFmt = "error extracting transaction ops: %s: %v"

func txnInnerOps(txnOp *db.Oplog) ([]db.Oplog, error) {
	doc := txnOp.Object
	rawAO, err := bsonutil.FindValueByKey("applyOps", &doc)
	if err != nil {
		return nil, fmt.Errorf(extractErrorFmt, "applyOps field", err)
	}

	ao, ok := rawAO.(bson.A)
	if !ok {
		return nil, fmt.Errorf(extractErrorFmt, "applyOps field", "not a BSON array")
	}

	ops := make([]db.Oplog, len(ao))
	for i, v := range ao {
		opDoc, ok := v.(bson.D)
		if !ok {
			return nil, fmt.Errorf(extractErrorFmt, "applyOps op", "not a BSON document")
		}
		op, err := bsonDocToOplog(opDoc)
		if err != nil {
			return nil, fmt.Errorf(extractErrorFmt, "applyOps op", err)
		}

		// The inner ops doesn't have these fields,
		// so we are assigning them from the parent transaction op
		op.Timestamp = txnOp.Timestamp
		op.Term = txnOp.Term
		op.Hash = txnOp.Hash

		ops[i] = *op
	}

	return ops, nil
}

const opConvertErrorFmt = "error converting bson.D to op: %s: %v"

func bsonDocToOplog(doc bson.D) (*db.Oplog, error) {
	op := db.Oplog{}

	for _, v := range doc {
		switch v.Key {
		case "op":
			s, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "op field", "not a string")
			}
			op.Operation = s
		case "ns":
			s, ok := v.Value.(string)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "ns field", "not a string")
			}
			op.Namespace = s
		case "o":
			d, ok := v.Value.(bson.D)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "o field", "not a BSON Document")
			}
			op.Object = d
		case "o2":
			d, ok := v.Value.(bson.D)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "o2 field", "not a BSON Document")
			}
			op.Query = d
		case "ui":
			u, ok := v.Value.(primitive.Binary)
			if !ok {
				return nil, fmt.Errorf(opConvertErrorFmt, "ui field", "not binary data")
			}
			op.UI = &u
		}
	}

	return &op, nil
}

func (o *OplogRestore) applyTxn(id string) (err error) {
	t, ok := o.txnData[id]
	if !ok {
		return errors.Errorf("unknown transaction id %s", id)
	}

	for _, op := range t.applyOps {
		err = o.handleNonTxnOp(op)
		if err != nil {
			return errors.Wrap(err, "applying transaction op")
		}
	}

	delete(o.txnData, id)
	return nil
}

func (o *OplogRestore) TxnLeftovers() (uncommitted map[string]Txn, lastCommits []pbm.RestoreTxn) {
	return o.txnData, o.txnCommit.s
}

func (o *OplogRestore) HandleUncommittedTxn(commits map[string]primitive.Timestamp) (partial, uncommitted []Txn, err error) {
	if len(o.txnData) == 0 {
		return nil, nil, nil
	}

	for id, t := range o.txnData {
		if _, ok := commits[id]; ok {
			if !t.allOps {
				partial = append(partial, t)
				continue
			}

			err := o.applyTxn(id)
			if err != nil {
				return partial, uncommitted, errors.Wrapf(err, "applying uncommitted txn %s", id)
			}
			delete(o.txnData, id)
		}
	}

	for _, t := range o.txnData {
		uncommitted = append(uncommitted, t)
	}

	return partial, uncommitted, nil
}

func (o *OplogRestore) handleNonTxnOp(op db.Oplog) error {
	// have to handle it here one more time because before the op gets thru
	// txnBuffer its namespace is `collection.$cmd` instead of the real one
	if o.excludeNS.Has(op.Namespace) {
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

			collName, ok := op.Object[0].Value.(string)
			if !ok {
				return errors.Errorf("could not parse collection name from op: %v", op)
			}

			o.indexCatalog.AddIndex(dbName, collName, index)
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
				bsonutil.RemoveKey("noPadding", &op.Object)
				bsonutil.RemoveKey("usePowerOf2Sizes", &op.Object)
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
			o.indexCatalog.DropCollection(dbName, collName)

			collation, err := bsonutil.FindSubdocumentByKey("collation", &op.Object)
			if err != nil {
				o.indexCatalog.SetCollation(dbName, collName, true)
			}
			localeValue, _ := bsonutil.FindValueByKey("locale", &collation)
			if localeValue == "simple" {
				o.indexCatalog.SetCollation(dbName, collName, true)
			}

			op2 := op
			op2.Object = bson.D{{"drop", collName}}
			if err := o.handleNonTxnOp(op2); err != nil {
				return errors.WithMessage(err, "oplog: drop collection before create")
			}
		}
	}

	err = o.applyOps([]interface{}{op})
	if err != nil {
		// https://jira.percona.com/browse/PBM-818
		if o.unsafe &&
			strings.Contains(err.Error(), "E11000 duplicate key error") &&
			op.Namespace == "config.chunks" {
			return nil
		}

		opb, errm := json.Marshal(op)
		return errors.Wrapf(err, "op: %s | merr %v", opb, errm)
	}

	return nil
}

type cqueue struct {
	s []pbm.RestoreTxn
	c int
}

func newCQueue(cap int) *cqueue {
	return &cqueue{s: make([]pbm.RestoreTxn, 0, cap), c: cap}
}

func (c *cqueue) push(v pbm.RestoreTxn) {
	if len(c.s) == c.c {
		c.s = c.s[1:]
	}

	c.s = append(c.s, v)
}

func (c *cqueue) last() *pbm.RestoreTxn {
	if len(c.s) == 0 {
		return nil
	}

	return &c.s[len(c.s)-1]
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
func (o *OplogRestore) applyOps(entries []interface{}) error {
	singleRes := o.dst.Database("admin").RunCommand(context.TODO(), bson.D{{"applyOps", entries}})
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
func (o *OplogRestore) filterUUIDs(op db.Oplog) (db.Oplog, error) {
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
func (o *OplogRestore) newFilteredApplyOps(cmd bson.D) (bson.D, error) {
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
