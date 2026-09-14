package idx

import (
	"maps"
	"strings"

	mongoidx "github.com/mongodb/mongo-tools/common/idx"
	"github.com/mongodb/mongo-tools/common/options"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-backup-mongodb/pbm/version"
)

const bucketPrefix = "system.buckets."

// BuildGroup contains prepared index specifications for one createIndexes command.
type BuildGroup struct {
	Collection string // Command target, without the database prefix.
	RawData    bool   // Bypass logical TS index-key translation.
	Indexes    []*mongoidx.IndexDocument
}

func (g *BuildGroup) removeIDIndex() {
	for i, index := range g.Indexes {
		if len(index.Key) == 1 && index.Key[0].Key == "_id" {
			g.Indexes = append(g.Indexes[:i], g.Indexes[i+1:]...)
			return
		}
	}
}

// Catalog tracks pending indexes under logical collection namespaces.
type Catalog struct {
	catalog *mongoidx.IndexCatalog
	// Only bucket specifications have markers, keyed by logical namespace and index name.
	bucketSpecs     map[options.Namespace]map[string]bool
	supportsRawData bool
}

// NewCatalog creates an empty index catalog configured for the target MongoDB version.
func NewCatalog(targetVersion *version.MongoVersion) *Catalog {
	return &Catalog{
		catalog:         mongoidx.NewIndexCatalog(),
		bucketSpecs:     make(map[options.Namespace]map[string]bool),
		supportsRawData: targetVersion.SupportsRawData(),
	}
}

// Namespaces returns logical catalog owners, including entries with no indexes.
func (c *Catalog) Namespaces() []options.Namespace {
	return c.catalog.Namespaces()
}

// AddIndex stores a normal or logical TS specification, replacing the same index name.
func (c *Catalog) AddIndex(database, collection string, index *mongoidx.IndexDocument) {
	c.addIndex(database, collection, index, false)
}

// AddIndexes stores a batch of specifications via AddIndex.
func (c *Catalog) AddIndexes(database, collection string, indexes []*mongoidx.IndexDocument) {
	for _, index := range indexes {
		c.AddIndex(database, collection, index)
	}
}

// AddOplogIndex stores an oplog specification, recognizing legacy bucket namespaces.
func (c *Catalog) AddOplogIndex(database, collection string, index *mongoidx.IndexDocument) {
	c.addIndex(database, collection, index, strings.HasPrefix(collection, bucketPrefix))
}

// AddOplogIndexes stores a batch of oplog specifications.
func (c *Catalog) AddOplogIndexes(database, collection string, indexes []*mongoidx.IndexDocument) {
	for _, index := range indexes {
		c.AddOplogIndex(database, collection, index)
	}
}

func (c *Catalog) addIndex(database, collection string, index *mongoidx.IndexDocument, isBucket bool) {
	name, ok := index.Options["name"].(string)
	if !ok {
		return
	}
	ns := logicalNamespace(database, collection)
	c.catalog.AddIndex(ns.DB, ns.Collection, index)
	if isBucket {
		if c.bucketSpecs[ns] == nil {
			c.bucketSpecs[ns] = make(map[string]bool)
		}
		c.bucketSpecs[ns][name] = true
	} else {
		delete(c.bucketSpecs[ns], name)
		if len(c.bucketSpecs[ns]) == 0 {
			delete(c.bucketSpecs, ns)
		}
	}
}

// GetIndex returns an index by name.
func (c *Catalog) GetIndex(database, collection, index string) *mongoidx.IndexDocument {
	ns := logicalNamespace(database, collection)
	return c.catalog.GetIndex(ns.DB, ns.Collection, index)
}

// GetIndexes returns the collection's indexes, adding explicit simple collation where required.
func (c *Catalog) GetIndexes(database, collection string) []*mongoidx.IndexDocument {
	ns := logicalNamespace(database, collection)
	return c.catalog.GetIndexes(ns.DB, ns.Collection)
}

// SetCollation records whether a collection uses simple collation.
func (c *Catalog) SetCollation(database, collection string, simple bool) {
	ns := logicalNamespace(database, collection)
	c.catalog.SetCollation(ns.DB, ns.Collection, simple)
}

// DropDatabase removes a database from the catalog.
func (c *Catalog) DropDatabase(database string) {
	c.catalog.DropDatabase(database)
	for ns := range c.bucketSpecs {
		if ns.DB == database {
			delete(c.bucketSpecs, ns)
		}
	}
}

// DropCollection removes a collection from the catalog.
func (c *Catalog) DropCollection(database, collection string) {
	ns := logicalNamespace(database, collection)
	c.catalog.DropCollection(ns.DB, ns.Collection)
	delete(c.bucketSpecs, ns)
}

// RenameCollection replaces destination state after a successful MongoDB rename.
// An untracked source has no pending indexes, so it clears destination state too.
// Each rename must be processed once; replay deduplication is a separate concern.
func (c *Catalog) RenameCollection(fromDB, fromCollection, toDB, toCollection string) {
	from := logicalNamespace(fromDB, fromCollection)
	to := logicalNamespace(toDB, toCollection)
	if from == to {
		return
	}

	// GetIndexes materializes index collation before the source entry is removed.
	indexes := c.catalog.GetIndexes(from.DB, from.Collection)
	bucketSpecs := c.bucketSpecs[from]
	c.DropCollection(from.DB, from.Collection)
	c.DropCollection(to.DB, to.Collection)
	c.catalog.AddIndexes(to.DB, to.Collection, indexes)
	if len(bucketSpecs) != 0 {
		c.bucketSpecs[to] = bucketSpecs
	}
}

// DeleteIndexes removes matching indexes. Key-pattern selectors must match the stored keys.
func (c *Catalog) DeleteIndexes(database, collection string, command bson.D) error {
	ns := logicalNamespace(database, collection)
	if err := c.catalog.DeleteIndexes(ns.DB, ns.Collection, command); err != nil {
		return err
	}
	maps.DeleteFunc(c.bucketSpecs[ns], func(name string, _ bool) bool {
		return c.catalog.GetIndex(ns.DB, ns.Collection, name) == nil
	})
	if len(c.bucketSpecs[ns]) == 0 {
		delete(c.bucketSpecs, ns)
	}
	return nil
}

// CollMod updates an index without changing its build routing.
func (c *Catalog) CollMod(database, collection string, indexMod any) error {
	ns := logicalNamespace(database, collection)
	return c.catalog.CollMod(ns.DB, ns.Collection, indexMod)
}

// BuildGroups returns nonempty, ready-to-build direct and bucket groups, in that order.
// It excludes each group's _id index, sets the target ns, and removes v from copies.
func (c *Catalog) BuildGroups(database, collection string) []BuildGroup {
	ns := logicalNamespace(database, collection)
	directGroup := c.newBuildGroup(ns.Collection, false)
	bucketGroup := c.newBuildGroup(ns.Collection, true)

	for _, index := range c.catalog.GetIndexes(ns.DB, ns.Collection) {
		name, _ := index.Options["name"].(string)
		group := &directGroup
		if c.bucketSpecs[ns][name] {
			group = &bucketGroup
		}
		copyIndex := *index
		copyIndex.Options = maps.Clone(index.Options)
		copyIndex.Options["ns"] = ns.DB + "." + group.Collection
		delete(copyIndex.Options, "v")
		group.Indexes = append(group.Indexes, &copyIndex)
	}

	// Exclude the _id index from the pending builds.
	directGroup.removeIDIndex()
	bucketGroup.removeIDIndex()

	var groups []BuildGroup
	if len(directGroup.Indexes) != 0 {
		groups = append(groups, directGroup)
	}
	if len(bucketGroup.Indexes) != 0 {
		groups = append(groups, bucketGroup)
	}
	return groups
}

func (c *Catalog) newBuildGroup(collection string, isBucket bool) BuildGroup {
	if isBucket && !c.supportsRawData {
		collection = bucketPrefix + collection
	}
	return BuildGroup{Collection: collection, RawData: isBucket && c.supportsRawData}
}

func logicalNamespace(database, collection string) options.Namespace {
	return options.Namespace{DB: database, Collection: strings.TrimPrefix(collection, bucketPrefix)}
}
