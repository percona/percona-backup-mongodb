package archive

import (
	"io"
	"strings"
	"sync"

	"github.com/mongodb/mongo-tools/common/archive"
	"github.com/mongodb/mongo-tools/common/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx/bsoncore"
	"golang.org/x/sync/errgroup"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type (
	NewWriter func(ns string) (io.WriteCloser, error)
	NewReader func(ns string) (io.ReadCloser, error)
)

type archiveMeta struct {
	Header     *archive.Header `bson:",inline"`
	Namespaces []*Namespace    `bson:"namespaces"`
}

type Namespace struct {
	*archive.CollectionMetadata `bson:",inline"`

	CRC  int64 `bson:"crc"`
	Size int64 `bson:"size"`
}

const MetaFile = "metadata.json"

const MaxBSONSize = db.MaxBSONSize

var terminatorBytes = []byte{0xFF, 0xFF, 0xFF, 0xFF}

type (
	NSFilterFn  func(ns string) bool
	DocFilterFn func(ns string, d bson.Raw) bool
)

func DefaultNSFilter(string) bool { return true }

func DefaultDocFilter(string, bson.Raw) bool { return true }

func Decompose(r io.Reader, newWriter NewWriter, nsFilter NSFilterFn, docFilter DocFilterFn) error {
	meta, err := readPrelude(r)
	if err != nil {
		return errors.Wrap(err, "prelude")
	}

	if nsFilter == nil {
		nsFilter = DefaultNSFilter
	}
	if docFilter == nil {
		docFilter = DefaultDocFilter
	}

	c := newConsumer(newWriter, nsFilter, docFilter)
	if err := (&archive.Parser{In: r}).ReadAllBlocks(c); err != nil {
		return errors.Wrap(err, "archive parser")
	}

	// save metadata for selected namespaces only
	nss := make([]*Namespace, 0, len(meta.Namespaces))
	for _, n := range meta.Namespaces {
		ns := NSify(n.Database, n.Collection)
		if !nsFilter(ns) {
			continue
		}

		n.CRC = c.crc[ns]
		n.Size = c.size[ns]
		nss = append(nss, n)
	}
	meta.Namespaces = nss

	err = writeMetadata(meta, newWriter)
	return errors.Wrap(err, "metadata")
}

func Compose(w io.Writer, nsFilter NSFilterFn, newReader NewReader) error {
	meta, err := readMetadata(newReader)
	if err != nil {
		return errors.Wrap(err, "metadata")
	}

	nss := make([]*Namespace, 0, len(meta.Namespaces))
	for _, ns := range meta.Namespaces {
		if nsFilter(NSify(ns.Database, ns.Collection)) {
			nss = append(nss, ns)
		}
	}

	meta.Namespaces = nss

	if err := writePrelude(w, meta); err != nil {
		return errors.Wrap(err, "prelude")
	}

	err = writeAllNamespaces(w, newReader,
		int(meta.Header.ConcurrentCollections),
		meta.Namespaces)
	return errors.Wrap(err, "write namespaces")
}

func readPrelude(r io.Reader) (*archiveMeta, error) {
	prelude := archive.Prelude{}
	err := prelude.Read(r)
	if err != nil {
		return nil, errors.Wrap(err, "read")
	}

	m := &archiveMeta{Header: prelude.Header}
	m.Namespaces = make([]*Namespace, len(prelude.NamespaceMetadatas))
	for i, n := range prelude.NamespaceMetadatas {
		m.Namespaces[i] = &Namespace{CollectionMetadata: n}
	}

	return m, nil
}

func writePrelude(w io.Writer, m *archiveMeta) error {
	prelude := archive.Prelude{Header: m.Header}
	prelude.NamespaceMetadatas = make([]*archive.CollectionMetadata, len(m.Namespaces))
	for i, n := range m.Namespaces {
		prelude.NamespaceMetadatas[i] = n.CollectionMetadata
	}

	err := prelude.Write(w)
	return errors.Wrap(err, "write")
}

func writeAllNamespaces(w io.Writer, newReader NewReader, lim int, nss []*Namespace) error {
	mu := sync.Mutex{}
	eg := errgroup.Group{}
	eg.SetLimit(lim)

	for _, ns := range nss {
		ns := ns

		eg.Go(func() error {
			if ns.Size == 0 {
				mu.Lock()
				defer mu.Unlock()

				return errors.Wrap(closeChunk(w, ns), "close empty chunk")
			}

			nss := NSify(ns.Database, ns.Collection)
			r, err := newReader(nss)
			if err != nil {
				return errors.Wrap(err, "new reader")
			}
			defer r.Close()

			err = splitChunks(r, MaxBSONSize*2, func(b []byte) error {
				mu.Lock()
				defer mu.Unlock()

				return errors.Wrap(writeChunk(w, ns, b), "write chunk")
			})
			if err != nil {
				return errors.Wrap(err, "split")
			}

			mu.Lock()
			defer mu.Unlock()

			return errors.Wrap(closeChunk(w, ns), "close chunk")
		})
	}

	return eg.Wait()
}

func splitChunks(r io.Reader, size int, write func([]byte) error) error {
	chunk := make([]byte, 0, size)
	buf, err := ReadBSONBuffer(r, nil)
	for err == nil {
		if len(chunk)+len(buf) > size {
			if err := write(chunk); err != nil {
				return err
			}
			chunk = chunk[:0]
		}

		chunk = append(chunk, buf...)
		buf, err = ReadBSONBuffer(r, buf[:cap(buf)])
	}

	if !errors.Is(err, io.EOF) {
		return errors.Wrap(err, "read bson")
	}

	if len(chunk) == 0 {
		return nil
	}

	return errors.Wrap(write(chunk), "last")
}

// ReadBSONBuffer reads raw bson document from r reader using buf buffer
func ReadBSONBuffer(r io.Reader, buf []byte) ([]byte, error) {
	var l [4]byte

	_, err := io.ReadFull(r, l[:])
	if err != nil {
		return nil, errors.Wrap(err, "length")
	}

	size := int(int32(l[0]) | int32(l[1])<<8 | int32(l[2])<<16 | int32(l[3])<<24)
	if size < 0 {
		return nil, bsoncore.ErrInvalidLength
	}

	if len(buf) < size {
		buf = make([]byte, size)
	}

	copy(buf, l[:])
	_, err = io.ReadFull(r, buf[4:size])
	if err != nil {
		return nil, err
	}

	if buf[size-1] != 0x00 {
		return nil, bsoncore.ErrMissingNull
	}

	return buf[:size], nil
}

func writeChunk(w io.Writer, ns *Namespace, data []byte) error {
	nsHeader := archive.NamespaceHeader{
		Database:   ns.Database,
		Collection: ns.Collection,
	}
	if ns.Type == "timeseries" {
		nsHeader.Collection = "system.buckets." + nsHeader.Collection
	}

	header, err := bson.Marshal(nsHeader)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := SecureWrite(w, header); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := SecureWrite(w, data); err != nil {
		return errors.Wrap(err, "data")
	}

	err = SecureWrite(w, terminatorBytes)
	return errors.Wrap(err, "terminator")
}

func closeChunk(w io.Writer, ns *Namespace) error {
	nsHeader := archive.NamespaceHeader{
		Database:   ns.Database,
		Collection: ns.Collection,
		EOF:        true,
		CRC:        ns.CRC,
	}
	if ns.Type == "timeseries" {
		nsHeader.Collection = "system.buckets." + nsHeader.Collection
	}

	header, err := bson.Marshal(nsHeader)
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	if err := SecureWrite(w, header); err != nil {
		return errors.Wrap(err, "header")
	}

	err = SecureWrite(w, terminatorBytes)
	return errors.Wrap(err, "terminator")
}

func writeMetadata(meta *archiveMeta, newWriter NewWriter) error {
	w, err := newWriter(MetaFile)
	if err != nil {
		return errors.Wrap(err, "new writer")
	}
	defer w.Close()

	data, err := bson.MarshalExtJSONIndent(meta, true, true, "", "\t")
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	return SecureWrite(w, data)
}

func readMetadata(newReader NewReader) (*archiveMeta, error) {
	r, err := newReader(MetaFile)
	if err != nil {
		return nil, errors.Wrap(err, "new metafile reader")
	}
	defer r.Close()

	return ReadMetadata(r)
}

func ReadMetadata(r io.Reader) (*archiveMeta, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read")
	}

	meta := &archiveMeta{}
	err = bson.UnmarshalExtJSON(data, true, meta)
	return meta, errors.Wrap(err, "unmarshal")
}

type consumer struct {
	open      NewWriter
	nsFilter  NSFilterFn
	docFilter DocFilterFn
	nss       map[string]io.WriteCloser
	crc       map[string]int64
	size      map[string]int64
	curr      string
}

func newConsumer(newWriter NewWriter, nsFilter NSFilterFn, docFilter DocFilterFn) *consumer {
	return &consumer{
		open:      newWriter,
		nsFilter:  nsFilter,
		docFilter: docFilter,
		nss:       make(map[string]io.WriteCloser),
		crc:       make(map[string]int64),
		size:      make(map[string]int64),
	}
}

func (c *consumer) HeaderBSON(data []byte) error {
	h := &archive.NamespaceHeader{}
	if err := bson.Unmarshal(data, h); err != nil {
		return errors.Wrap(err, "unmarshal")
	}

	ns := NSify(h.Database, h.Collection)
	if !c.nsFilter(ns) {
		// non-selected namespace. ignore
		c.curr = ""
		return nil
	}

	if !h.EOF {
		c.curr = ns
		return nil
	}

	c.crc[ns] = h.CRC

	w := c.nss[ns]
	if w == nil {
		return nil
	}

	delete(c.nss, ns)
	return errors.Wrap(w.Close(), "close")
}

func (c *consumer) BodyBSON(data []byte) error {
	ns := c.curr
	if ns == "" {
		// ignored ns. skip data loading/reading
		return nil
	}
	if !c.docFilter(ns, bson.Raw(data)) {
		// ignored doc. skip data loading/reading
		return nil
	}

	w := c.nss[ns]
	if w == nil {
		var err error
		w, err = c.open(ns)
		if err != nil {
			return errors.Wrapf(err, "open: %q", ns)
		}

		c.nss[ns] = w
	}

	c.size[ns] += int64(len(data))
	return errors.Wrapf(SecureWrite(w, data), "%q", ns)
}

func (c *consumer) End() error {
	errs := []error{}

	wg := &sync.WaitGroup{}
	mu := &sync.Mutex{}

	wg.Add(len(c.nss))
	for ns, w := range c.nss {
		go func() {
			defer wg.Done()

			err := w.Close()
			if err != nil {
				mu.Lock()
				errs = append(errs, errors.Wrapf(err, "close: %q", ns))
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return errors.Join(errs...)
}

func SecureWrite(w io.Writer, data []byte) error {
	n, err := w.Write(data)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(data) {
		return io.ErrShortWrite
	}

	return nil
}

func NSify(db, coll string) string {
	return db + "." + strings.TrimPrefix(coll, "system.buckets.")
}
