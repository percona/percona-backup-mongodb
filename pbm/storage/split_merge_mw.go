package storage

import (
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const (
	pbmPartSize  = 20 * 1024 * 1024 // 20MB
	pbmPartToken = ".pbmpart."
)

type SpitMergeMiddleware struct {
	s Storage
}

func NewSplitMergeMW(s Storage) Storage {
	return &SpitMergeMiddleware{
		s: s,
	}
}

func (sm *SpitMergeMiddleware) Type() Type {
	return sm.s.Type()
}

func (sm *SpitMergeMiddleware) Save(name string, data io.Reader, options ...Option) error {
	fName := name

	errC := make(chan error)
	for {
		pr, pw := io.Pipe()

		go func() {
			_, err := io.CopyN(pw, data, pbmPartSize)
			pw.Close()
			errC <- err
		}()

		err := sm.s.Save(fName, pr, options...)
		if err != nil {
			return errors.Wrap(err, "save during split-merge mw")
		}

		wErr := <-errC
		if wErr != nil {
			if wErr == io.EOF {
				break
			}
			return errors.Wrap(err, "write pipeline split-merge mw")
		}

		fName, err = createNextPart(fName)
		if err != nil {
			return errors.Wrap(err, "pbm part name creation")
		}
	}

	return nil
}

func (sm *SpitMergeMiddleware) SourceReader(name string) (io.ReadCloser, error) {
	return sm.s.SourceReader(name)
}

func (sm *SpitMergeMiddleware) FileStat(name string) (FileInfo, error) {
	return sm.s.FileStat(name)
}

func (sm *SpitMergeMiddleware) List(prefix, suffix string) ([]FileInfo, error) {
	return sm.s.List(prefix, suffix)
}
func (sm *SpitMergeMiddleware) Delete(name string) error {
	return sm.s.Delete(name)
}
func (sm *SpitMergeMiddleware) Copy(src, dst string) error {
	return sm.s.Copy(src, dst)
}

// createNextPart returns file name for the next pbm part.
// Input for the name creation is the last part name: base part or any indexed part.
// For part names PBM uses following naming schema:
// file_name.pbmpart.15, where:
// - file_name is the base file name
// - `.pbmpart.` is token that identifies PBM's multi files schema naming
// - 15 is part index
//
// Example of PBM's multi-files schena on disk:
// collection-14-4294136943066280761.wt <-- base part, it has zero-based index which is omitted
// collection-14-4294136943066280761.wt.pbmpart.1 <-- second part (part index 1)
// collection-14-4294136943066280761.wt.pbmpart.2 <-- third part (part index 2)
// collection-14-4294136943066280761.wt.pbmpart.3 <-- the last part
func createNextPart(fname string) (string, error) {
	if strings.Contains(fname, pbmPartToken) {
		fileParts := strings.Split(fname, ".")

		partID, err := strconv.Atoi(fileParts[len(fileParts)-1])
		if err != nil {
			return "", errors.Wrap(err, "parsing id pbm part")
		}
		partID++

		fNewName := fmt.Sprintf("%s.%d", strings.Join(fileParts[:len(fileParts)-1], "."), partID)
		return fNewName, nil
	} else {
		// creating part name based on base part: e.g. base-file.pbmpart.1
		return fmt.Sprintf("%s%s1", fname, pbmPartToken), nil
	}
}
