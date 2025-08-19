package storage

import (
	"fmt"
	"io"
	"regexp"
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
	files, err := sm.s.List(name, "")
	if err != nil || len(files) <= 1 {
		// let's handle this case like there's no middleware
		return sm.s.SourceReader(name)
	}

	sortedNames := make([]string, len(files))
	for _, f := range files {
		i, err := getPartIndex(f.Name)
		if err != nil {
			return nil, errors.Wrap(err, "parsing id pbm part from files")
		}
		sortedNames[i] = f.Name
	}

	partReaders := make([]io.Reader, len(sortedNames))
	for i, f := range sortedNames {
		partReaders[i], err = sm.s.SourceReader(f)
		if err != nil {
			return nil, errors.Wrapf(err, "reading pbm part %s", f)
		}
	}
	mr := io.MultiReader(partReaders...)
	return io.NopCloser(mr), nil
}

func (sm *SpitMergeMiddleware) FileStat(name string) (FileInfo, error) {
	fi, err := sm.listWithParts(name)
	if err != nil {
		return FileInfo{}, errors.Wrap(err, "list with parts for mw file stat op")
	}

	totalSize := int64(0)
	for _, f := range fi {
		totalSize += f.Size
	}
	res := FileInfo{
		Name: fi[0].Name, // base part has 0 index
		Size: totalSize,
	}

	return res, nil
}

func (sm *SpitMergeMiddleware) List(prefix, suffix string) ([]FileInfo, error) {
	fi, err := sm.s.List(prefix, suffix)
	if err != nil {
		return nil, errors.Wrap(err, "list files")
	}

	res := slices.DeleteFunc(
		fi,
		func(f FileInfo) bool { return strings.Contains(f.Name, pbmPartToken) },
	)

	return res, nil
}

func (sm *SpitMergeMiddleware) Delete(name string) error {
	fi, err := sm.listWithParts(name)
	if err != nil {
		return errors.Wrap(err, "list with parts for mw delete op")
	}

	for _, f := range fi {
		if err = sm.s.Delete(f.Name); err != nil {
			return errors.Wrapf(err, "delete file part: %s", f.Name)
		}
	}

	return nil
}

func (sm *SpitMergeMiddleware) Copy(src, dst string) error {
	fi, err := sm.listWithParts(src)
	if err != nil {
		return errors.Wrap(err, "list with parts for mw delete op")
	}

	dstPartName := dst
	for _, f := range fi {
		if f.Name != src {
			// copy base part
			sm.s.Copy(src, dstPartName)
		} else {
			dstPartName, err := createNextPart(dstPartName)
			if err != nil {
				return errors.Wrap(err, "create next part name")
			}
			if err := sm.s.Copy(f.Name, dstPartName); err != nil {
				return errors.Wrapf(err, "copy %s to %s", f.Name, dstPartName)
			}
		}
	}

	return nil
}

// listWithParts fetches file with base name and all it's PBM parts.
func (sm *SpitMergeMiddleware) listWithParts(name string) ([]FileInfo, error) {
	fi, err := sm.s.List(name, "")
	if err != nil {
		return nil, errors.Wrap(err, "list files for mw list op")
	}

	fiParts := []FileInfo{}
	for _, f := range fi {
		if getBasePart(name) == name {
			fiParts = append(fiParts, f)
		}
	}

	// sort based on the index
	res := make([]FileInfo, len(fiParts))
	for _, f := range fiParts {
		if f.Name == name {
			res[0] = f
		} else {
			i, err := getPartIndex(f.Name)
			if err != nil {
				return nil, errors.Wrap(err, "sort file parts")
			}
			res[i] = f
		}
	}
	//todo: add validation (holes)

	return res, nil
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

func getPartIndex(fname string) (int, error) {
	partID := 0
	if strings.Contains(fname, pbmPartToken) {
		fileParts := strings.Split(fname, ".")

		var err error
		partID, err = strconv.Atoi(fileParts[len(fileParts)-1])
		if err != nil {
			return 0, errors.Wrap(err, "parsing id pbm part")
		}
	}

	return partID, nil
}

func getBasePart(fname string) string {
	base := fname

	pattern := regexp.MustCompile(`\.pbmpart\.\d+$`)
	if pattern.MatchString(fname) {
		fileParts := strings.Split(fname, ".")
		base = strings.Join(fileParts[:len(fileParts)-2], ".")
	}

	return base
}
