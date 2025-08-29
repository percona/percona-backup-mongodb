package storage

import (
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

const (
	pbmPartToken = ".pbmpart."
)

type SpitMergeMiddleware struct {
	s          Storage
	maxObjSize int64 // in bytes
}

func NewSplitMergeMW(s Storage, maxObjSize float64) Storage {
	maxObjSizeB := int64(maxObjSize * 1024 * 1024 * 1024 * 1024)
	return &SpitMergeMiddleware{
		s:          s,
		maxObjSize: maxObjSizeB,
	}
}

func (sm *SpitMergeMiddleware) Type() Type {
	return sm.s.Type()
}

type wInfo struct {
	n   int64
	err error
}

func (sm *SpitMergeMiddleware) Save(name string, data io.Reader, options ...Option) error {
	fName := name

	wInfoC := make(chan wInfo)
	for {
		pr, pw := io.Pipe()

		go func() {
			n, err := io.CopyN(pw, data, sm.maxObjSize)
			pw.Close()
			wInfoC <- wInfo{n, err}
		}()

		err := sm.s.Save(fName, pr, options...)
		if err != nil {
			return errors.Wrap(err, "save during split-merge mw")
		}

		winfo := <-wInfoC
		if winfo.err != nil {
			if winfo.err == io.EOF && winfo.n != 0 {
				break
			} else if winfo.err == io.EOF && winfo.n == 0 {
				if err := sm.s.Delete(fName); err != nil {
					return errors.Wrap(err, "empty file deletion")
				}
				break
			}
			return errors.Wrap(winfo.err, "write pipeline split-merge mw")
		}

		fName, err = createNextPart(fName)
		if err != nil {
			return errors.Wrap(err, "pbm part name creation")
		}
	}

	return nil
}

func (sm *SpitMergeMiddleware) SourceReader(name string) (io.ReadCloser, error) {
	fi, err := sm.FileWithParts(name)
	if err != nil {
		return nil, errors.Wrap(err, "list with parts for mw source reader")
	}
	if len(fi) <= 1 {
		// the same behaviour like without mw
		return sm.s.SourceReader(name)
	}

	pr, pw := io.Pipe()

	go func() {
		for _, f := range fi {
			r, err := sm.s.SourceReader(f.Name)
			if err != nil {
				pw.CloseWithError(errors.Wrapf(err, "reading pbm part %s", f.Name))
				return
			}
			if _, err = io.Copy(pw, r); err != nil {
				pr.CloseWithError(errors.Wrapf(err, "copy file stream: %s:", f.Name))
				return
			}
			if err = r.Close(); err != nil {
				pr.CloseWithError(errors.Wrapf(err, "closing file stream: %s", f.Name))
				return
			}
		}
		pw.Close()
	}()

	return io.NopCloser(pr), nil
}

func (sm *SpitMergeMiddleware) FileStat(name string) (FileInfo, error) {
	fi, err := sm.FileWithParts(name)
	if err != nil {
		return FileInfo{}, errors.Wrap(err, "list with parts for mw file stat op")
	}
	if len(fi) <= 1 {
		return sm.s.FileStat(name)
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
	var fi []FileInfo
	var err error
	if suffix == ".tmp" {
		fi, err = sm.s.List(prefix, suffix)
	} else {
		// fetch all without suffix, and filter after
		fi, err = sm.s.List(prefix, "")
	}
	if err != nil {
		return nil, errors.Wrap(err, "list files for mw list op")
	}

	baseParts := map[string]int64{}
	for _, f := range fi {
		baseFile := GetBasePart(f.Name)
		if !strings.HasSuffix(baseFile, suffix) {
			continue
		}
		baseParts[baseFile] += f.Size
	}

	res := make([]FileInfo, len(baseParts))
	i := 0
	for f, s := range baseParts {
		res[i] = FileInfo{Name: f, Size: s}
		i++
	}

	return res, nil
}

func (sm *SpitMergeMiddleware) Delete(name string) error {
	fi, err := sm.FileWithParts(name)
	if err != nil {
		return errors.Wrap(err, "list with parts for mw delete op")
	}
	if len(fi) <= 1 {
		return sm.s.Delete(name)
	}

	for _, f := range fi {
		if err = sm.s.Delete(f.Name); err != nil {
			return errors.Wrapf(err, "delete file part: %s", f.Name)
		}
	}

	return nil
}

func (sm *SpitMergeMiddleware) Copy(src, dst string) error {
	fi, err := sm.FileWithParts(src)
	if err != nil {
		return errors.Wrap(err, "list with parts for mw delete op")
	}
	if len(fi) <= 1 {
		return sm.s.Copy(src, dst)
	}

	dstPartName := dst
	for _, f := range fi {
		if f.Name == src {
			// copy base part
			if err = sm.s.Copy(src, dstPartName); err != nil {
				return errors.Wrap(err, "copy base part")
			}
		} else {
			dstPartName, err = createNextPart(dstPartName)
			if err != nil {
				return errors.Wrap(err, "create next part name")
			}
			if err = sm.s.Copy(f.Name, dstPartName); err != nil {
				return errors.Wrapf(err, "copy %s to %s", f.Name, dstPartName)
			}
		}
	}
	return nil
}

// FileWithParts fetches file with base name and all it's PBM parts.
func (sm *SpitMergeMiddleware) FileWithParts(name string) ([]FileInfo, error) {
	d, f := path.Split(name)
	fList, err := sm.s.List(d, "")
	if err != nil {
		return nil, errors.Wrap(err, "list files for mw list op")
	}

	fiParts := []FileInfo{}
	for _, fi := range fList {
		if f == GetBasePart(path.Base(fi.Name)) {
			fiParts = append(fiParts, fi)
		}
	}

	// sort based on the index
	res := make([]FileInfo, len(fiParts))
	for _, f := range fiParts {
		if f.Name == name {
			res[0] = FileInfo{Name: path.Join(d, f.Name), Size: f.Size}
		} else {
			i, err := GetPartIndex(f.Name)
			if err != nil {
				return nil, errors.Wrap(err, "sort file parts")
			}
			res[i] = FileInfo{Name: path.Join(d, f.Name), Size: f.Size}
		}
	}

	return res, nil
}

// setPartsSize set pbm part size (in bytes) for the purpose of unit testing.
func (sm *SpitMergeMiddleware) setPartsSize(maxObjSize int64) {
	sm.maxObjSize = maxObjSize
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

func GetPartIndex(fname string) (int, error) {
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

// GetBasePart extract base part of the file.
// Base part is file without .pbmpart.xy suffix.
func GetBasePart(fname string) string {
	base := fname

	pattern := regexp.MustCompile(`\.pbmpart\.\d+$`)
	if pattern.MatchString(fname) {
		fileParts := strings.Split(fname, ".")
		base = strings.Join(fileParts[:len(fileParts)-2], ".")
	}

	return base
}
