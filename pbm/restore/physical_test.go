package restore

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm/log"
)

func TestMoveAll(t *testing.T) {
	t.Run("move all files and dir", func(t *testing.T) {
		tempSrc, _ := os.MkdirTemp("", "src")
		defer os.RemoveAll(tempSrc)

		tempDst, _ := os.MkdirTemp("", "dst")
		defer os.RemoveAll(tempDst)

		testFiles := []string{"file1", "file2", "file3"}
		for i, file := range testFiles {
			_ = os.WriteFile(
				filepath.Join(tempSrc, file),
				[]byte(fmt.Sprintf("test content %d", i)), 0o644)
		}

		subDir := filepath.Join(tempSrc, "subdir")
		_ = os.Mkdir(subDir, 0o755)

		err := moveAll(tempSrc, tempDst, nil, log.DiscardLogger.NewDefaultEvent())
		if err != nil {
			t.Fatalf("moveAll failed: %v", err)
		}

		// files are moved
		for _, file := range testFiles {
			if _, err := os.Stat(filepath.Join(tempDst, file)); os.IsNotExist(err) {
				t.Errorf("file %s not found in destination directory", file)
			}
			if _, err := os.Stat(filepath.Join(tempSrc, file)); !os.IsNotExist(err) {
				t.Errorf("file %s still exists in source directory", file)
			}
		}

		// subdir is moved
		if _, err := os.Stat(filepath.Join(tempDst, "subdir")); os.IsNotExist(err) {
			t.Errorf("subdirectory not found in destination directory")
		}
		if _, err := os.Stat(filepath.Join(tempSrc, "subdir")); !os.IsNotExist(err) {
			t.Errorf("subdirectory still exists in source directory")
		}
	})

	t.Run("ignore files and dirs", func(t *testing.T) {
		tempSrc, _ := os.MkdirTemp("", "src")
		defer os.RemoveAll(tempSrc)

		tempDst, _ := os.MkdirTemp("", "dst")
		defer os.RemoveAll(tempDst)

		testFiles := []string{"file1", "file2", "ignore_me"}
		for i, file := range testFiles {
			_ = os.WriteFile(
				filepath.Join(tempSrc, file),
				[]byte(fmt.Sprintf("test content %d", i)), 0o644)
		}

		_ = os.Mkdir(filepath.Join(tempSrc, "ignore_dir"), 0o755)
		_ = os.Mkdir(filepath.Join(tempSrc, "normal_dir"), 0o755)

		toIgnore := []string{"ignore_me", "ignore_dir"}

		err := moveAll(tempSrc, tempDst, toIgnore, log.DiscardLogger.NewDefaultEvent())
		if err != nil {
			t.Fatalf("moveAll failed: %v", err)
		}

		// non-ignored files are moved
		movedFiles := []string{"file1", "file2"}
		for _, file := range movedFiles {
			if _, err := os.Stat(filepath.Join(tempDst, file)); os.IsNotExist(err) {
				t.Errorf("file %s not found in destination directory", file)
			}
		}

		// ignored items remain in source
		for _, item := range toIgnore {
			if _, err := os.Stat(filepath.Join(tempSrc, item)); os.IsNotExist(err) {
				t.Errorf("ignored item %s not found in source directory", item)
			}
			if _, err := os.Stat(filepath.Join(tempDst, item)); !os.IsNotExist(err) {
				t.Errorf("ignored item %s was moved to destination directory", item)
			}
		}

		// non-ignored directory is moved
		if _, err := os.Stat(filepath.Join(tempDst, "normal_dir")); os.IsNotExist(err) {
			t.Errorf("normal directory not found in destination directory")
		}
	})

	t.Run("source dir doesn't exist", func(t *testing.T) {
		tempDst, _ := os.MkdirTemp("", "dst")
		defer os.RemoveAll(tempDst)

		nonExistentDir := "/path/not/exist"

		err := moveAll(nonExistentDir, tempDst, nil, log.DiscardLogger.NewDefaultEvent())
		if err == nil {
			t.Fatal("want error")
		}
		if !strings.Contains(err.Error(), "open dir") {
			t.Errorf("want:'open dir', got:%v", err)
		}
	})

	t.Run("write permission error", func(t *testing.T) {
		tempSrc, _ := os.MkdirTemp("", "src")
		defer os.RemoveAll(tempSrc)

		tempDst, _ := os.MkdirTemp("", "dst")
		defer os.RemoveAll(tempDst)

		_ = os.Chmod(tempDst, 0o400)

		// Create test file in source
		_ = os.WriteFile(
			filepath.Join(tempSrc, "test"),
			[]byte("test content"), 0o644)

		err := moveAll(tempSrc, tempDst, nil, log.DiscardLogger.NewDefaultEvent())
		if err == nil {
			t.Fatal("want perm error")
		}

		if !strings.Contains(err.Error(), "move test") {
			t.Errorf("want:'move test', got:%v", err)
		}
	})
}

func TestWaitMgoFreePort(t *testing.T) {
	t.Run("wait for the port a bit", func(t *testing.T) {
		ln, err := net.Listen("tcp", ":0")
		if err != nil {
			t.Fatalf("failed to bind port: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port

		portUsed := 5 * time.Second
		go func() {
			time.Sleep(portUsed)
			ln.Close()
		}()

		start := time.Now()
		err = waitMgoFreePort(port)
		duration := time.Since(start)

		if err != nil {
			t.Fatalf("error while waiting for the free port: %v", err)
		}
		if duration < portUsed-time.Second ||
			duration > portUsed+time.Second {
			t.Fatalf("wrong duration time, want~=%v, got=%v", portUsed, duration)
		}
	})

	t.Run("sequential check", func(t *testing.T) {
		var err error
		for i := range 10 {
			start := time.Now()
			err = waitMgoFreePort(8088)
			if err != nil {
				t.Fatalf("free port err: %v", err)
			}
			t.Logf("finish %d round, with duration %s", i+1, time.Since(start))
		}
	})
}
