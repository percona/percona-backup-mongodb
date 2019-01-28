package storage

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kr/pretty"
)

func TestLoadFromYaml(t *testing.T) {
	s, err := NewStorageBackendsFromYaml(filepath.Join("testdata", "test.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	want := Storages{
		Storages: map[string]Storage{
			"s3-us-west": {
				Type: "s3",
				S3: S3{
					Region:      "us-west",
					EndpointURL: "https://minio",
					Bucket:      "bucket name",
					Credentials: Credentials{
						AccessKeyID:     "<something>",
						SecretAccessKey: "<something>",
						Vault: struct {
							Server string "yaml:\"server\""
							Secret string "yaml:\"secret\""
							Token  string "yaml:\"token\""
						}{Server: "localhost:port", Secret: "dont tell", Token: "anyone"},
					},
				},
				Filesystem: Filesystem{},
			},
			"local-filesystem": {
				Type:       "filesystem",
				S3:         S3{},
				Filesystem: Filesystem{Path: "path/to/the/backup/dir"},
			},
		},
	}
	if !reflect.DeepEqual(want, s) {
		t.Errorf("Want:\n:%s\nGot:\n%s\n", pretty.Sprint(want), pretty.Sprint(s))
	}
}

func TestExists(t *testing.T) {
	s, err := NewStorageBackendsFromYaml(filepath.Join("testdata", "test.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		Name string
		Want bool
	}{
		{Name: "s3-us-west", Want: true},
		{Name: "local-filesystem", Want: true},
		{Name: "an-invalid-name", Want: false},
	}
	for _, tc := range tcs {
		if exists := s.Exists(tc.Name); exists != tc.Want {
			t.Errorf("Exists for storage name %s should be %v, got %v", tc.Name, tc.Want, exists)
		}
	}
}

func TestStorageInfo(t *testing.T) {
	s, err := NewStorageBackendsFromYaml(filepath.Join("testdata", "test.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	tcs := []struct {
		Name string
		Want StorageInfo
	}{
		{
			Name: "s3-us-west",
			Want: StorageInfo{
				Type: "s3",
				S3: S3{
					Region:      "us-west",
					EndpointURL: "https://minio",
					Bucket:      "bucket name",
					Credentials: Credentials{},
				},
				Filesystem: Filesystem{},
			},
		},
		{
			Name: "local-filesystem",
			Want: StorageInfo{
				Type:       "filesystem",
				S3:         S3{},
				Filesystem: Filesystem{Path: "path/to/the/backup/dir"},
			},
		},
	}

	for _, tc := range tcs {
		info, err := s.StorageInfo(tc.Name)
		if err != nil {
			t.Errorf("Cannot get info for storage %s: %s", tc.Name, err)
			continue
		}
		if !reflect.DeepEqual(info, tc.Want) {
			t.Errorf("Invalid info for storage name %s:\nGot: %s\n,Want: %s", tc.Name, pretty.Sprint(info), pretty.Sprint(tc.Want))
		}
	}
}

func TestStoragesInfo(t *testing.T) {
	s, err := NewStorageBackendsFromYaml(filepath.Join("testdata", "test.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]StorageInfo{
		"s3-us-west": {
			Type: "s3",
			S3: S3{
				Region:      "us-west",
				EndpointURL: "https://minio",
				Bucket:      "bucket name",
				Credentials: Credentials{},
			},
			Filesystem: Filesystem{},
		},
		"local-filesystem": {
			Type:       "filesystem",
			S3:         S3{},
			Filesystem: Filesystem{Path: "path/to/the/backup/dir"},
		},
	}

	si := s.StoragesInfo()
	if !reflect.DeepEqual(si, want) {
		t.Errorf("Invalid list of storages:\n%v", pretty.Diff(want, si))
	}
}
