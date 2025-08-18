package storage

import "testing"

func TestCreateNextPart(t *testing.T) {
	t.Run("next part for base file", func(t *testing.T) {
		fname := "file_name"
		want := "file_name.pbmpart.1"

		got, err := createNextPart(fname)

		if err != nil {
			t.Errorf("got error: %v", err)
		}
		if got != want {
			t.Errorf("want=%s, got=%v", want, got)
		}
	})

	t.Run("next part for the first part", func(t *testing.T) {
		fname := "file_name.pbmpart.1"
		want := "file_name.pbmpart.2"

		got, err := createNextPart(fname)

		if err != nil {
			t.Errorf("got error: %v", err)
		}
		if got != want {
			t.Errorf("want=%s, got=%v", want, got)
		}
	})

	t.Run("next part for index 9", func(t *testing.T) {
		fname := "file_name.pbmpart.9"
		want := "file_name.pbmpart.10"

		got, err := createNextPart(fname)

		if err != nil {
			t.Errorf("got error: %v", err)
		}
		if got != want {
			t.Errorf("want=%s, got=%v", want, got)
		}
	})

	t.Run("error while parsing index", func(t *testing.T) {
		fname := "file_name.pbmpart.X"

		got, err := createNextPart(fname)

		if err == nil {
			t.Error("want error, get nil")
		}
		if got != "" {
			t.Error("file name should be empty string")
		}
	})

	t.Run("token exists, base part doesn't", func(t *testing.T) {
		fname := ".pbmpart.5"
		want := ".pbmpart.6"

		got, err := createNextPart(fname)

		if err != nil {
			t.Errorf("got error: %v", err)
		}
		if got != want {
			t.Errorf("want=%s, got=%v", want, got)
		}
	})

	t.Run("token exists, index doesn't", func(t *testing.T) {
		fname := "file_name.pbmpart."

		got, err := createNextPart(fname)

		if err == nil {
			t.Error("want error, get nil")
		}
		if got != "" {
			t.Error("file name should be empty string")
		}
	})
}
