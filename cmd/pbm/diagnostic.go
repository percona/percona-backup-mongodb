package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/sdk"
)

type diagnosticOptions struct {
	path string
	opid string
	name string
}

func handleDiagnostic(
	ctx context.Context,
	pbm *sdk.Client,
	opts diagnosticOptions,
) (fmt.Stringer, error) {
	if opts.opid == "" && opts.name == "" {
		return nil, errors.New("--opid or --name must be provided")
	}

	if opts.opid == "" {
		cid, err := sdk.FindCommandIDByName(ctx, pbm, opts.name)
		if err != nil {
			if errors.Is(err, sdk.ErrNotFound) {
				return nil, errors.New("command not found")
			}
			return nil, errors.Wrap(err, "find opid by name")
		}
		opts.opid = string(cid)
	}

	report, err := sdk.Diagnostic(ctx, pbm, sdk.CommandID(opts.opid))
	if err != nil {
		return nil, err
	}

	if fileInfo, err := os.Stat(opts.path); err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "stat")
		}
		err = os.MkdirAll(opts.path, 0o777)
		if err != nil {
			return nil, errors.Wrap(err, "create path")
		}
	} else if !fileInfo.IsDir() {
		return nil, errors.Errorf("%s is not a dir", opts.path)
	}

	err = writeToFile(opts.path, opts.opid+".report.json", report)
	if err != nil {
		return nil, errors.Wrapf(err,
			"failed to save %s", filepath.Join(opts.path, opts.opid+".report.json"))
	}

	switch report.Command.Cmd {
	case sdk.CmdBackup:
		meta, err := pbm.GetBackupByOpID(ctx, opts.opid, sdk.GetBackupByNameOptions{})
		if err != nil {
			return nil, errors.Wrap(err, "get backup meta")
		}
		err = writeToFile(opts.path, opts.opid+".backup.json", meta)
		if err != nil {
			return nil, errors.Wrapf(err,
				"failed to save %s", filepath.Join(opts.path, opts.opid+".backup.json"))
		}
	case sdk.CmdRestore:
		meta, err := pbm.GetRestoreByOpID(ctx, opts.opid)
		if err != nil {
			return nil, errors.Wrap(err, "get restore meta")
		}
		err = writeToFile(opts.path, opts.opid+".restore.json", meta)
		if err != nil {
			return nil, errors.Wrapf(err,
				"failed to save %s", filepath.Join(opts.path, opts.opid+".restore.json"))
		}
	}

	err = writeLogToFile(ctx, pbm, opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to save command log")
	}

	return outMsg{""}, nil
}

//nolint:nonamedreturns
func writeLogToFile(ctx context.Context, pbm *sdk.Client, opts diagnosticOptions) (err error) {
	filename := filepath.Join(opts.path, opts.opid+".log")
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			file.Close()
			os.Remove(filename)
		}
	}()

	cur, err := sdk.CommandLogCursor(ctx, pbm, sdk.CommandID(opts.opid))
	if err != nil {
		return errors.Wrap(err, "open log cursor")
	}
	defer cur.Close(ctx)

	eol := []byte("\n")
	for cur.Next(ctx) {
		rec, err := cur.Record()
		if err != nil {
			return errors.Wrap(err, "log: decode")
		}

		data, err := bson.MarshalExtJSON(rec, true, true)
		if err != nil {
			return errors.Wrap(err, "log: encode")
		}

		n, err := file.Write(data)
		if err != nil {
			return errors.Wrap(err, "log: write")
		}
		if n != len(data) {
			return errors.Wrap(io.ErrShortWrite, "log")
		}

		n, err = file.Write(eol)
		if err != nil {
			return errors.Wrap(err, "log: write")
		}
		if n != len(eol) {
			return errors.Wrap(io.ErrShortWrite, "log")
		}
	}

	err = cur.Err()
	if err != nil {
		return errors.Wrap(err, "log cursor")
	}

	err = file.Close()
	if err != nil {
		return errors.Wrap(err, "failed to save file command.log")
	}

	return nil
}

func writeToFile(dirname, name string, val any) error {
	data, err := bson.MarshalExtJSONIndent(val, true, true, "", "  ")
	if err != nil {
		return errors.Wrap(err, "marshal")
	}

	file, err := os.Create(filepath.Join(dirname, name))
	if err != nil {
		return err
	}
	defer file.Close()

	n, err := file.Write(data)
	if err != nil {
		return errors.Wrap(err, "write")
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	err = file.Close()
	if err != nil {
		return errors.Wrap(err, "close file")
	}

	return nil
}
