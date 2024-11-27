package main

import (
	"errors"
	"reflect"
	"testing"
)

func TestCloningValidation(t *testing.T) {
	testCases := []struct {
		desc    string
		opts    restoreOpts
		wantErr error
	}{
		{
			desc: "ns-to options is missing when cloning",
			opts: restoreOpts{
				nsFrom: "d.c",
			},
			wantErr: ErrNSToMissing,
		},
		{
			desc: "ns-from options is missing when cloning",
			opts: restoreOpts{
				nsTo: "d.c",
			},
			wantErr: ErrNSFromMissing,
		},
		{
			desc: "cloning with selective restore is not allowed",
			opts: restoreOpts{
				nsFrom: "d.c1",
				nsTo:   "d.c2",
				ns:     "d.c",
			},
			wantErr: ErrSelAndCloning,
		},
		{
			desc: "cloning with restoring users and roles are not allowed",
			opts: restoreOpts{
				nsFrom:        "d.c1",
				nsTo:          "d.c2",
				usersAndRoles: true,
			},
			wantErr: ErrCloningWithUAndR,
		},
		{
			desc: "cloning with PITR is not allowed",
			opts: restoreOpts{
				nsFrom: "d.c1",
				nsTo:   "d.c2",
				pitr:   "2024-10-27T11:23:30",
			},
			wantErr: ErrCloningWithPITR,
		},
		{
			desc: "cloning with wild cards within nsFrom",
			opts: restoreOpts{
				nsFrom: "d.*",
				nsTo:   "d.c2",
			},
			wantErr: ErrCloningWithWildCards,
		},
		{
			desc: "cloning with wild cards within nsTo",
			opts: restoreOpts{
				nsFrom: "d.c1",
				nsTo:   "d.*",
			},
			wantErr: ErrCloningWithWildCards,
		},
		{
			desc: "cloning with ns without dot within nsFrom",
			opts: restoreOpts{
				nsFrom: "c",
				nsTo:   "c.d",
			},
			wantErr: ErrInvalidNamespace,
		},
		{
			desc: "cloning with ns without dot within nsTo",
			opts: restoreOpts{
				nsFrom: "d.c",
				nsTo:   "d",
			},
			wantErr: ErrInvalidNamespace,
		},
		{
			desc: "no error without cloning options",
			opts: restoreOpts{
				nsFrom: "",
				nsTo:   "",
			},
			wantErr: nil,
		},
		{
			desc: "no error when cloning options are correct",
			opts: restoreOpts{
				nsFrom: "b.a",
				nsTo:   "d.c",
			},
			wantErr: nil,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.desc, func(t *testing.T) {
			err := validateNSFromNSTo(&tC.opts)
			if !errors.Is(err, tC.wantErr) {
				t.Errorf("Invalid validation error: want=%v, got=%v", tC.wantErr, err)
			}
		})
	}
}

func Test_parseCLINumInsertionWorkersOption(t *testing.T) {

	type args struct {
		value int32
	}

	var num int32 = 1

	tests := []struct {
		name    string
		args    args
		want    *int32
		wantErr bool
	}{
		{
			name: "valid number - no error",
			args: args{
				value: 1,
			},
			want:    &num,
			wantErr: false,
		},
		{
			name: "zero  - no error, but return nil",
			args: args{
				value: 0,
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "negative value  - error",
			args: args{
				value: -1,
			},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseCLINumInsertionWorkersOption(tt.args.value)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseCLINumInsertionWorkersOption() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseCLINumInsertionWorkersOption() got = %v, want %v", got, tt.want)
			}
		})
	}
}
