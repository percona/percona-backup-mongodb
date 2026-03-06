package oss

import (
	"testing"

	"github.com/percona/percona-backup-mongodb/pbm/storage"
)

func TestGetMaxObjSizeGB(t *testing.T) {
	tests := []struct {
		name string
		cfg  *Config
		want float64
	}{
		{
			name: "nil MaxObjSizeGB returns default",
			cfg:  &Config{},
			want: defaultMaxObjSizeGB,
		},
		{
			name: "MaxObjSizeGB below lower bound returns default",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(0.5)},
			want: defaultMaxObjSizeGB,
		},
		{
			name: "MaxObjSizeGB at lower bound returns configured value",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(storage.LowerValidMaxObjSizeGB))},
			want: storage.LowerValidMaxObjSizeGB,
		},
		{
			name: "MaxObjSizeGB above lower bound returns configured value",
			cfg:  &Config{MaxObjSizeGB: storage.Ref(float64(100))},
			want: 100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.GetMaxObjSizeGB()
			if got != tt.want {
				t.Errorf("GetMaxObjSizeGB: got=%v, want=%v", got, tt.want)
			}
		})
	}
}
