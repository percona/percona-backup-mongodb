package pathset_test

import (
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/percona/percona-backup-mongodb/pbm"
	"github.com/percona/percona-backup-mongodb/util/pathset"
)

func TestPBMStorageConfig(t *testing.T) {
	rnd := rand.New(rand.NewSource(time.Now().Unix()))
	target, _ := quick.Value(reflect.TypeOf(pbm.StorageConf{}), rnd)

	for path, tp := range scanKeys(target) {
		gen, _ := quick.Value(tp, rnd)
		val := formatValue(gen)

		cnf := pbm.StorageConf{}
		err := pathset.SetValue(&cnf, path, val)
		if err != nil {
			t.Errorf("set %s: %s: %q", path, err.Error(), val)
			continue
		}

		got, err := pathset.GetValue(cnf, path)
		if err != nil {
			t.Errorf("get %s: %s", path, err.Error())
			continue
		}

		want := gen.Interface()
		if !reflect.DeepEqual(want, got) {
			t.Errorf("%s: want %v, got %v", path, want, got)
		}
	}
}

func scanKeys(value reflect.Value) map[string]reflect.Type {
	for value.Kind() == reflect.Ptr {
		if value.IsNil() {
			elem := value.Type().Elem()
			if elem.Kind() != reflect.Struct {
				return nil
			}

			value = reflect.New(elem)
		}

		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.Struct:
		return scanStructFields(value)
	case reflect.Map:
		return scanMapKeys(value)
	}

	return nil
}

func scanStructFields(value reflect.Value) map[string]reflect.Type {
	rv := make(map[string]reflect.Type)

	for i := 0; i != value.NumField(); i++ {
		structField := value.Type().Field(i)
		if !structField.IsExported() {
			continue
		}

		tag, ok := structField.Tag.Lookup("json")
		if ok {
			if i := strings.IndexRune(tag, ','); i != -1 {
				tag = tag[:i]
			}
		} else {
			tag = structField.Name
		}
		if tag == "" || tag == "-" {
			continue
		}

		nested := scanKeys(value.Field(i))
		if len(nested) != 0 {
			for k, t := range nested {
				rv[tag+"."+k] = t
			}
		} else {
			rv[tag] = structField.Type
		}
	}

	return rv
}

func scanMapKeys(value reflect.Value) map[string]reflect.Type {
	rv := make(map[string]reflect.Type)

	for it := value.MapRange(); it.Next(); {
		tag, field := it.Key().String(), it.Value()
		if nested := scanKeys(field); len(nested) != 0 {
			for k, t := range nested {
				rv[tag+"."+k] = t
			}
		} else {
			rv[tag] = field.Type()
		}
	}

	return rv
}

func formatValue(val reflect.Value) string {
	switch val.Kind() {
	case reflect.String:
		return val.String()
	case reflect.Bool:
		return strconv.FormatBool(val.Bool())
	case reflect.Int:
		return strconv.Itoa(int(val.Int()))
	case reflect.Float64:
		return strconv.FormatFloat(val.Float(), 'e', -1, 64)
	}

	panic("unsupported value")
}
