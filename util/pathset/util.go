package pathset

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

var (
	InvalidPathErr = errors.New("invalid path")
	NotFoundErr    = errors.New("not found")
)

func GetValue(value interface{}, path string) (interface{}, error) {
	rv, err := followPath(reflect.ValueOf(value), strings.Split(path, "."), false)
	if err != nil {
		return nil, NotFoundErr
	}

	return rv.Interface(), nil
}

func SetValue(target interface{}, path, value string) (err error) {
	v := reflect.ValueOf(target)
	if v.Kind() != reflect.Ptr {
		return errors.New("should be pointer")
	}

	rv, err := followPath(reflect.Indirect(v), strings.Split(path, "."), true)
	if err != nil {
		return err
	}
	if !rv.CanSet() {
		return errors.New("cannot set value")
	}

	parsed, err := parseValue(rv.Kind(), value)
	if err != nil {
		return fmt.Errorf("parse: %s", err.Error())
	}

	vv := reflect.ValueOf(parsed)
	if !vv.CanConvert(rv.Type()) {
		return errors.Errorf("cannot convert %q to %v", value, parsed)
	}

	rv.Set(vv.Convert(rv.Type()))

	return nil
}

func followPath(src reflect.Value, path []string, create bool) (reflect.Value, error) {
	if len(path) == 0 {
		return src, nil
	}

	src = followPtr(src, create)
	switch src.Kind() {
	case reflect.Struct:
		return followStructPath(src, path, create)
	case reflect.Map:
		return followMapPath(src, path, create)
	}

	return reflect.Value{}, errors.New("unsupported nested type")
}

func followStructPath(src reflect.Value, path []string, create bool) (reflect.Value, error) {
	for i := 0; i != src.NumField(); i++ {
		field := src.Type().Field(i)
		if !field.IsExported() {
			continue
		}

		tag, ok := field.Tag.Lookup("json")
		if ok {
			if i := strings.IndexRune(tag, ','); i != -1 {
				tag = tag[:i]
			}
		} else {
			tag = field.Name
		}
		if tag == "" || tag == "-" {
			continue
		}

		if tag == path[0] {
			return followPath(src.Field(i), path[1:], create)
		}
	}

	return reflect.Value{}, NotFoundErr
}

func followMapPath(src reflect.Value, path []string, create bool) (reflect.Value, error) {
	key := reflect.ValueOf(path[0])
	val := src.MapIndex(key)

	if val == (reflect.Value{}) {
		val = newValue(src.Type().Elem())
		if create {
			src.SetMapIndex(key, val)
		}
	}

	return followPath(val, path[1:], create)
}

func followPtr(src reflect.Value, create bool) reflect.Value {
	for src.Kind() == reflect.Ptr {
		if !src.IsNil() {
			src = src.Elem()
			continue
		}

		val := newValue(src.Type().Elem())
		if create {
			src.Set(val.Addr())
		} else {
			src = val
		}
	}

	if src.Kind() == reflect.Map && src.IsNil() {
		val := newValue(src.Type())
		if create {
			src.Set(val)
		} else {
			src = val
		}
	}

	return src
}

func newValue(elem reflect.Type) reflect.Value {
	if elem.Kind() == reflect.Map {
		return reflect.MakeMap(elem)
	} else {
		return reflect.New(elem).Elem()
	}
}

func parseValue(t reflect.Kind, s string) (interface{}, error) {
	switch t {
	case reflect.String:
		return s, nil
	case reflect.Bool:
		return strconv.ParseBool(s)
	case reflect.Int:
		return strconv.Atoi(s)
	case reflect.Float64:
		return strconv.ParseFloat(s, 64)
	}

	return nil, errors.New("unsupported type")
}
