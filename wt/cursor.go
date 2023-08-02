package wt

// #cgo CFLAGS: --std=c11
// #cgo CFLAGS: -Wall -Wextra -Wpedantic
// #cgo LDFLAGS: -lwiredtiger
//
// #include <stdlib.h>
// #include <wiredtiger.h>
//
// int open_cursor(WT_SESSION *sess, const char *name, WT_CURSOR **cur) {
//     return sess->open_cursor(sess, name, NULL, "raw", cur);
// }
//
// int cursor_close(WT_CURSOR *cur) {
//     return cur->close(cur);
// }
//
// int cursor_next(WT_CURSOR *cur) {
//     return cur->next(cur);
// }
//
// int cursor_get_key(WT_CURSOR *cur, WT_ITEM *key) {
//     return cur->get_key(cur, key);
// }
//
// int cursor_get_value(WT_CURSOR *cur, WT_ITEM *val) {
//     return cur->get_value(cur, val);
// }
//
// void cursor_set_key(WT_CURSOR *cur, WT_ITEM *key) {
//     cur->set_key(cur, key);
// }
//
// void cursor_set_value(WT_CURSOR *cur, WT_ITEM *val) {
//     cur->set_value(cur, val);
// }
//
// int cursor_insert(WT_CURSOR *cur) {
//     return cur->insert(cur);
// }
//
// int cursor_search(WT_CURSOR *cur) {
//     return cur->search(cur);
// }
//
// int cursor_largest_key(WT_CURSOR *cur) {
//     return cur->largest_key(cur);
// }
import "C"

import (
	"errors"
	"unsafe"
)

var ErrNotFound = errors.New("not found")

type WTCursor C.WT_CURSOR

func newCursor(sess *C.WT_SESSION, name string) (*WTCursor, error) {
	t := unsafe.Pointer(C.CString(name))
	defer C.free(t)

	var cur *C.WT_CURSOR
	ret := C.open_cursor(sess, (*C.char)(t), &cur)
	if ret != 0 {
		return nil, (*WTSession)(sess).wtError(ret)
	}

	return (*WTCursor)(cur), nil
}

func (c *WTCursor) Close() error {
	ret := C.cursor_close((*C.WT_CURSOR)(c))
	if ret != 0 {
		return (*WTSession)(c.session).wtError(ret)
	}

	return nil
}

func (c *WTCursor) Next() error {
	ret := C.cursor_next((*C.WT_CURSOR)(c))
	if ret != 0 {
		if ret == C.WT_NOTFOUND {
			return ErrNotFound
		}

		return (*WTSession)(c.session).wtError(ret)
	}

	return nil
}

func (c *WTCursor) Key() (*WTItem, error) {
	item := &C.WT_ITEM{}
	ret := C.cursor_get_key((*C.WT_CURSOR)(c), item)
	if ret != 0 {
		return nil, (*WTSession)(c.session).wtError(ret)
	}

	return (*WTItem)(item), nil
}

func (c *WTCursor) Value() (*WTItem, error) {
	item := &C.WT_ITEM{}
	ret := C.cursor_get_value((*C.WT_CURSOR)(c), item)
	if ret != 0 {
		return nil, (*WTSession)(c.session).wtError(ret)
	}

	return (*WTItem)(item), nil
}

func (c *WTCursor) Insert(key, val *WTItem) error {
	C.cursor_set_key((*C.WT_CURSOR)(c), (*C.WT_ITEM)(key))
	C.cursor_set_value((*C.WT_CURSOR)(c), (*C.WT_ITEM)(val))

	ret := C.cursor_insert((*C.WT_CURSOR)(c))
	if ret != 0 {
		return (*WTSession)(c.session).wtError(ret)
	}

	return nil
}

func (c *WTCursor) Search(key *WTItem) error {
	C.cursor_set_key((*C.WT_CURSOR)(c), (*C.WT_ITEM)(key))

	ret := C.cursor_search((*C.WT_CURSOR)(c))
	if ret != 0 {
		if ret == C.WT_NOTFOUND {
			return ErrNotFound
		}

		return (*WTSession)(c.session).wtError(ret)
	}

	return nil
}

func (c *WTCursor) LargestKey() (*WTItem, error) {
	ret := C.cursor_largest_key((*C.WT_CURSOR)(c))
	if ret != 0 {
		if ret == C.WT_NOTFOUND {
			return nil, ErrNotFound
		}

		return nil, (*WTSession)(c.session).wtError(ret)
	}

	return c.Key()
}
