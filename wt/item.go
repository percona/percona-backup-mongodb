package wt

// #cgo CFLAGS: --std=c11
// #cgo CFLAGS: -Wall -Wextra -Wpedantic
//
// #include <stdlib.h>
// #include <wiredtiger.h>
//
// int s_unpack_int64(WT_SESSION *sess, WT_ITEM *item, int64_t *n) {
//     return wiredtiger_struct_unpack(sess, item->data, item->size, "q", n);
// }
//
// int s_pack_int64(WT_SESSION *sess, void *data, size_t size, int64_t n) {
//     return wiredtiger_struct_pack(sess, data, size, "q", n);
// }
//
// int s_pack_size_int64(WT_SESSION *sess, size_t *size, int64_t n) {
//     return wiredtiger_struct_size(sess, size, "q", n);
// }
import "C"

import "unsafe"

type WTItem C.WT_ITEM

func NewItem(data []byte) *WTItem {
	return &WTItem{
		data: C.CBytes(data),
		size: C.size_t(len(data)),
	}
}

func NewStringItem(data string) *WTItem {
	return &WTItem{
		data: unsafe.Pointer(C.CString(data)),
		size: C.size_t(len(data) + 1),
	}
}

func NewInt64Item(sess *WTSession, n int64) (*WTItem, error) {
	var size C.size_t
	if ret := C.s_pack_size_int64((*C.WT_SESSION)(sess), &size, C.int64_t(n)); ret != 0 {
		return nil, sess.wtError(ret)
	}

	data := C.malloc(size)
	if ret := C.s_pack_int64((*C.WT_SESSION)(sess), data, size, C.int64_t(n)); ret != 0 {
		C.free(data)
		return nil, sess.wtError(ret)
	}

	return &WTItem{data: data, size: size}, nil
}

func (w *WTItem) Drop() {
	if w.data == nil {
		return
	}

	C.free(w.data)
	w.data = nil
}

func (w *WTItem) Int64(sess *WTSession) (int64, error) {
	var n C.int64_t
	if ret := C.s_unpack_int64((*C.WT_SESSION)(sess), (*C.WT_ITEM)(w), &n); ret != 0 {
		return 0, sess.wtError(ret)
	}

	return int64(n), nil
}

func (w *WTItem) String() string {
	return C.GoString((*C.char)(w.data))
}

func (w *WTItem) Bytes() []byte {
	return C.GoBytes(w.data, C.int(w.size))
}
