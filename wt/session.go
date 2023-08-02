package wt

// #cgo CFLAGS: --std=c11
// #cgo CFLAGS: -Wall -Wextra -Wpedantic
// #cgo CFLAGS: -Wno-strict-prototypes
// #cgo LDFLAGS: -lwiredtiger
//
// #include <stdlib.h>
// #include <wiredtiger.h>
//
// int connection_close(WT_CONNECTION *conn) {
//     return conn->close(conn, NULL);
// }
//
// int open_session(WT_CONNECTION *conn, const char *cfg, WT_SESSION **sess) {
//     return conn->open_session(conn, NULL, cfg, sess);
// }
//
// int session_close(WT_SESSION *sess) {
//     return sess->close(sess, NULL);
// }
//
// int session_create(WT_SESSION *sess, const char *name, const char *config) {
//     return sess->create(sess, name, config);
// }
//
// int session_checkpoint(WT_SESSION *sess) {
//     return sess->checkpoint(sess, NULL);
// }
//
// const char *session_strerror(WT_SESSION *sess, int ret) {
//     return sess->strerror(sess, ret);
// }
import "C"

import (
	"errors"
	"fmt"
	"unsafe"
)

type WTSession C.WT_SESSION

func OpenSession(home, config string) (*WTSession, error) {
	h := unsafe.Pointer(C.CString(home))
	c := unsafe.Pointer(C.CString(config))
	defer C.free(c)
	defer C.free(h)

	var conn *C.WT_CONNECTION
	if ret := C.wiredtiger_open((*C.char)(h), nil, (*C.char)(c), &conn); ret != 0 {
		return nil, fmt.Errorf("open connection: %w", wtError(ret))
	}

	var sess *C.WT_SESSION
	if ret := C.open_session(conn, nil, &sess); ret != 0 {
		err := fmt.Errorf("open session: %w", wtError(ret))
		_ = closeConnection(conn)
		return nil, err
	}

	return (*WTSession)(sess), nil
}

func (s *WTSession) Close() error {
	sess := (*C.WT_SESSION)(s)
	conn := sess.connection

	defer func() {
		_ = closeConnection(conn)
	}()

	if ret := C.session_close(sess); ret != 0 {
		return fmt.Errorf("close session: %w", s.wtError(ret))
	}

	return nil
}

func (s *WTSession) wtError(e C.int) error {
	return errors.New(C.GoString(C.session_strerror((*C.WT_SESSION)(s), e)))
}

func (s *WTSession) OpenCursor(name string) (*WTCursor, error) {
	return newCursor((*C.WT_SESSION)(s), name)
}

func (s *WTSession) Create(name, config string) error {
	n := unsafe.Pointer(C.CString(name))
	c := unsafe.Pointer(C.CString(config))
	defer C.free(c)
	defer C.free(n)

	ret := C.session_create((*C.WT_SESSION)(s), (*C.char)(n), (*C.char)(c))
	if ret != 0 && ret != 17 {
		return s.wtError(ret)
	}

	return nil
}

func (s *WTSession) Checkpoint() error {
	if ret := C.session_checkpoint((*C.WT_SESSION)(s)); ret != 0 {
		return s.wtError(ret)
	}

	return nil
}

func (s *WTSession) importTable(name string) error {
	const config = "import=(enabled=true,repair=true,compare_timestamp=stable)"
	return s.Create("table:"+name, config)
}

func closeConnection(conn *C.WT_CONNECTION) error {
	if ret := C.connection_close(conn); ret != 0 {
		return fmt.Errorf("close connection: %w", wtError(ret))
	}

	return nil
}

func wtError(v C.int) error {
	return errors.New(C.GoString(C.wiredtiger_strerror(v)))
}
