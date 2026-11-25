package main

import (
	"os"
	"reflect"
	"strings"
	"unsafe"
)

const mauthPrefix = "--" + mongoConnFlag + "="

func setarg(i int, as string) {
	ptr := (*reflect.StringHeader)(unsafe.Pointer(&os.Args[i]))
	arg := (*[1 << 30]byte)(unsafe.Pointer(ptr.Data))[:ptr.Len]

	n := copy(arg, as)
	for ; n < len(arg); n++ {
		arg[n] = 0
	}
}

// hidecreds replaces creds (user & pass) with bunch of `x` if there are any in mongo connection flags
func hidecreds() {
	for k, v := range os.Args {
		if strings.HasPrefix(v, mauthPrefix) {
			uri := strings.TrimPrefix(v, mauthPrefix)
			if masked := maskCreds(uri); masked != uri {
				setarg(k, mauthPrefix+masked)
			}
		}
	}
}

// maskCreds replaces the user and password of a mongo-uri with `x`, keeping
// its length and the rest of it. The mongo-uri isn't parsed as a URL: a unix
// socket host is percent-encoded there, which a URL host can't be.
func maskCreds(uri string) string {
	start := 0
	if i := strings.Index(uri, "://"); i >= 0 {
		start = i + len("://")
	}

	// The credentials end at the last `@` before the path or the options.
	authority := uri[start:]
	if i := strings.IndexAny(authority, "/?"); i >= 0 {
		authority = authority[:i]
	}
	at := strings.LastIndex(authority, "@")
	if at < 0 {
		return uri
	}

	userinfo := []byte(authority[:at])
	sep := strings.IndexByte(authority[:at], ':')
	for i := range userinfo {
		if i != sep {
			userinfo[i] = 'x'
		}
	}

	return uri[:start] + string(userinfo) + uri[start+at:]
}
