package main

import (
	"fmt"
	"log"
	"net/url"
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

// hidecreds erases creds (user & pass) if there are any in mongo connection flags
func hidecreds() {
	for k, v := range os.Args {
		if strings.HasPrefix(v, mauthPrefix) {
			str := strings.TrimPrefix(v, mauthPrefix)
			u, err := url.Parse(str)
			if err != nil {
				log.Println(err)
				return
			}
			if u.User == nil || u.User.String() == "" {
				return
			}

			u.User = nil
			fmt.Println(u.String())
			setarg(k, mauthPrefix+u.String())
		}
	}
}
