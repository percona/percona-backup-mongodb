package context

import "context"

type (
	Context    = context.Context
	CancelFunc = context.CancelFunc
)

// since 1.20
// type CancelCauseFunc = context.CancelCauseFunc

var (
	Canceled         = context.Canceled
	DeadlineExceeded = context.DeadlineExceeded
)

var (
	Background   = context.Background
	TODO         = context.TODO
	WithCancel   = context.WithCancel
	WithDeadline = context.WithDeadline
	WithTimeout  = context.WithTimeout
	WithValue    = context.WithValue
)

// since 1.20
// var (
// 	Cause           = context.Cause
// 	WithCancelCause = context.WithCancelCause
// )

// since 1.21
// var (
// 	WithoutCancel     = context.WithoutCancel
// 	AfterFunc         = context.AfterFunc
// 	WithDeadlineCause = context.WithDeadlineCause
// 	WithTimeoutCause  = context.WithTimeoutCause
// )
