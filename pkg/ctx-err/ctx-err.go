package ctxerr

import (
	"fmt"
	"strings"
	"sync"

	"golang.org/x/net/context"
)

type CtxErrCanceler struct {
	CancelFunc  context.CancelFunc
	cancelErrs  []error
	cancelErrMu sync.Mutex
}

func (c *CtxErrCanceler) Cancel(err error) {
	c.cancelErrMu.Lock()
	c.cancelErrs = append(c.cancelErrs, err)
	c.CancelFunc()
	c.cancelErrMu.Unlock()
}

func (c *CtxErrCanceler) Errors() []error {
	return c.cancelErrs
}

func (c *CtxErrCanceler) IsCanceled() bool {
	return len(c.cancelErrs) > 0
}

func (c *CtxErrCanceler) String() string {
	if len(c.cancelErrs) == 0 {
		return "No errors"
	}

	b := strings.Builder{}

	b.WriteString("Errors in order of occurrence:\n")
	for i, err := range c.cancelErrs {
		b.WriteString(fmt.Sprintf("\t%d. %s\n", i+1, err))
	}
	return b.String()
}
