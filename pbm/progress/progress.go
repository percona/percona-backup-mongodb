package progress

import (
	"sync"

	"github.com/mongodb/mongo-tools-common/progress"
)

type Collection struct {
	name  string
	curr  int64
	total int64
	p     progress.Progressor
}

type Progress struct {
	exsists map[string]struct{}
	c       []*Collection
	mx      sync.Mutex
}

func (p *Progress) Attach(name string, progressor progress.Progressor) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if _, ok := p.exsists[name]; ok {
		return
	}

	c := &Collection{
		name: name,
		p:    progressor,
	}

	p.c = append(p.c, c)
	p.exsists[name] = struct{}{}
}

func (p *Progress) Detach(name string) {
	p.mx.Lock()
	defer p.mx.Unlock()
	if _, ok := p.exsists[name]; !ok {
		return
	}

	var coll *Collection
	for _, c := range p.c {
		if c.name == name {
			coll = c
			break
		}
	}

	if coll.p == nil {
		return
	}

	coll.curr, coll.total = coll.p.Progress()
	coll.p = nil
}

func (p *Progress) Get() []Collection {
	p.mx.Lock()
	defer p.mx.Unlock()

	cols := make([]Collection, len(p.c))

	for _, c := range p.c {
		cols = append(cols, *c)
	}

	return cols
}

func (c *Collection) Name() string {
	return c.name
}

func (c *Collection) Progress() (current int64, total int64) {
	if c.p != nil {
		return c.p.Progress()
	}

	return c.curr, c.total
}
