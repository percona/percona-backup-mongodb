package pkg

import (
	"context"
	"time"

	"github.com/docker/docker/api/types"
	docker "github.com/docker/docker/client"
	"github.com/pkg/errors"
)

type Ctl struct {
	cn        *docker.Client
	ctx       context.Context
	container string
}

func NewPBM(ctx context.Context, host string) (*Ctl, error) {
	cn, err := docker.NewClient(host, "1.40", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "docker client")
	}

	return &Ctl{
		cn:        cn,
		ctx:       ctx,
		container: "docker_agent-rs103_1",
	}, nil
}

func (c *Ctl) BackupStart() error {
	id, err := c.cn.ContainerExecCreate(c.ctx, c.container, types.ExecConfig{
		Env: []string{"PBM_MONGODB_URI=mongodb://dba:test1234@rs103:27017"},
		Cmd: []string{"pbm", "backup"},
	})
	if err != nil {
		return errors.Wrap(err, "ContainerExecCreate")
	}

	err = c.cn.ContainerExecStart(c.ctx, id.ID, types.ExecStartCheck{})
	if err != nil {
		return errors.Wrap(err, "ContainerExecStart")
	}

	tmr := time.NewTimer(17 * time.Second)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			return errors.New("timeout reached")
		case <-tkr.C:
			insp, err := c.cn.ContainerExecInspect(c.ctx, id.ID)
			if err != nil {
				return errors.Wrap(err, "ContainerExecInspect")
			}
			if !insp.Running && insp.ExitCode == 0 {
				return nil
			}
		}
	}
}
