package pbm

import (
	"context"
	"log"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/filters"
	docker "github.com/docker/docker/client"
	"github.com/pkg/errors"
)

type Docker struct {
	cn  *docker.Client
	ctx context.Context
}

func NewDocker(ctx context.Context, host string) (*Docker, error) {
	cn, err := docker.NewClient(host, "1.40", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "docker client")
	}

	return &Docker{
		cn:  cn,
		ctx: ctx,
	}, nil
}

// StopAgents stops agent containers of the given replicaset
func (d *Docker) StopAgents(rsName string) error {
	fltr := filters.NewArgs()
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := d.cn.ContainerList(d.ctx, types.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers) == 0 {
		return errors.Errorf("no containers found for replset %s", rsName)
	}

	for _, c := range containers {
		log.Println("stopping container", c.ID)
		err = d.cn.ContainerStop(d.ctx, c.ID, nil)
		if err != nil {
			return errors.Wrapf(err, "stop container %s", c.ID)
		}
	}

	return nil
}

// StartAgents starts stopped agent containers of the given replicaset
func (d *Docker) StartAgents(rsName string) error {
	fltr := filters.NewArgs()
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := d.cn.ContainerList(d.ctx, types.ContainerListOptions{
		All:     true,
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers) == 0 {
		return errors.Errorf("no containers found for replset %s", rsName)
	}

	for _, c := range containers {
		log.Println("Straing container", c.ID)
		err = d.cn.ContainerStart(d.ctx, c.ID, types.ContainerStartOptions{})
		if err != nil {
			return errors.Wrapf(err, "start container %s", c.ID)
		}
	}

	return nil
}

// RestartAgents restarts agent containers of the given replicaset
func (d *Docker) RestartAgents(rsName string) error {
	fltr := filters.NewArgs()
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := d.cn.ContainerList(d.ctx, types.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}

	for _, c := range containers {
		log.Println("restarting container", c.ID)
		err = d.cn.ContainerRestart(d.ctx, c.ID, nil)
		if err != nil {
			return errors.Wrapf(err, "restart container %s", c.ID)
		}
	}

	return nil
}
