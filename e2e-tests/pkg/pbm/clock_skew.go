package pbm

import (
	"context"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	docker "github.com/docker/docker/client"
	"github.com/pkg/errors"
)

func ClockSkew(rsName, ts, dockerHost string) error {
	cn, err := docker.NewClient(dockerHost, "1.40", nil, nil)
	if err != nil {
		return errors.Wrap(err, "docker client")
	}

	// fltrImg := filters.NewArgs()
	// fltrImg.Add("label", "com.percona.pbm.app=agent")
	// imgs, err := cn.ImageList(context.Background(), types.ImageListOptions{
	// 	Filters: fltrImg,
	// })
	// if err != nil {
	// 	return errors.Wrap(err, "images list")
	// }
	// if len(imgs) == 0 {
	// 	return errors.New("no images found")
	// }

	fltr := filters.NewArgs()
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := cn.ContainerList(context.Background(), types.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}

	for _, c := range containers {
		containerOld, err := cn.ContainerInspect(context.Background(), c.ID)
		if err != nil {
			return errors.Wrapf(err, "ContainerInspect for %s", c.ID)
		}

		err = cn.ContainerRemove(context.Background(), c.ID, types.ContainerRemoveOptions{Force: true})
		if err != nil {
			return errors.Wrapf(err, "remove container %s", c.ID)
		}

		envs := append(containerOld.Config.Env, []string{
			`LD_PRELOAD=/usr/local/lib/libfaketime.so.1`,
			`FAKETIME="` + ts + `"`,
		}...)
		containerNew, err := cn.ContainerCreate(context.Background(), &container.Config{
			Image: containerOld.Image,
			Env:   envs,
			Cmd:   []string{"pbm-agent"},
		}, nil, nil, "")
		if err != nil {
			return errors.Wrap(err, "ContainerCreate")
		}

		err = cn.ContainerStart(context.Background(), containerNew.ID, types.ContainerStartOptions{})
		if err != nil {
			return errors.Wrap(err, "ContainerStart")
		}

		// statusCh, errCh := cn.ContainerWait(ctx, resp.ID, container.WaitConditionNotRunning)
		// select {
		// case err := <-errCh:
		// 	if err != nil {
		// 		return errors.Wrap(err, "container failed")
		// 	}
		// case <-statusCh:
		// }

		// cexec, err := cn.ContainerExecCreate(context.Background(), c.ID, types.ExecConfig{
		// 	Env: []string{
		// 		`LD_PRELOAD=/usr/local/lib/libfaketime.so.1`,
		// 		`FAKETIME="` + ts + `"`,
		// 	},
		// 	Cmd: []string{"pbm-agent"},
		// })
		// if err != nil {
		// 	return errors.Wrapf(err, "ContainerExecCreate in container %s", c.ID)
		// }
		// err = cn.ContainerExecStart(context.Background(), cexec.ID, types.ExecStartCheck{})
		// if err != nil {
		// 	return errors.Wrapf(err, "ContainerExecStart in container %s", c.ID)
		// }
	}

	return nil
}
