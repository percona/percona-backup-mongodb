package pbm

import (
	"context"
	"log"
	"strings"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

func ClockSkew(rsName, ts, dockerHost string) error {
	if ts != "0" {
		log.Printf("==Skew the clock for %s on the replicaset %s ", ts, rsName)
	}

	cn, err := client.New(client.WithHost(dockerHost))
	if err != nil {
		return errors.Wrap(err, "docker client")
	}
	defer cn.Close()

	fltr := make(client.Filters)
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := cn.ContainerList(context.Background(), client.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}

	for _, c := range containers.Items {
		inspect, err := cn.ContainerInspect(context.Background(), c.ID, client.ContainerInspectOptions{})
		if err != nil {
			return errors.Wrapf(err, "ContainerInspect for %s", c.ID)
		}
		containerOld := inspect.Container

		envs := append([]string{}, containerOld.Config.Env...)
		if ts == "0" {
			ldPreloadDefined := false
			for _, env := range envs {
				if strings.HasPrefix(env, "LD_PRELOAD") {
					ldPreloadDefined = true
					break
				}
			}

			if !ldPreloadDefined {
				log.Printf("Variable LD_PRELOAD isn't defined, skipping container restart for %s\n", containerOld.Name)
				continue
			}

			var filteredEnvs []string
			for _, env := range envs {
				if !strings.HasPrefix(env, "LD_PRELOAD") {
					filteredEnvs = append(filteredEnvs, env)
				}
			}
			envs = filteredEnvs
		} else {
			envs = append(envs,
				`LD_PRELOAD=/lib64/faketime/libfaketime.so.1`,
				`FAKETIME=`+ts,
			)
		}

		log.Printf("Removing container %s/%s\n", containerOld.ID, containerOld.Name)
		_, err = cn.ContainerRemove(context.Background(), c.ID, client.ContainerRemoveOptions{Force: true})
		if err != nil {
			return errors.Wrapf(err, "remove container %s", c.ID)
		}

		log.Printf("Creating container %s/%s with the clock skew %s\n", containerOld.ID, containerOld.Name, ts)
		containerNew, err := cn.ContainerCreate(context.Background(), client.ContainerCreateOptions{
			Config: &container.Config{
				Image:  containerOld.Image,
				Env:    envs,
				Cmd:    []string{"pbm-agent"},
				Labels: containerOld.Config.Labels,
				User:   "1001",
			},
			HostConfig: containerOld.HostConfig,
			NetworkingConfig: &network.NetworkingConfig{
				EndpointsConfig: containerOld.NetworkSettings.Networks,
			},
			Name: containerOld.Name,
		})
		if err != nil {
			return errors.Wrap(err, "ContainerCreate")
		}

		_, err = cn.ContainerStart(context.Background(), containerNew.ID, client.ContainerStartOptions{})
		if err != nil {
			return errors.Wrap(err, "ContainerStart")
		}
		log.Printf("New container %s/%s has started\n", containerOld.ID, containerOld.Name)
	}

	return nil
}
