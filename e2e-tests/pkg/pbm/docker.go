package pbm

import (
	"context"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

type Docker struct {
	cn  *client.Client
	ctx context.Context
}

func NewDocker(ctx context.Context, host string) (*Docker, error) {
	cn, err := client.New(client.WithHost(host))
	if err != nil {
		return nil, errors.Wrap(err, "docker client")
	}

	return &Docker{
		cn:  cn,
		ctx: ctx,
	}, nil
}

// StopContainers stops containers with the given labels
func (d *Docker) StopContainers(labels []string) error {
	fltr := make(client.Filters)
	for _, v := range labels {
		fltr.Add("label", v)
	}
	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}

	for _, c := range containers.Items {
		log.Println("stopping container", c.ID)
		_, err = d.cn.ContainerStop(d.ctx, c.ID, client.ContainerStopOptions{})
		if err != nil {
			return errors.Wrapf(err, "stop container %s", c.ID)
		}
	}

	return nil
}

// StopAgents stops agent containers of the given replicaset
func (d *Docker) StopAgents(rsName string) error {
	return d.StopContainers([]string{"com.percona.pbm.agent.rs=" + rsName})
}

// PauseAgents pause agent containers of the given replicaset
func (d *Docker) PauseAgents(rsName string) error {
	fltr := make(client.Filters)
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers.Items) == 0 {
		return errors.Errorf("no containers found for replset %s", rsName)
	}

	for _, c := range containers.Items {
		log.Println("stopping container", c.ID)
		_, err = d.cn.ContainerPause(d.ctx, c.ID, client.ContainerPauseOptions{})
		if err != nil {
			return errors.Wrapf(err, "stop container %s", c.ID)
		}
	}

	return nil
}

// UnpauseAgents unpause agent containers of the given replicaset
func (d *Docker) UnpauseAgents(rsName string) error {
	fltr := make(client.Filters)
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers.Items) == 0 {
		return errors.Errorf("no containers found for replset %s", rsName)
	}

	for _, c := range containers.Items {
		log.Println("stopping container", c.ID)
		_, err = d.cn.ContainerUnpause(d.ctx, c.ID, client.ContainerUnpauseOptions{})
		if err != nil {
			return errors.Wrapf(err, "stop container %s", c.ID)
		}
	}

	return nil
}

// StartAgents starts stopped agent containers of the given replicaset
func (d *Docker) StartAgents(rsName string) error {
	return d.StartContainers([]string{"com.percona.pbm.agent.rs=" + rsName})
}

// StartAgents starts stopped agent containers of the given replicaset
func (d *Docker) StartContainers(labels []string) error {
	fltr := make(client.Filters)
	for _, v := range labels {
		fltr.Add("label", v)
	}
	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		All:     true,
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers.Items) == 0 {
		return errors.Errorf("no containers found for lables %v", labels)
	}

	for _, c := range containers.Items {
		log.Println("Straing container", c.ID)
		_, err = d.cn.ContainerStart(d.ctx, c.ID, client.ContainerStartOptions{})
		if err != nil {
			return errors.Wrapf(err, "start container %s", c.ID)
		}
	}

	return nil
}

// RestartAgents restarts agent containers of the given replicaset
func (d *Docker) RestartAgents(rsName string) error {
	return d.RestartContainers([]string{"com.percona.pbm.agent.rs=" + rsName})
}

// RestartAgents restarts agent containers of the given replicaset
func (d *Docker) RestartContainers(labels []string) error {
	fltr := make(client.Filters)
	for _, v := range labels {
		fltr.Add("label", v)
	}
	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}

	for _, c := range containers.Items {
		log.Println("restarting container", c.ID)
		_, err = d.cn.ContainerRestart(d.ctx, c.ID, client.ContainerRestartOptions{})
		if err != nil {
			return errors.Wrapf(err, "restart container %s", c.ID)
		}
	}

	return nil
}

func (d *Docker) RunOnReplSet(rsName string, wait time.Duration, cmd ...string) error {
	fltr := make(client.Filters)
	fltr.Add("label", "com.percona.pbm.agent.rs="+rsName)
	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers.Items) == 0 {
		return errors.Errorf("no containers found for replset %s", rsName)
	}

	var wg sync.WaitGroup
	for _, c := range containers.Items {
		log.Printf("run %v on conainer %s%v\n", cmd, c.ID, c.Names)
		wg.Add(1)
		go func(cont container.Summary) {
			out, err := d.RunCmd(cont.ID, wait, cmd...)
			if err != nil {
				log.Fatalf("ERROR: run cmd %v on container %s%v: %v", cmd, cont.ID, cont.Names, err)
			}
			if out != "" {
				log.Println(out)
			}
			wg.Done()
		}(c)
	}

	wg.Wait()
	return nil
}

func (d *Docker) RunCmd(containerID string, wait time.Duration, cmd ...string) (string, error) {
	execConf := client.ExecCreateOptions{
		User:         "root",
		Cmd:          cmd,
		Privileged:   true,
		AttachStderr: true,
		AttachStdout: true,
	}
	id, err := d.cn.ExecCreate(d.ctx, containerID, execConf)
	if err != nil {
		return "", errors.Wrap(err, "ContainerExecCreate")
	}

	attach, err := d.cn.ExecAttach(d.ctx, id.ID, client.ExecAttachOptions{})
	if err != nil {
		return "", errors.Wrap(err, "attach to failed container")
	}
	defer attach.Close()

	tmr := time.NewTimer(wait)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			return "", errors.New("timeout reached")
		case <-tkr.C:
			insp, err := d.cn.ExecInspect(d.ctx, id.ID, client.ExecInspectOptions{})
			if err != nil {
				return "", errors.Wrap(err, "ContainerExecInspect")
			}
			if !insp.Running {
				logs, err := io.ReadAll(attach.Reader)
				if err != nil {
					return "", errors.Wrap(err, "read logs of failed container")
				}

				switch insp.ExitCode {
				case 0:
					return string(logs), nil
				default:
					return "", errors.Errorf("container exited with %d code. Logs: %s", insp.ExitCode, logs)
				}
			}
		}
	}
}

func (d *Docker) StartAgentContainers(labels []string) error {
	fltr := make(client.Filters)
	for _, v := range labels {
		fltr.Add("label", v)
	}

	containers, err := d.cn.ContainerList(d.ctx, client.ContainerListOptions{
		All:     true,
		Filters: fltr,
	})
	if err != nil {
		return errors.Wrap(err, "container list")
	}
	if len(containers.Items) == 0 {
		return errors.Errorf("no containers found for labels %v", labels)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(containers.Items))

	for _, c := range containers.Items {
		wg.Add(1)
		go func(cont container.Summary) {
			defer wg.Done()

			var buf strings.Builder
			var started bool
			for i := 1; i <= 5; i++ {
				_, err := d.cn.ContainerStart(d.ctx, cont.ID, client.ContainerStartOptions{})
				if err != nil {
					errCh <- errors.Wrapf(err, "start container %s", cont.ID)
					return
				}

				since := time.Now().Format(time.RFC3339Nano)
				time.Sleep(5 * time.Second)
				out, err := d.cn.ContainerLogs(d.ctx, cont.ID, client.ContainerLogsOptions{
					ShowStdout: true,
					ShowStderr: true,
					Follow:     false,
					Since:      since,
				})
				if err != nil {
					errCh <- errors.Wrapf(err, "get logs for container %s", cont.ID)
					return
				}

				buf.Reset()
				_, err = io.Copy(&buf, out)
				if err != nil {
					errCh <- errors.Wrapf(err, "read logs for container %s", cont.ID)
					return
				}

				if strings.Contains(buf.String(), "listening for the commands") {
					log.Printf("PBM agent %s started properly \n", cont.ID)
					started = true
					break
				}

				_, err = d.cn.ContainerStop(d.ctx, cont.ID, client.ContainerStopOptions{})
				if err != nil {
					errCh <- errors.Wrapf(err, "stop container %s", cont.ID)
					return
				}

				log.Printf("PBM agent %s wasn't started, retrying in %d seconds\n", cont.ID, i*5)
				time.Sleep(time.Duration(i*5) * time.Second)
			}

			if !started {
				errCh <- errors.Errorf("Can't start container %s, last logs: %s", cont.ID, buf.String())
			}
		}(c)
	}

	wg.Wait()
	close(errCh)

	errs := []error{}
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return errors.Errorf("Can't start PBM agents:\n%s", errs)
	}

	return nil
}
