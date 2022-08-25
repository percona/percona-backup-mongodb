package pbm

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	docker "github.com/docker/docker/client"
	"github.com/pkg/errors"

	"github.com/percona/percona-backup-mongodb/pbm"
)

type Ctl struct {
	cn        *docker.Client
	ctx       context.Context
	container string
	env       []string
}

var backupNameRE = regexp.MustCompile(`Backup '([0-9\-\:TZ]+)' to remote store`)

func NewCtl(ctx context.Context, host string) (*Ctl, error) {
	cn, err := docker.NewClient(host, "1.39", nil, nil)
	if err != nil {
		return nil, errors.Wrap(err, "docker client")
	}

	return &Ctl{
		cn:        cn,
		ctx:       ctx,
		container: "pbmagent_rs101",
	}, nil
}

func (c *Ctl) PITRon() error {
	out, err := c.RunCmd("pbm", "config", "--set", "pitr.enabled=true")
	if err != nil {
		return errors.Wrap(err, "config set pitr.enabled=true")
	}

	_, err = c.RunCmd("pbm", "config", "--set", "pitr.oplogSpanMin=1")
	if err != nil {
		return errors.Wrap(err, "config set pitr.oplogSpanMin=1")
	}

	fmt.Println("done", out)
	return nil
}

func (c *Ctl) PITRoff() error {
	out, err := c.RunCmd("pbm", "config", "--set", "pitr.enabled=false")
	if err != nil {
		return err
	}

	fmt.Println("done", out)
	return nil
}

func (c *Ctl) ApplyConfig(file string) error {
	out, err := c.RunCmd("pbm", "config", "--file", file)
	if err != nil {
		return err
	}

	fmt.Println("done", out)
	return nil
}

func (c *Ctl) Resync() error {
	out, err := c.RunCmd("pbm", "config", "--force-resync")
	if err != nil {
		return err
	}

	fmt.Println("done", out)
	return nil
}

func (c *Ctl) Backup(typ pbm.BackupType) (string, error) {
	out, err := c.RunCmd("pbm", "backup", "--type", string(typ), "--compression", "s2")
	if err != nil {
		return "", err
	}

	fmt.Println("done", out)
	name := backupNameRE.FindStringSubmatch(out)
	if name == nil {
		return "", errors.Errorf("no backup name found in output:\n%s", out)
	}
	return name[1], nil
}

type ListOut struct {
	Snapshots []SnapshotStat `json:"snapshots"`
	PITR      struct {
		On       bool                   `json:"on"`
		Ranges   []PitrRange            `json:"ranges"`
		RsRanges map[string][]PitrRange `json:"rsRanges,omitempty"`
	} `json:"pitr"`
}

type SnapshotStat struct {
	Name       string     `json:"name"`
	Size       int64      `json:"size,omitempty"`
	Status     pbm.Status `json:"status"`
	Err        string     `json:"error,omitempty"`
	RestoreTS  int64      `json:"restoreTo"`
	PBMVersion string     `json:"pbmVersion"`
}

type PitrRange struct {
	Err   string       `json:"error,omitempty"`
	Range pbm.Timeline `json:"range"`
}

func (c *Ctl) List() (*ListOut, error) {
	o, err := c.RunCmd("pbm", "list", "-o", "json")
	if err != nil {
		return nil, errors.Wrap(err, "run pbm list -o json")
	}
	l := new(ListOut)
	err = json.Unmarshal(skipCtl(o), l)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal list")
	}

	return l, nil
}

func skipCtl(str string) []byte {
	for i := 0; i < len(str); i++ {
		if str[i] == '{' {
			return []byte(str[i:])
		}
	}
	return []byte(str)
}

func stripCtl(str string) []byte {
	b := make([]byte, len(str))
	var bl int
	for i := 0; i < len(str); i++ {
		c := str[i]
		if c >= 32 && c != 127 {
			b[bl] = c
			bl++
		}
	}
	return b[:bl]
}

func (c *Ctl) CheckBackup(bcpName string, waitFor time.Duration) error {
	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			sts, err := c.RunCmd("pbm", "status")
			if err != nil {
				return errors.Wrap(err, "timeout reached. pbm status")
			}
			return errors.Errorf("timeout reached. pbm status:\n%s", sts)
		case <-tkr.C:
			out, err := c.RunCmd("pbm", "list")
			if err != nil {
				return err
			}
			for _, s := range strings.Split(out, "\n") {
				s := strings.TrimSpace(s)
				if s == bcpName {
					return nil
				}
				if strings.HasPrefix(s, bcpName) {
					status := strings.TrimSpace(strings.Split(s, bcpName)[1])
					if strings.Contains(status, "Failed with") {
						return errors.New(status)
					}
				}
			}
		}
	}
}

func (c *Ctl) CheckRestore(bcpName string, waitFor time.Duration) error {
	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			list, err := c.RunCmd("pbm", "list", "--restore")
			if err != nil {
				return errors.Wrap(err, "timeout reached. get backups list")
			}
			return errors.Errorf("timeout reached. backups list:\n%s", list)
		case <-tkr.C:
			out, err := c.RunCmd("pbm", "list", "--restore")
			if err != nil {
				return err
			}
			for _, s := range strings.Split(out, "\n") {
				s := strings.TrimSpace(s)
				if s == bcpName {
					return nil
				}
				if strings.HasPrefix(s, bcpName) {
					status := strings.TrimSpace(strings.Split(s, bcpName)[1])
					if strings.Contains(status, "Failed with") {
						return errors.New(status)
					}
				}
			}
		}
	}
}

func (c *Ctl) CheckPITRestore(t time.Time, timeout time.Duration) error {
	rinlist := "PITR: " + t.Format("2006-01-02T15:04:05Z")
	return c.waitForRestore(rinlist, timeout)
}

func (c *Ctl) CheckOplogReplay(a, b time.Time, timeout time.Duration) error {
	rinlist := fmt.Sprintf("Oplog Replay: %v - %v",
		a.UTC().Format(time.RFC3339),
		b.UTC().Format(time.RFC3339))
	return c.waitForRestore(rinlist, timeout)
}

func (c *Ctl) waitForRestore(rinlist string, waitFor time.Duration) error {
	tmr := time.NewTimer(waitFor)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			list, err := c.RunCmd("pbm", "list", "--restore")
			if err != nil {
				return errors.Wrap(err, "timeout reached. get backups list")
			}
			return errors.Errorf("timeout reached. backups list:\n%s", list)
		case <-tkr.C:
			out, err := c.RunCmd("pbm", "list", "--restore")
			if err != nil {
				return err
			}
			for _, s := range strings.Split(out, "\n") {
				s := strings.TrimSpace(s)
				if s == rinlist {
					return nil
				}
				if strings.HasPrefix(s, rinlist) {
					status := strings.TrimSpace(strings.Split(s, rinlist)[1])
					if strings.Contains(status, "Failed with") {
						return errors.New(status)
					}
				}
			}
		}
	}
}

// Restore starts restore and returns the name of op
func (c *Ctl) Restore(bcpName string) (string, error) {
	o, err := c.RunCmd("pbm", "restore", bcpName, "-o", "json")
	if err != nil {
		return "", errors.Wrap(err, "run meta")
	}
	o = strings.TrimSpace(o)
	if i := strings.Index(o, "{"); i != -1 {
		o = o[i:]
	}
	m := struct {
		Name string `json:"name"`
	}{}
	err = json.Unmarshal([]byte(o), &m)
	if err != nil {
		return "", errors.Wrapf(err, "unmarshal restore meta \n%s\n", o)
	}
	return m.Name, nil
}

func (c *Ctl) ReplayOplog(a, b time.Time) error {
	_, err := c.RunCmd("pbm", "oplog-replay",
		"--start", a.Format("2006-01-02T15:04:05"),
		"--end", b.Format("2006-01-02T15:04:05"))
	return err
}

func (c *Ctl) PITRestore(t time.Time) error {
	_, err := c.RunCmd("pbm", "restore", "--time", t.Format("2006-01-02T15:04:05"))
	return err
}

func (c *Ctl) PITRestoreClusterTime(t, i uint32) error {
	_, err := c.RunCmd("pbm", "restore", "--time", fmt.Sprintf("%d,%d", t, i))
	return err
}

func (c *Ctl) RunCmd(cmds ...string) (string, error) {
	execConf := types.ExecConfig{
		Env:          c.env,
		Cmd:          cmds,
		AttachStderr: true,
		AttachStdout: true,
	}
	id, err := c.cn.ContainerExecCreate(c.ctx, c.container, execConf)
	if err != nil {
		return "", errors.Wrap(err, "ContainerExecCreate")
	}

	container, err := c.cn.ContainerExecAttach(c.ctx, id.ID, execConf)
	if err != nil {
		return "", errors.Wrap(err, "attach to failed container")
	}
	defer container.Close()

	tmr := time.NewTimer(pbm.WaitBackupStart)
	tkr := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-tmr.C:
			return "", errors.New("timeout reached")
		case <-tkr.C:
			insp, err := c.cn.ContainerExecInspect(c.ctx, id.ID)
			if err != nil {
				return "", errors.Wrap(err, "ContainerExecInspect")
			}
			if !insp.Running {
				logs, err := ioutil.ReadAll(container.Reader)
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

func (c *Ctl) ContainerLogs() (string, error) {
	r, err := c.cn.ContainerLogs(
		c.ctx, c.container,
		types.ContainerLogsOptions{
			ShowStderr: true,
		})
	if err != nil {
		return "", errors.Wrap(err, "get logs of failed container")
	}
	defer r.Close()
	logs, err := ioutil.ReadAll(r)
	if err != nil {
		return "", errors.Wrap(err, "read logs of failed container")
	}

	return string(logs), nil
}
