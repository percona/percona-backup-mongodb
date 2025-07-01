package kinit

import (
	"context"
	"net/url"
	"os/exec"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
)

type RenewalManager struct {
	Keytab    string
	Principal string
	Interval  time.Duration
}

func New(mongoURI, keytab string, interval time.Duration) (*RenewalManager, error) {
	if _, err := exec.LookPath("kinit"); err != nil {
		return nil, errors.New("kinit not found in PATH")
	}

	principal, err := principalFromMongoUri(mongoURI)
	if err != nil {
		return nil, errors.Wrap(err, "parsing principal")
	}

	return &RenewalManager{
		Keytab:    keytab,
		Principal: principal,
		Interval:  interval,
	}, nil
}

func (rm RenewalManager) Start(ctx context.Context) {
	l := log.FromContext(ctx).NewEvent("", "", "", primitive.Timestamp{})
	
	if err := rm.renew(ctx); err != nil {
		l.Error("Kerberos ticket renewal failed: %v", err)
		// TODO: determine if it should exit
	} else {
		l.Info("Kerberos ticket obtained successfully")
	}

	tmr := time.NewTimer(rm.Interval)
	go func() {
		defer tmr.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tmr.C:
				if err := rm.renew(ctx); err != nil {
					l.Error("Kerberos ticket renewal failed: %v", err)
				} else {
					l.Info("Kerberos ticket obtained successfully")
				}
			}
		}
	}()
}

func (rm RenewalManager) renew(ctx context.Context) error {
	args := []string{"-k"}
	if rm.Keytab != "" {
		args = append(args, "-t", rm.Keytab)
	}
	args = append(args, rm.Principal)

	cmd := exec.CommandContext(ctx, "kinit", args...)
	if _, err := cmd.CombinedOutput(); err != nil {
		return errors.Wrap(err, "kinit failed")
	}
	return nil
}

func principalFromMongoUri(uri string) (string, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", errors.Wrap(err, "cannot parse URI")
	}

	if parsed.User == nil {
		return "", errors.New("no user info in URI")
	}

	principal := parsed.User.Username()
	if principal == "" {
		return "", errors.New("empty principal in URI")
	}

	return principal, nil
}
