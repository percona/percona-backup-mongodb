package kinit

import (
	"context"
	"net/url"
	"os/exec"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
	"github.com/percona/percona-backup-mongodb/pbm/log"
)

// Renewal periodically refreshes a Kerberos TGT using the `kinit` command-line tool.
type Renewal struct {
	Keytab    string
	Principal string
	Interval  time.Duration
}

func New(mongoURI, keytab string, interval time.Duration) (*Renewal, error) {
	if _, err := exec.LookPath("kinit"); err != nil {
		return nil, errors.New("kinit not found in PATH")
	}

	principal, err := principalFromMongoURI(mongoURI)
	if err != nil {
		return nil, errors.Wrap(err, "parsing principal")
	}

	return &Renewal{
		Keytab:    keytab,
		Principal: principal,
		Interval:  interval,
	}, nil
}

// Start performs an immediate renewal and then continues to renew the ticket every Interval.
func (r Renewal) Start(ctx context.Context) {
	l := log.FromContext(ctx).NewEvent("", "", "", primitive.Timestamp{})

	if err := r.renew(ctx); err != nil {
		l.Error("Kerberos ticket renewal failed: %v", err)
	} else {
		l.Info("Kerberos ticket obtained successfully")
	}

	tkr := time.NewTicker(r.Interval)
	go func() {
		defer tkr.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tkr.C:
				if err := r.renew(ctx); err != nil {
					l.Error("Kerberos ticket renewal failed: %v", err)
				} else {
					l.Info("Kerberos ticket obtained successfully")
				}
			}
		}
	}()
}

func (r Renewal) renew(ctx context.Context) error {
	args := []string{"-k"}
	if r.Keytab != "" {
		args = append(args, "-t", r.Keytab)
	}
	args = append(args, r.Principal)

	cmd := exec.CommandContext(ctx, "kinit", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		msg := strings.TrimSpace(string(out))
		return errors.New(msg)
	}
	return nil
}

func principalFromMongoURI(uri string) (string, error) {
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
