package pbm

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/connect"
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/errors"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
	"github.com/percona/percona-backup-mongodb/x/pbm/task"
)

// RunWorkerAgent starts the worker agent: it performs backup/restore work.
func RunWorkerAgent(
	ctx context.Context,
	lg, etcdLg, serfLg *zap.Logger,
	cfg *WorkerAgentConfig,
) error {
	if cfg.MongoURI == "" {
		return errors.New("worker agent requires MongoDB connection string")
	}

	ac, err := connect.ConnectAgent(ctx, lg, cfg.MongoURI, cfg.Name)
	if err != nil {
		return fmt.Errorf("resolve agent: %w", err)
	}
	defer ac.Disconnect(context.Background(), lg)

	lg = lg.With(zap.String("agent", ac.Name))

	statusSvc := status.NewForWorkerAgent(lg, ac.Name, ac.NodeConn)

	d, err := disco.Start(ctx, lg, serfLg, ac.Name, cfg.Config, statusSvc.DiscoSync())
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
	}
	defer func() {
		if err := d.Stop(); err != nil {
			lg.Warn("serf shutdown", zap.Error(err))
		}
	}()
	lg.Info("added to PBM cluster")

	ccDB, err := etcd.NewClient(etcdLg, cfg.EtcdEndpoints)
	if err != nil {
		return fmt.Errorf("connect control state: %w", err)
	}
	defer ccDB.Close()

	statusSvc.SetPublisher(d)
	statusSvc.SetRepo(status.NewRepo(ccDB))
	go statusSvc.Run(ctx)

	backupRepo := backup.New(ccDB)
	configSvc := config.New(ccDB, backup.NewStorageResyncer(lg, backupRepo))
	physSvc := backup.NewPhysSvc(
		lg,
		ccDB,
		backupRepo,
		ac.NodeConn,
		ac.LeadConn,
		statusSvc,
		configSvc,
		ac.Name,
		task.NewComposer(lg, ccDB, task.NewScheduler(statusSvc, configSvc)),
	)
	inbox := task.NewInbox(lg, ccDB, ac.Name, physSvc)
	go func() {
		lg.Info("run inbox")
		if err := inbox.Run(ctx); err != nil {
			lg.Info("task inbox stopped", zap.Error(err))
		}
	}()

	<-ctx.Done()

	lg.Info("agent is shutdown")
	return nil
}
