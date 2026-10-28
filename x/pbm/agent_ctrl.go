package pbm

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/percona/percona-backup-mongodb/x/pbm/api"
	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/connect"
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
	"github.com/percona/percona-backup-mongodb/x/pbm/task"
)

// RunCtrlAgent starts the control agent: it joins the PBM discovery cluster,
// brings up embedded etcd, and blocks until ctx is canceled or the server
// reports a fatal error.
func RunCtrlAgent(
	ctx context.Context,
	lg, etcdLg, serfLg *zap.Logger,
	cfg *CtrlAgentConfig,
) error {
	ac, err := connect.ConnectAgent(ctx, lg, cfg.MongoURI, cfg.Name)
	if err != nil {
		return fmt.Errorf("resolve agent: %w", err)
	}
	defer ac.Disconnect(context.Background(), lg)

	lg = lg.With(zap.String("agent", ac.Name))

	statusSvc := status.NewForCtrlAgent(lg, ac.Name, ac.NodeConn, cfg.APISrvPort)

	d, err := disco.Start(ctx, lg, serfLg, ac.Name, cfg.WorkerAgentConfig.Config, statusSvc.DiscoSync())
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
	}
	defer func() {
		if err := d.Stop(); err != nil {
			lg.Warn("serf shutdown", zap.Error(err))
		}
	}()
	lg.Info("added to PBM cluster")

	etcdSrv, err := etcd.Start(ctx, etcdLg, ac.Name, cfg.Config)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return fmt.Errorf("start etcd: %w", err)
	}
	defer etcdSrv.Close()
	lg.Info("started control collection db")

	backupRepo := backup.New(etcdSrv.Client())
	storageResyncer := backup.NewStorageResyncer(lg, backupRepo)
	configSvc := config.New(etcdSrv.Client(), storageResyncer)
	physSvc := backup.NewPhysSvc(
		lg,
		etcdSrv.Client(),
		backupRepo,
		ac.NodeConn,
		ac.LeadConn,
		statusSvc,
		configSvc,
		ac.Name,
		task.NewComposer(lg, etcdSrv.Client(), task.NewScheduler(statusSvc, configSvc)),
	)

	inbox := task.NewInbox(lg, etcdSrv.Client(), ac.Name, physSvc)
	go func() {
		lg.Info("run inbox")
		if err := inbox.Run(ctx); err != nil {
			lg.Info("task inbox stopped", zap.Error(err))
		}
	}()

	// wire the status service before starting its loop
	statusSvc.SetPublisher(d)
	statusSvc.SetRepo(status.NewRepo(etcdSrv.Client()))
	statusSvc.SetLeaderChecker(etcdSrv)
	go statusSvc.Run(ctx)

	apiSrv := api.Start(
		api.Config{Port: cfg.APISrvPort},
		api.NewRouter(lg, statusSvc, configSvc, backupRepo, physSvc),
	)
	lg.Info("started REST API", zap.Int("port", cfg.APISrvPort))

	select {
	case <-ctx.Done():
		lg.Info("agent is shutdown")
		return nil
	case err := <-etcdSrv.Err():
		return fmt.Errorf("embedded etcd stopped: %w", err)
	case err := <-apiSrv.Err():
		return fmt.Errorf("api server stopped: %w", err)
	}
}
