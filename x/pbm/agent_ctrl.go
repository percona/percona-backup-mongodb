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
	lg = lg.With(zap.String("agent", cfg.Name))

	nodeConn, err := connect.ConnectDirect(ctx, cfg.MongoURI)
	if err != nil {
		return fmt.Errorf("connect local mongod: %w", err)
	}
	defer connect.Disconnect(lg, nodeConn)

	leadConn, err := connect.Connect(ctx, cfg.MongoURI, "pbmx-agent")
	if err != nil {
		return fmt.Errorf("connect cluster leader: %w", err)
	}
	defer leadConn.Disconnect(ctx)

	statusSvc := status.NewForCtrlAgent(lg, cfg.Name, nodeConn, cfg.APISrvPort)

	d, err := disco.Start(ctx, lg, serfLg, cfg.Name, cfg.WorkerAgentConfig.Config, statusSvc.DiscoSync())
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
	}
	defer func() {
		if err := d.Stop(); err != nil {
			lg.Warn("serf shutdown", zap.Error(err))
		}
	}()
	lg.Info("added to PBM cluster")

	etcdSrv, err := etcd.Start(ctx, etcdLg, cfg.Name, cfg.Config)
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
		nodeConn,
		leadConn,
		statusSvc,
		configSvc,
		cfg.Name,
		task.NewComposer(lg, etcdSrv.Client(), task.NewScheduler(statusSvc, configSvc)),
	)

	inbox := task.NewInbox(lg, etcdSrv.Client(), cfg.Name, physSvc)
	go func() {
		lg.Info("run inbox")
		if err := inbox.Run(ctx); err != nil {
			lg.Info("task inbox stopped", zap.Error(err))
		}
	}()

	// wire the status service before starting its loop
	statusSvc.SetPublisher(d)
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
