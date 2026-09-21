package pbm

import (
	"context"
	"fmt"
	"log"

	"github.com/percona/percona-backup-mongodb/x/pbm/backup"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
	"github.com/percona/percona-backup-mongodb/x/pbm/connect"
	"github.com/percona/percona-backup-mongodb/x/pbm/disco"
	"github.com/percona/percona-backup-mongodb/x/pbm/etcd"
	"github.com/percona/percona-backup-mongodb/x/pbm/status"
	"github.com/percona/percona-backup-mongodb/x/pbm/task"
)

// RunWorkerAgent starts the worker agent: it performs backup/restore work.
func RunWorkerAgent(ctx context.Context, cfg *WorkerAgentConfig) error {
	nodeConn, err := connect.ConnectDirect(ctx, cfg.MongoURI)
	if err != nil {
		return fmt.Errorf("connect local mongod: %w", err)
	}
	defer connect.Disconnect(nodeConn)

	leadConn, err := connect.Connect(ctx, cfg.MongoURI, "pbmx-agent")
	if err != nil {
		return fmt.Errorf("connect cluster leader: %w", err)
	}
	defer leadConn.Disconnect(ctx)

	statusSvc := status.NewForWorkerAgent(cfg.Name, nodeConn)

	d, err := disco.Start(ctx, cfg.Name, cfg.Config, statusSvc.DiscoSync())
	if err != nil {
		return fmt.Errorf("start pbm cluster: %w", err)
	}
	defer func() {
		if err := d.Stop(); err != nil {
			log.Printf("serf shutdown: %v", err)
		}
	}()
	log.Printf("agent: %s added to PBM cluster", cfg.Name)

	statusSvc.SetPublisher(d)
	go statusSvc.Run(ctx)

	ccDB, err := etcd.NewClient(cfg.EtcdEndpoints)
	if err != nil {
		return fmt.Errorf("connect control state: %w", err)
	}
	defer ccDB.Close()

	backupRepo := backup.New(ccDB)
	configSvc := config.New(ccDB, backup.NewStorageResyncer(backupRepo))
	physSvc := backup.NewPhysSvc(
		ccDB,
		backupRepo,
		nodeConn,
		leadConn,
		statusSvc,
		configSvc,
		cfg.Name,
		task.NewComposer(ccDB),
	)
	inbox := task.NewInbox(ccDB, cfg.Name, physSvc)
	go func() {
		log.Printf("run inbox for agent: %s", cfg.Name)
		if err := inbox.Run(ctx); err != nil {
			log.Printf("agent: task inbox: %v", err)
		}
	}()

	<-ctx.Done()

	log.Printf("agent: %s is shutdown", cfg.Name)
	return nil
}
