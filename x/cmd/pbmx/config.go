package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/percona/percona-backup-mongodb/x/pbm/apiclient"
	"github.com/percona/percona-backup-mongodb/x/pbm/config"
)

func configCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config [config-name]",
		Short: "Set, change or list the config",
		Args:  cobra.MaximumNArgs(1),
		RunE:  runConfig,
	}

	cmd.Flags().Bool("list", false, "List all configurations")
	cmd.Flags().String("file", "", "Upload config from YAML file")
	cmd.Flags().Bool("force-resync", false, "Resync backup list with the current store")
	cmd.MarkFlagsMutuallyExclusive("list", "file")
	cmd.MarkFlagsMutuallyExclusive("list", "force-resync")
	cmd.MarkFlagsMutuallyExclusive("file", "force-resync")

	return cmd
}

func runConfig(cmd *cobra.Command, args []string) error {
	list, _ := cmd.Flags().GetBool("list")
	file, _ := cmd.Flags().GetString("file")
	forceResync, _ := cmd.Flags().GetBool("force-resync")

	cli := apiclient.New(splitList(viper.GetString(apiEndpointsFlag)))
	out := cmd.OutOrStdout()
	ctx := cmd.Context()

	switch {
	case file != "":
		if len(args) > 0 {
			return errors.New("config name argument is not allowed with --file")
		}
		return upsertConfig(ctx, cli, out, file)

	case list:
		if len(args) > 0 {
			return errors.New("config name argument is not allowed with --list")
		}
		cfgs, err := cli.ListConfigs(ctx)
		if err != nil {
			return err
		}
		return printConfigs(out, cfgs)

	case forceResync:
		name := config.DefaultConfigName
		if len(args) > 0 {
			name = args[0]
		}
		if err := cli.ResyncConfig(ctx, name); err != nil {
			if errors.Is(err, apiclient.ErrNotFound) {
				return fmt.Errorf("configuration %q not found", name)
			}
			return err
		}
		fmt.Fprintf(out, "backup list resync started for configuration %q\n", name)
		return nil

	default:
		name := config.DefaultConfigName
		if len(args) > 0 {
			name = args[0]
		}
		cfg, err := cli.GetConfig(ctx, name)
		if err != nil {
			if errors.Is(err, apiclient.ErrNotFound) {
				return fmt.Errorf("configuration %q not found", name)
			}
			return err
		}
		fmt.Fprint(out, cfg.String())
		return nil
	}
}

// upsertConfig reads a YAML config from file ("-" for stdin) and saves it via
// the PUT /config endpoint.
func upsertConfig(ctx context.Context, cli *apiclient.Client, out io.Writer, file string) error {
	var (
		cfg *config.Config
		err error
	)
	if file == "-" {
		cfg, err = config.Parse(os.Stdin)
	} else {
		cfg, err = readConfigFromFile(file)
	}
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	if err := cli.SetConfig(ctx, cfg); err != nil {
		return err
	}

	name := cfg.Name
	if name == "" {
		name = config.DefaultConfigName
	}
	fmt.Fprintf(out, "configuration %q saved\n", name)
	return nil
}

func readConfigFromFile(filename string) (*config.Config, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", filename, err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("close %q: %v", filename, err)
		}
	}()

	return config.Parse(f)
}

// printConfigs renders configs as YAML documents separated by "---".
func printConfigs(out io.Writer, cfgs []*config.Config) error {
	for i, cfg := range cfgs {
		if i > 0 {
			fmt.Fprintln(out, "---")
		}
		fmt.Fprint(out, cfg.String())
	}
	return nil
}
