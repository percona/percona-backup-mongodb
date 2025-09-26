package util

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/percona/percona-backup-mongodb/pbm/errors"
)

func AskConfirmation(question string) error {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return errors.Wrap(err, "stat stdin")
	}
	if (fi.Mode() & os.ModeCharDevice) == 0 {
		return errors.New("no tty")
	}

	if runtime.GOOS == "linux" {
		question = fmt.Sprintf("\033[1;33m%s\033[0m", question) // Yellow text for Linux terminals
	}
	fmt.Printf("%s [y/N] ", question)

	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "read stdin")
	}

	switch strings.TrimSpace(scanner.Text()) {
	case "yes", "Yes", "YES", "Y", "y":
		return nil
	}

	return errors.ErrUserCanceled
}
