package main

import (
	"os"
	"os/exec"
)

const (
	envSidecar         = "PBM_AGENT_SIDECAR"
	envSidecarSleepSec = "PBM_AGENT_SIDECAR_SLEEP"

	agentCmd = "pbm-agent"
)

func main() {
	for {
		cmd := exec.Command(agentCmd, os.Args...)
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		cmd.Run()
	}
}
