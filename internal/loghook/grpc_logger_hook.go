package loghook

import (
	"fmt"
	"os"
	"time"

	pb "github.com/percona/mongodb-backup/proto/messages"
	"github.com/sirupsen/logrus"
)

type GrpcLogging struct {
	clientID string
	stream   pb.Messages_LoggingClient
	level    logrus.Level
}

func NewGrpcLogging(clientID string, clientLogChan pb.Messages_LoggingClient) *GrpcLogging {
	return &GrpcLogging{
		clientID: clientID,
		stream:   clientLogChan,
		level:    logrus.ErrorLevel,
	}
}

func (hook *GrpcLogging) Fire(entry *logrus.Entry) error {
	msg := &pb.LogEntry{
		ClientID: hook.clientID,
		Level:    uint32(entry.Level),
		Ts:       time.Now().UTC().Unix(),
		Message:  entry.Message,
	}

	if err := hook.stream.Send(msg); err != nil {
		fmt.Fprintf(os.Stderr, "Unable to send log to stream: %v", err)
	}

	return nil
}

func (hook *GrpcLogging) Levels() []logrus.Level {
	levels := []logrus.Level{}

	for i := uint32(hook.level); i < uint32(logrus.DebugLevel); i++ {
		levels = append(levels, logrus.Level(i))
	}

	return levels
}

func (hook *GrpcLogging) SetLevel(level logrus.Level) {
	hook.level = level
}
