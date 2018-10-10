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
	line, err := entry.String()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to read entry, %v", err)
		return err
	}

	msg := &pb.LogEntry{
		ClientID: hook.clientID,
		Level:    uint32(entry.Level),
		Ts:       time.Now().UTC().Unix(),
		Message:  line,
	}

	hook.stream.Send(msg)

	return nil
}

func (hook *GrpcLogging) Levels() []logrus.Level {
	levels := []logrus.Level{}

	for i := uint32(hook.level); i < uint32(logrus.PanicLevel); i++ {
		levels = append(levels, logrus.Level(i))
	}

	return levels
}

func (hook *GrpcLogging) SetLevel(level logrus.Level) {
	hook.level = level
}
