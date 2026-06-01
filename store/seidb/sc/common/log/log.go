package log

import (
	"fmt"
	"strings"

	"cosmossdk.io/log"
)

// Logger is a lightweight shim compatible with seilog usage in memiavl.
type Logger interface {
	Info(msg string, keyvals ...interface{})
	Debug(msg string, keyvals ...interface{})
	Error(msg string, keyvals ...interface{})
}

type moduleLogger struct {
	logger log.Logger
}

func NewLogger(_ ...string) Logger {
	return moduleLogger{logger: log.NewNopLogger()}
}

func (l moduleLogger) Info(msg string, keyvals ...interface{}) {
	l.logger.Info(msg, keyvals...)
}

func (l moduleLogger) Debug(msg string, keyvals ...interface{}) {
	l.logger.Debug(msg, keyvals...)
}

func (l moduleLogger) Error(msg string, keyvals ...interface{}) {
	l.logger.Error(msg, keyvals...)
}

// SetDefaultLogger wires a real logger for memiavl diagnostics.
func SetDefaultLogger(logger log.Logger) {
	if logger == nil {
		return
	}
	defaultLogger = moduleLogger{logger: logger.With("module", "seidb", "sc", "memiavl")}
}

var defaultLogger Logger = NewLogger()

func Default() Logger {
	return defaultLogger
}

func FormatKeyvals(keyvals ...interface{}) string {
	if len(keyvals) == 0 {
		return ""
	}
	var b strings.Builder
	for i := 0; i < len(keyvals); i += 2 {
		if i > 0 {
			b.WriteString(" ")
		}
		if i+1 < len(keyvals) {
			fmt.Fprintf(&b, "%v=%v", keyvals[i], keyvals[i+1])
		} else {
			fmt.Fprintf(&b, "%v", keyvals[i])
		}
	}
	return b.String()
}
