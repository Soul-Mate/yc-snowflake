package logger

import (
	"log"
	"os"
)

const (
	LDebug = iota
	LInfo
	LWarn
	LErr
)

const (
	LOG_PREFIX_DEBUG   = "Debug:"
	LOG_PREFIX_INFO    = "Info:"
	LOG_PREFIX_WARNING = "Warning:"
	LOG_PREFIX_ERROR   = "Error:"
)

type WrapLog struct {
	path   string
	writer *os.File
}

func NewLogger(path string) (*WrapLog, error) {
	l := new(WrapLog)
	if path == "" {
		l.writer = os.Stdout
		return l, nil
	}

	l.path = path
	_, err := os.Stat(l.path);
	if err == nil {
		f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, err
		}
		l.writer = f
		return l, nil
	} else {
		if os.IsNotExist(err) {
			if fd, err := os.Create(l.path); err != nil {
				return nil, err
			} else {
				l.writer = fd
				return l, nil
			}
		} else if os.IsExist(err) {
			f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0644)
			if err != nil {
				return nil, err
			}
			l.writer = f
			return l, nil
		} else {
			return nil, err
		}
	}
}

func (l *WrapLog) Printf(level int, format string, v ...interface{}) {
	l.createLevelLog(level).Printf(format, v...)
}

func (l *WrapLog) Print(level int, v ...interface{}) {
	l.createLevelLog(level).Print(v...)
}

func (l *WrapLog) Debug(format string, v ...interface{}) {
	l.createLevelLog(LDebug).Printf(format, v...)
}

func (l *WrapLog) Info(format string, v ...interface{}) {
	l.createLevelLog(LInfo).Printf(format, v...)
}

func (l *WrapLog) Warning(format string, v ...interface{}) {
	l.createLevelLog(LWarn).Printf(format, v...)
}

func (l *WrapLog) Error(format string, v ...interface{}) {
	l.createLevelLog(LErr).Printf(format, v...)
}

func (l *WrapLog) createLevelLog(level int) *log.Logger {
	flag := log.Ldate | log.Ltime | log.Lshortfile
	prefix := ""
	switch level {
	case LDebug:
		prefix = LOG_PREFIX_DEBUG
		return log.New(l.writer, LOG_PREFIX_DEBUG, flag)
	case LInfo:
		prefix = LOG_PREFIX_INFO
	case LWarn:
		prefix = LOG_PREFIX_WARNING
	case LErr:
		prefix = LOG_PREFIX_ERROR
	default:
		prefix = "yc-snowflake"
	}
	return log.New(l.writer, prefix, flag)
}
