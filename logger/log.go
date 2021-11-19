package logger

import (
	"os"
	"sync"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// for sake of performance, no lock here
// should avoid calling log in package init func
var (
	log  *zap.SugaredLogger
	once sync.Once
)

var levelMap = map[string]zapcore.Level{
	"debug":  zapcore.DebugLevel,
	"info":   zapcore.InfoLevel,
	"warn":   zapcore.WarnLevel,
	"error":  zapcore.ErrorLevel,
	"dpanic": zapcore.DPanicLevel,
	"panic":  zapcore.PanicLevel,
	"fatal":  zapcore.FatalLevel,
}

func getLoggerLevel(lvl string) zapcore.Level {
	if level, ok := levelMap[lvl]; ok {
		return level
	}
	return zapcore.InfoLevel
}

func Global() *zap.SugaredLogger {
	return log
}

// Init should be called firstly in main
// Only the first Init call can init the logger, and the later calls will be ignored
func Init(fn string, level string, app string) {
	once.Do(func() { initHelper(fn, level, app) })
}

func initHelper(fn string, level string, app string) {
	// set default log file name
	if fn == "" {
		fn = "data-debug.log"
	}

	hook := &lumberjack.Logger{
		Filename:   fn,
		MaxSize:    25, // 20m
		MaxBackups: 4,
		MaxAge:     7, // 7days
		LocalTime:  true,
		Compress:   true,
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "file",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,     // UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.ShortCallerEncoder,     // 全路径编码器
		EncodeName:     zapcore.FullNameEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),                                          // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout), zapcore.AddSync(hook)), // 打印到控制台和文件
		zap.NewAtomicLevelAt(getLoggerLevel(level)),                                    // 日志级别
	)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).With(zap.String("app", app))
	log = logger.Sugar()
}

// Debug
func Debug(args ...interface{}) {
	log.Debug(args...)
}

// Debugf
func Debugf(template string, args ...interface{}) {
	log.Debugf(template, args...)
}

// Debugw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func Debugw(msg string, kvs ...interface{}) {
	log.Debugw(msg, kvs)
}

// Info
func Info(args ...interface{}) {
	log.Info(args...)
}

// Infof
func Infof(template string, args ...interface{}) {
	log.Infof(template, args...)
}

// Infow logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func Infow(msg string, kvs ...interface{}) {
	log.Infow(msg, kvs)
}

// Warn
func Warn(args ...interface{}) {
	log.Warn(args...)
}

// Warnf
func Warnf(template string, args ...interface{}) {
	log.Warnf(template, args...)
}

// Warnw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func Warnw(msg string, kvs ...interface{}) {
	log.Warnw(msg, kvs)
}

// Error
func Error(args ...interface{}) {
	log.Error(args...)
}

// Errorf
func Errorf(template string, args ...interface{}) {
	log.Errorf(template, args...)
}

// Errorw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func Errorw(msg string, kvs ...interface{}) {
	log.Errorw(msg, kvs)
}

// DPanic
func DPanic(args ...interface{}) {
	log.DPanic(args...)
}

// DPanic
func DPanicf(template string, args ...interface{}) {
	log.DPanicf(template, args...)
}

// DPanicw logs a message with some additional context. In development, the
// logger then panics. (See DPanicLevel for details.) The variadic key-value
// pairs are treated as they are in With.
func DPanicw(msg string, kvs ...interface{}) {
	log.DPanicw(msg, kvs)
}

// Panic
func Panic(args ...interface{}) {
	log.Panic(args...)
}

// Panicf
func Panicf(template string, args ...interface{}) {
	log.Panicf(template, args...)
}

// Panicw logs a message with some additional context, then panics. The
// variadic key-value pairs are treated as they are in With.
func Panicw(msg string, kvs ...interface{}) {
	log.Panicw(msg, kvs)
}

// Fatal
func Fatal(args ...interface{}) {
	log.Fatal(args...)
}

// Fatalf
func Fatalf(template string, args ...interface{}) {
	log.Fatalf(template, args...)
}

// Fatalw logs a message with some additional context, then calls os.Exit. The
// variadic key-value pairs are treated as they are in With.
func Fatalw(msg string, kvs ...interface{}) {
	log.Fatalw(msg, kvs)
}

func init() {
	initHelper("start.log", "debug", "")
}

// Sync flushes any buffered log entries.
func Sync() error {
	return log.Sync()
}

// New sugar logger
func New(app string, level zapcore.Level) *zap.SugaredLogger {
	if level < zap.DebugLevel || level > zap.FatalLevel {
		level = zap.ErrorLevel
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "file",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,  // 小写编码器
		EncodeTime:     zapcore.ISO8601TimeEncoder,     // UTC 时间格式
		EncodeDuration: zapcore.SecondsDurationEncoder, //
		EncodeCaller:   zapcore.ShortCallerEncoder,     // 全路径编码器
		EncodeName:     zapcore.FullNameEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),                   // 编码器配置
		zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), // 打印到控制台
		zap.NewAtomicLevelAt(level),                             // 日志级别
	)

	l := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(0)).With(zap.String("app", app))
	return l.Sugar()
}
