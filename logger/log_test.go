package logger

import (
	"testing"
	"whgo/data/octopus/whtesting"

	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestNew(t *testing.T) {
	l := New("test", zapcore.InfoLevel)
	assert.NotNil(t, l)
	l.Info("test info logging")

	l = New("test", -1)
	assert.NotNil(t, l)
	l.Error("test error logging")
}

func TestSetLevel(t *testing.T) {
	chlogger := NewChangeable("changeable_test", zapcore.InfoLevel)
	chlogger.Info("This is info log")
	chlogger.level.SetLevel(zapcore.ErrorLevel)
	nonexist := "This won't be logged"
	chlogger.Info(nonexist)
	existerror := "this is error log"
	chlogger.Error(existerror)
	checker, err := whtesting.NewChecker("changeable_test.log")
	if err != nil {
		return
	}
	// scan完文件之后，删掉log文件
	// TODO 应该在test的teardown做
	defer checker.Clean()
	// 检查改变级别之后，info级别的log是否没有打印出来
	if checker.LineExist(nonexist) {
		t.Fail()
	}
}

func TestChangeableLoggerCallback(t *testing.T) {
	chlogger := NewChangeable("log_test", zapcore.InfoLevel)
	chlogger.SetLevelFromString("warn")
	if chlogger.level.Level() != zap.NewAtomicLevelAt(zapcore.WarnLevel).Level() {
		t.Fail()
	}
}
