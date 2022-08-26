package utils

import (
	"io"
	"os"
	"path"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func GetLogger(dir string) *zap.Logger {

	file, err := os.OpenFile(path.Join(dir, "log.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		zap.L().Sugar().Error("打开日志文件失败", err)
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	// encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	// encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.ConsoleSeparator = " | "

	encoder := zapcore.NewConsoleEncoder(encoderConfig)
	writer := zapcore.AddSync(io.MultiWriter(file, os.Stdout))

	core := zapcore.NewCore(encoder, writer, zapcore.DebugLevel)
	return zap.New(core, zap.AddCaller())
}
