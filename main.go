package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/hsmtkk/azure-blob-upload/upload"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var command = &cobra.Command{
	Use:  "azure-blob-upload srcDirectory container",
	Run:  run,
	Args: cobra.ExactArgs(2),
}

func init() {
}

func main() {
	if err := command.Execute(); err != nil {
		log.Fatal(err)
	}
}

func run(cmd *cobra.Command, args []string) {
	srcDirectory := args[0]
	container := args[1]

	accountName := requiredEnv("ACCOUNT_NAME")
	accountKey := requiredEnv("ACCOUNT_KEY")

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("failed to init logger; %s", err)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	entries, err := os.ReadDir(srcDirectory)
	if err != nil {
		log.Fatalf("failed to read directory; %s; %s", srcDirectory, err)
	}

	uploader := upload.NewUploader(sugar, accountName, accountKey, container)

	for _, entry := range entries {
		fileName := entry.Name()
		sugar.Infow("start", "name", fileName)
		filePath := filepath.Join(srcDirectory, fileName)
		if err := uploader.Upload(filePath); err != nil {
			sugar.Errorw("failed to upload file", "name", fileName)
		}
		sugar.Infow("end", "name", fileName)
	}
}

func requiredEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("you must define %s environment variable", key)
	}
	return val
}
