package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/hsmtkk/azure-blob-upload/upload"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var command = &cobra.Command{
	Use:  "azure-blob-upload",
	Run:  run,
	Args: cobra.ExactArgs(2),
}

func init() {}

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

	uploader, err := upload.NewUploader(sugar, accountName, accountKey, container)
	if err != nil {
		log.Fatalf("failed to init uploader; %s", err)
	}

	entries, err := os.ReadDir(srcDirectory)
	if err != nil {
		log.Fatalf("failed to read directory; %s; %s", srcDirectory, err)
	}

	var wg sync.WaitGroup
	for _, entry := range entries {
		path := filepath.Join(srcDirectory, entry.Name())
		wg.Add(1)
		go func(path string) {
			defer wg.Done()
			if err := uploader.Upload(path); err != nil {
				sugar.Errorw("upload failed", "error", err)
			}
		}(path)
	}
	wg.Wait()
}

func requiredEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("you must define %s environment variable", key)
	}
	return val
}
