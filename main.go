package main

import (
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/hsmtkk/azure-blob-upload/work"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var command = &cobra.Command{
	Use:  "azure-blob-upload srcDirectory container",
	Run:  run,
	Args: cobra.ExactArgs(2),
}

var numOfWorkers int

func init() {
	command.Flags().IntVar(&numOfWorkers, "numOfWorkers", 4, "number of workers")
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

	var wg sync.WaitGroup
	filePathChan := make(chan string)
	for i := 0; i < numOfWorkers; i++ {
		worker := work.NewWorker(sugar, accountName, accountKey, container, i, filePathChan)
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.Run()
		}()
	}

	for _, entry := range entries {
		path := filepath.Join(srcDirectory, entry.Name())
		filePathChan <- path
	}
	close(filePathChan)

	wg.Wait()
}

func requiredEnv(key string) string {
	val := os.Getenv(key)
	if val == "" {
		log.Fatalf("you must define %s environment variable", key)
	}
	return val
}
