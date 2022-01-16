package work

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"go.uber.org/zap"
)

type Worker struct {
	sugar         *zap.SugaredLogger
	accountName   string
	accountKey    string
	containerName string
	workerID      int
	filePathChan  <-chan string
}

func NewWorker(sugar *zap.SugaredLogger, accoutName, accountKey, containerName string, workerID int, filePathChan <-chan string) *Worker {
	return &Worker{sugar, accoutName, accountKey, containerName, workerID, filePathChan}
}

func (w *Worker) Run() {
	for filePath := range w.filePathChan {
		fileName := filepath.Base(filePath)
		w.sugar.Infow("begin upload", "id", w.workerID, "name", fileName)
		container, err := w.newContainer()
		if err != nil {
			w.sugar.Errorw("failed to get container", "error", err)
		}
		file, err := os.Open(filepath.Clean(filePath))
		if err != nil {
			w.sugar.Errorw("failed to open file", "name", fileName, "error", err)
		}
		blockBlob := container.NewBlockBlobClient(fileName)
		_, err = blockBlob.Upload(context.Background(), file, nil)
		if err != nil {
			w.sugar.Errorw("failed to upload file", "name", fileName, "error", err)
		}
		w.sugar.Infow("end upload", "id", w.workerID, "name", fileName)
	}
}

func (w *Worker) newContainer() (azblob.ContainerClient, error) {
	credential, err := azblob.NewSharedKeyCredential(w.accountName, w.accountKey)
	if err != nil {
		return azblob.ContainerClient{}, fmt.Errorf("failed to init credential; %w", err)
	}
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", w.accountName)
	serviceClient, err := azblob.NewServiceClientWithSharedKey(url, credential, nil)
	if err != nil {
		return azblob.ContainerClient{}, fmt.Errorf("failed to init client; %w", err)
	}
	container := serviceClient.NewContainerClient(w.containerName)
	return container, nil
}
