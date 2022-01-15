package upload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"go.uber.org/zap"
)

type Uploader struct {
	sugar     *zap.SugaredLogger
	container azblob.ContainerClient
}

func NewUploader(sugar *zap.SugaredLogger, accountName, accountKey, containerName string) (*Uploader, error) {
	credential, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to init credential; %w", err)
	}
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	serviceClient, err := azblob.NewServiceClientWithSharedKey(url, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to init client; %w", err)
	}
	container := serviceClient.NewContainerClient(containerName)
	return &Uploader{sugar, container}, nil
}

func (u *Uploader) Upload(filePath string) error {
	u.sugar.Infow("upload begin", "path", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file; %w", err)
	}
	defer file.Close()

	fileName := filepath.Base(filePath)
	blockBlob := u.container.NewBlockBlobClient(fileName)
	_, err = blockBlob.Upload(context.Background(), file, nil)
	if err != nil {
		return fmt.Errorf("failed to upload file; %w", err)
	}

	u.sugar.Infow("upload end", "path", filePath)
	return nil
}
