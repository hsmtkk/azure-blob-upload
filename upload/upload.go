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
	sugar         *zap.SugaredLogger
	accountName   string
	accountKey    string
	containerName string
}

func NewUploader(sugar *zap.SugaredLogger, accountName, accountKey, containerName string) *Uploader {
	return &Uploader{sugar, accountName, accountKey, containerName}
}

func (u *Uploader) Upload(filePath string) error {
	fileName := filepath.Base(filePath)

	credential, err := azblob.NewSharedKeyCredential(u.accountName, u.accountKey)
	if err != nil {
		return fmt.Errorf("failed to init credential; %w", err)
	}
	url := fmt.Sprintf("https://%s.blob.core.windows.net/", u.accountName)
	serviceClient, err := azblob.NewServiceClientWithSharedKey(url, credential, nil)
	if err != nil {
		return fmt.Errorf("failed to init client; %w", err)
	}
	container, err := serviceClient.NewContainerClient(u.containerName)
	if err != nil {
		return fmt.Errorf("failed to init container client; %w", err)
	}

	file, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return fmt.Errorf("failed to open file; %w", err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			u.sugar.Errorw("failed to close file", "error", err)
		}
	}()

	blockBlob, err := container.NewBlockBlobClient(fileName)
	if err != nil {
		return fmt.Errorf("failed to init block blob client; %w", err)
	}
	if _, err := blockBlob.UploadFile(context.Background(), file, azblob.UploadOption{}); err != nil {
		return fmt.Errorf("failed to upload file; %w", err)
	}

	return nil
}
